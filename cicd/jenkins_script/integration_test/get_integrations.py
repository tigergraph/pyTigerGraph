#! /usr/bin/python3

import argparse
import json
import time
import sys
import os
import re

from pathlib import Path
from copy import deepcopy
from contextlib import suppress
from pprint import pprint
from collections import ChainMap
from collections import Counter
from collections import defaultdict

sys.path.insert(0, f'{Path(__file__).absolute().parent.parent}/python3_script')
import util


U2I = None
TEST_CONFIG = None
TEST_LOC = None
ALL_TYPES = None
ALL_RT_TYPES = None
ENVIRON = None


def read_json(fname=""):
    path = Path(__file__).absolute().parent

    if fname == "its_config":
        path = path.parent/"config"/fname
    elif fname == "its_location":
        path = path.parent/"config"/fname
    elif fname == "u2i_transform":
        path = path.parent/"config"/fname
    elif fname == "test_config":
        path = path.parent/"config"/fname
        # path = f"/mnt/nfs_datapool/mitLogs/config/test_config/{fname}"
    else:
        if not (path/fname).is_file():
            return ""

    with open(f"{path}.json", 'r', encoding='utf-8') as f:
        content = json.load(f)

    return content

def read_config(fname=""):
    it_map = {}
    path = Path(__file__).absolute().parent
    if fname == "excluded_tests":

        path = Path("/mnt/nfs_datapool/mitLogs/config/test_config/")

        if  ENVIRON["MIT_TG_VERSION"] < 3:
            path = path/(fname + "2")
        else:
            path = path/fname

    if os.path.exists(f"{path}.conf"):
        with open(f"{path}.conf", "r", encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                if line[0] != '#' and ':' in line:
                    repo, test_list = [*map(str.strip, line.split(':'))]
                    it_map[repo] = test_list.split()
    
    return it_map

def read_summary(match_job_path, fname="integration_test_summary"):
    it_map = {}

    if fname == "integration_test_summary":
        jobs = {"hourly_test", "mit_test", "wip_test", "home"}
        excluded_set = {"Total","running","uncompleted","On machine"}

        if match_job_path and any(job in match_job_path for job in jobs):    
            summary_path = Path(match_job_path)/fname

            if summary_path.exists():
                with open(summary_path, "r") as f:
                    lines = f.readlines()
                    for line in lines:                        
                        if len(line) > 1 and not any([e in line for e in excluded_set]):
                            arr = line.split()
                            test_type, test_name = arr[0], arr[1].replace('regress','')
                            if "failed" not in line:
                                if test_type not in it_map:
                                    it_map[test_type] = []
                                it_map[test_type].append(test_name)

    return it_map

def get_evs():
    all_evs = dict(os.environ)

    return all_evs

def init_global_vars():
    global U2I, TEST_CONFIG, TEST_LOC, ALL_TYPES, ALL_RT_TYPES, ENVIRON

    ENVIRON = get_evs()
    U2I = read_json("u2i_transform")
    TEST_CONFIG = read_json("test_config")
    TEST_LOC = read_json("its_location")
    ALL_TYPES = read_json("its_config")
    ALL_RT_TYPES = gen_rt_types(ALL_TYPES)

    if "MIT_TG_VERSION" not in ENVIRON:
        ENVIRON["MIT_TG_VERSION"] = "3.0.0"

    occ = [dot.start() for dot in re.finditer('\.', ENVIRON["MIT_TG_VERSION"])]
    ENVIRON["MIT_TG_VERSION"] = float(ENVIRON["MIT_TG_VERSION"][:occ[1]]) if len(occ) > 1 else float(ENVIRON["MIT_TG_VERSION"])

def gen_rt_types(all_types):
    rt_types = set()
    for repo in all_types:
        rt_types.add(repo)
        if all_types[repo]:
            for test_type in all_types[repo]:
                rt_types.add(f"{repo}_{test_type}")
    
    return rt_types

def to_str(obj):
    out = ""
    if isinstance(obj, set):
        out = " ".join(obj)         
    elif isinstance(obj, dict):        
        for k, v in obj.items():            
            if isinstance(v, dict):                
                for kk, vv in v.items():
                    values = " ".join(vv) if isinstance(vv, list) else vv
                    out += f"{k}_{kk}: {values} ; "
            elif isinstance(v, list):
                out += f"{k}: {' '.join(v)} ; "
            else:
                out += f"{k}: {v} ; "

    return out if out else "none"

def get_all_its(types="all"):
    types = ALL_TYPES.keys() if types == "all" else types.split()
    all_its = [{k: "all"} for k in types if k not in ALL_TYPES["gle"]]
    all_its = dict(ChainMap(*all_its))

    return all_its

def get_default_its(job_param="", branch_name=""): 
    whitelist = TEST_CONFIG["branch_whitelist"]
    config_set = whitelist[-1]["dependency"]
    if branch_name:
        for tmp_set in whitelist:
            if branch_name in tmp_set['branch']:
                config_set = tmp_set["dependency"]
                break

    default_its = set()
    if job_param:
        regPtn = '(?P<repo>\w+)\s*\#\s*(?P<pr_num>[^;]+)'
        reg = re.compile(regPtn, re.M)
        repos = {m.groupdict()['repo'] for m in reg.finditer(job_param)}
        
        uts = " ".join(set().union(*[set(config_set.get(repo, "").split()) for repo in repos if repo in config_set]))

        _, default_its = split_uits(uts)

    return default_its

def convert_ut2it(uts):
    u2i_test = {}
    for ut in uts:
        repo = U2I[ut]["repo"].lower()
        if "u2i" in U2I[ut]:                        
            if U2I[ut]["u2i"] == "all":
                tmp_u2i_test = [{f"{repo}_{test_type}": "all"} for test_type in ALL_TYPES[repo]]
                tmp_u2i_test = dict(ChainMap(*tmp_u2i_test))
                u2i_test.update(tmp_u2i_test)
            else:
                u2i_test.update({f"{repo}_{U2I[ut]['u2i']}": "all"})
        elif 'test_name' in U2I[ut]:
            if U2I[ut]["test_name"] == "all":
                u2i_test.update({repo: [U2I[ut]['test_name'] for ut in U2I if U2I[ut]['repo'] == repo]})
            else:
                if repo not in u2i_test:
                    u2i_test[repo] = []
                if U2I[ut]['test_name'] not in u2i_test[repo]:
                    u2i_test[repo].append(U2I[ut]['test_name'])

    return u2i_test

def add_subtypes(its):
    it_map = {}
    for repo in its:        
        if repo in ALL_TYPES and ALL_TYPES[repo] and not isinstance(its[repo], dict):
            for test_type in ALL_TYPES[repo]:
                it_map[f"{repo}_{test_type}"] = its[repo]
        else:
            if repo in ALL_TYPES["gle"]:
                it_map[f"gle_{repo}"] = its[repo]
            else:       
                it_map[repo] = its[repo]

    return it_map

def convert_all2data(its, tag):
    it_map = {}
    u2i_lst = [U2I[k] for k in U2I if 'test_name' in U2I[k] and U2I[k]['test_name'] != 'all']
    u2i_repos = {e['repo'] for e in u2i_lst}

    for rt, test_list in its.items():
        it_map[rt] = its[rt]     
        if test_list == "all": 
            if rt in u2i_repos:            
                it_map[rt] = [e['test_name'] for e in u2i_lst if e['repo'] == rt]
            else:     
                repo, test_type = split_rt(rt)
                with suppress(Exception): it_map[rt] = util.get_test_ids(test_loc=TEST_LOC, repo=repo, test_type=test_type, tag=tag)

    return it_map

def split_rt(rt):
    return rt.split('_') if '_' in rt else (rt, "")

def to_std_format(its):
    it_map = {}
    for k, v in its.items():
        if isinstance(v, dict):
            for kk, vv in v.items():
                it_map[f"{k}_{kk}"] = its[k][kk]
        else:
            it_map[k] = its[k]

    return it_map

def filter_tests(its, bypass={}, tag=""):
    it_map = {}

    u2i_lst = [U2I[k] for k in U2I if 'test_name' in U2I[k] and U2I[k]['test_name'] != 'all']
    u2i_repos = {e['repo'] for e in u2i_lst}
    u2i_dict = {repo: [e['test_name'] for e in u2i_lst if e['repo'] == repo] for repo in u2i_repos}

    it_repos = {it[:it.index('_')] for it in its if '_' in it}
    rb_dict = {repo: util.is_tag_exists(repo, tag) for repo in it_repos}

    if rb_dict and not any(rb_dict.values()):
        return it_map

    for k, v in its.items():
        if k not in ALL_RT_TYPES:
            continue

        if k in bypass:
            if bypass[k] == "all":
                continue

            test_list = [i for i in its[k] if i not in bypass[k]]
            if test_list:
                it_map[k] = test_list
            else:
                continue

        if k not in u2i_repos:
            repo, test_type = split_rt(k); test_ids = []

            if not rb_dict[repo]:
                continue

            test_ids = util.get_test_ids(test_loc=TEST_LOC, repo=repo, test_type=test_type, tag=tag)
 
            test_list = [val for val in (its[k] if k not in it_map else it_map[k]) if val in test_ids]
            if test_list:
                it_map[k] = test_list
        else:
            test_list = [val for val in (its[k] if k not in it_map else it_map[k]) if val in u2i_dict[k]]
            if test_list:
                it_map[k] = test_list

    return it_map

def split_uits(unit_tests):
    out_ut, out_it, uts, uts_e2e = [set()]*4

    regPtn = r'\"?\s*(?P<ut>\w+)\s*\"?'
    matchStr = re.findall(regPtn, unit_tests)

    if matchStr:
        reg = re.compile(regPtn, re.M)
        ut_map = [m.groupdict() for m in reg.finditer(unit_tests)]
        uts = set([d['ut'] for d in ut_map])

        uts_e2e = set([test for test in uts if test in U2I])
        uts = uts - uts_e2e

    return uts, uts_e2e

def join_dicts(*args):
    it_map = {}

    for d in args:
        for k, v in d.items():
            if v == "all":
                it_map[k] = v
            else:           
                if k not in it_map:
                    it_map[k] = []
                if it_map[k] != "all":
                    it_map[k] += v

    return it_map

def get_excluded_uits():
    excluded_uits = read_config("excluded_tests")

    return excluded_uits

def get_skipped_uits(match_job_path):
    skipped_uits = read_summary(match_job_path)

    return skipped_uits

def has_format(input_str, format):
    input_str = input_str.replace("'", '"')

    matchStr=""
    if format == "standard":
        regPtn = r'\"?(?P<rt>[A-Za-z]+_?\w+)\"?\s*:\s*\[?\s*(?P<nums>\s*((\s*\w+\s*,?\s*)+|\"?all\"?)\s*)\]?'                
        matchStr = re.findall(regPtn, input_str)
    elif format == "json":
        regPtn = r'(\{\s*)?(?P<chunk>\{?\s*\"?\w+\"?\s*:?\s*{.+?:.+?})(\}\s*)?'
        matchStr = re.findall(regPtn, input_str)

    return bool(matchStr)

def merge_same_keys(it_map):
    cnt = Counter([*d.keys()][0] for d in it_map)

    if all([v == 1 for v in cnt.values()]):
        return it_map

    res_map = {}       
    for d in it_map:
        for k, v in d.items():
            if k not in res_map:
                res_map[k] = set()
            res_map[k] |= set(v)

    res_map = [{k: list(v)} for k, v in res_map.items()]

    return res_map


def to_dict(its):
    if not its:
        return {}

    it_map = {}

    # json format
    if has_format(its, "json"):
        if not it_map:
            with suppress(Exception): it_map = json.loads(its)
            
        if not it_map:
            its_tmp = deepcopy(its)
            its_tmp = re.sub(r'[\"\']+', r'', its_tmp)
            its_tmp = re.sub(r'(\w+)', r'"\1"', its_tmp)
            its_tmp = its_tmp if its_tmp[0] == '{' else  f"{ {its_tmp} }".replace("'", "")
            with suppress(Exception): it_map = dict(json.loads(its_tmp))
            
        if not it_map:
            regPtn = r'\{?\s*\"?(?P<repo>[A-Za-z]+)\"?(\s*:?\s*)?{\s*(?P<test_types>((\s*\"?\w+\"?\s*:\s*(\[?\s*(\d+(\s*|\s*\,\s*))+\s*\]?|\"?all\"?)\s*,?)*))\s*\}'
            reg = re.compile(regPtn, re.M)
            it_map = [m.groupdict() for m in reg.finditer(its)]
            it_map = [{d['repo']: d['test_types']} for d in it_map if d['repo'] in ALL_TYPES]

            regPtn = '\"?(?P<test_type>[A-Za-z]+)\"?\s*:\s*\[?\s*(?P<tests>(\d+\s*,?\s*)+|\"?all\"?)\s*\]?\s*'
            reg = re.compile(regPtn, re.M)

            for i, it_d in enumerate(it_map):
                for repo, test_types in it_d.items():
                    v_dict = [m.groupdict() for m in reg.finditer(test_types)]
                    for d in v_dict:
                        if not isinstance(it_map[i][repo], dict):
                           it_map[i][repo] = {}                                            
                        it_map[i][repo][d['test_type']] = list(filter(None, re.split('\s+|\s*,\s*', d['tests']))) if d['tests'].strip() != "all" else d['tests'].strip()
  
            it_map = dict(ChainMap(*it_map))
          
    # standard format
    elif has_format(its, "standard"):
        regPtn = r'\"?(?P<rt>[A-Za-z]+_?\w+)\"?\s*:\s*\[?\s*(?P<tests>\s*((\s*\w+\s*,?\s*)+|\"?all\"?)\s*)\]?'                
        matchStr = re.findall(regPtn, its)
        reg = re.compile(regPtn, re.M)

        it_map = [m.groupdict() for m in reg.finditer(its)]
        it_map = [{d['rt']: list(filter(None, re.split('\s+|\s*,\s*', d['tests']))) if d['tests'].strip() != "all" else d['tests'].strip()} for d in it_map]
        
        it_map = merge_same_keys(it_map)
        it_map = dict(ChainMap(*it_map))

    return it_map


def main(args):
    init_global_vars()
    
    uts = set(); uts_e2e = set(); uts_e2e_def = set(); its = {}; bypass_its = {}; bypass_uts = {}
    if args.unit_tests:
        seq = " -"
        if seq in args.unit_tests:              
            bypass_str = args.unit_tests[args.unit_tests.index(seq):]
            args.unit_tests = args.unit_tests.replace(bypass_str, "")
            bypass_uts = set(bypass_str[len(seq):].split())

        uts, uts_e2e = split_uits(args.unit_tests)
        if args.cleanup:            
            return to_str(uts)
    
    if args.integration_tests:
        seq = " -"
        if seq in args.integration_tests:              
            bypass_str = args.integration_tests[args.integration_tests.index(seq):]
            args.integration_tests = args.integration_tests.replace(bypass_str, "")                
            bypass_its = to_dict(bypass_str[len(seq):])

        if "all" == args.integration_tests:
            its = get_all_its("gle")
        elif "all_its" == args.integration_tests:
            its = get_all_its()
        else:
            seq = "default"           
            if seq in args.integration_tests:
                args.integration_tests = args.integration_tests.replace(seq, "")
                uts_e2e_def = get_default_its(args.job_param, args.branch_name)

            its = to_dict(args.integration_tests)

    its = add_subtypes(to_std_format(its))
    u2i_tests = convert_ut2it(uts_e2e.union(uts_e2e_def).difference(bypass_uts))
    all_its = convert_all2data({**its, **u2i_tests}, args.branch_name)

    bypass_its = to_std_format(bypass_its)
    skipped_its = get_skipped_uits(args.match_job)
    excluded_uits = get_excluded_uits()
    unwanted_its = join_dicts(add_subtypes(bypass_its), add_subtypes(skipped_its), add_subtypes(excluded_uits))

    all_its = filter_tests(all_its, unwanted_its, args.branch_name)

    return to_str(all_its)

if __name__ == '__main__':    

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--integration_tests', help='list of integration tests')
    parser.add_argument('-u', '--unit_tests', help='list of unit tests')      
    parser.add_argument('-jp', '--job_param', help='jenkins job PARAM')
    parser.add_argument('-bn', '--branch_name', help='mark_tag_name job param')
    parser.add_argument('-mj', '--match_job', help='match job dir')
    parser.add_argument('-cleanup', action='store_true', default=False, help='remove uts that should be run as its')  

    args = parser.parse_args()

    print(main(args))
