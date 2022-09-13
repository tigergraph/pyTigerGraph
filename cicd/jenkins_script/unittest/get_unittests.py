#!/usr/bin/python3
# get all unittests from unittests_dependency.json

from os.path import split
import sys, os, json, math, random

def arr2Str(arr):
    str = ""
    for elem in arr:
        str += (" " if str else "") + elem
    return str

def getSuites():
    suites = {}
    unit_conf = os.path.join(os.path.dirname(__file__), 'unittests.conf')
    if os.path.isfile(unit_conf):
        funittests = open(unit_conf, "r")
        for line in funittests:
            if not line.startswith("#"):
               #support unit with multiple underscores
               suite = line[:line.find(":")]
               units = [suite[:i] for i, char in enumerate(suite) if char == "_"]
               for unit in units:
                   if unit and suite:
                      if unit not in suites:
                        suites[unit] = suite
                      else:
                        suites[unit] += " " + suite
               #supoort suite as UT name directly
               if suite and suite not in suites:
                   suites[suite] = suite
    return suites

def unit2suites(units, unit_suites, res_units):
    for unit in units.split():
        unit = (unit_suites[unit] if unit in unit_suites else unit)
        for item in unit.split():
          if item and item not in res_units:
              res_units.append(item)
    return res_units

def getExcluded():
    excluded_file="/mnt/nfs_datapool/mitLogs/config/test_config/excluded_tests.conf"
    if int(os.environ.get('MIT_TG_VERSION', '3.0.0').split(".")[0]) < 3:
        excluded_file="/mnt/nfs_datapool/mitLogs/config/test_config/excluded_tests2.conf"
    fexcluded = open(excluded_file, "r")
    for line in fexcluded:
        line = line.strip()
        scope = line[:line.find(":")]
        if scope == "unit":
            return line[line.find(":")+1:].split(" ")
    return []

def getSkipUT(match_job):
    # also skip successful unit test in matched job when using skip_build
    skip_ut = []
    fail_ut = []
    excludes = ["Total","running","uncompleted","On machine"]
    if ("hourly_test" in match_job) or ("mit_test" in match_job) or ("wip_test" in match_job):
        ut_summary = f"{match_job}/unit_test_summary"
        if not os.path.exists(ut_summary):
            return []
        with open(ut_summary,"r") as summary:
            lines = summary.readlines()
            for line in lines:
                if line.rstrip("\r\n") == "": continue
                if any(key in line for key in excludes): continue
                ut_name = line.split(" ")[0]
                if "failed" in line:
                    fail_ut.append(ut_name)
                if ut_name not in skip_ut:
                    skip_ut.append(ut_name)
    res_ut = [u for u in skip_ut if u not in fail_ut ]
    return res_ut

def main(parameters):
    if len(parameters) < 5:
        print("Invalid arguments: " + str(parameters[1:]))
        sys.exit(1)
    test_config_file = parameters[1]
    param = parameters[2]
    unittests = parameters[3]
    match_job = parameters[4]

    depend_dict = {}
    with open(test_config_file) as test_config:
        test_config_json = json.load(test_config)
        depend_dict = test_config_json["dependency"]

    res_units = []
    bypass_ut = []
    unit_suites = getSuites()
    excluded_units = getExcluded()
    skip_units = getSkipUT(match_job) if match_job != "none" else []
    if " -" in unittests and len(unittests.split(" -")) == 2: 
        bypass_ut = unit2suites(unittests.split(" -")[1],unit_suites,bypass_ut)
        unittests = unittests.split(" -")[0]
    if unittests == "all":
        for repo, units in depend_dict.items():
            res_units = unit2suites(units, unit_suites, res_units)
        #return arr2Str(res_units)
    else:
        if "default" not in unittests :
            return arr2Str(unit2suites(unittests, unit_suites, res_units))
        else:
           for pr in param.split(';'):
               if pr == "":
                   continue
               if len(pr.split('#')) != 2:
                   print("Invalid PARAM")
                   sys.exit(1)
               repo = pr.split('#')[0].lower().strip()
               if repo not in depend_dict:
                   continue
               units = depend_dict[repo]
               res_units = unit2suites(units, unit_suites, res_units)
    res_units = [u for u in res_units if (u not in (excluded_units + skip_units + bypass_ut))]
    return arr2Str(res_units)
# end function main

##############################################
# Arguments:
#   0: this script name
#   1: unittests_dependency.json
#   2: pull request PARAM
#   3: unittests (default or customized)
##############################################
if __name__ == "__main__":
    print(main(sys.argv))
