#!/usr/bin/python

import sys, os.path, datetime, subprocess, re
import requests, json, os


def check(condition, error, msg):
    if not(condition):
        raise error, msg
    # end if
# end function check

class GithubAPIFail(RuntimeError):
    def __init__(self, arg):
        self.args = [arg]
# end class GithubAPIFail

class MergeFail(RuntimeError):
    def __init__(self, arg):
        self.args = [arg]
# end class MergeFail

class NotifyFail(RuntimeError):
    def __init__(self, arg):
        self.args = [arg]
# end class NotifyFail

class validateFail(RuntimeError):
    def __init__(self, arg):
        self.args = [arg]
# end class validateFail

class OpenIssueFail(RuntimeError):
    def __init__(self, arg):
        self.args = [arg]
# end class OpenIssueFail

class AdvanceTagFail(RuntimeError):
    def __init__(self, arg):
        self.args = [arg]
# end class AdvanceTagFail

class IssueExistanceFail(RuntimeError):
    def __init__(self, arg):
        self.args = [arg]
# end class IssueExistanceFail

def print_err(msg):
    sys.stderr.write(msg + '\n')

def send_http_request(url, headers, method, data=None, params={}):
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
    if method == 'GET':
        response = requests.get(url, verify=False, headers=headers, data=data, params=params)
    elif method == 'POST':
        response = requests.post(url, verify=False, headers=headers, data=data, params=params)
    elif method == 'PUT':
        response = requests.put(url, verify=False, headers=headers, data=data, params=params)
    elif method == 'DELETE':
        response = requests.delete(url, verify=False, headers=headers, data=data, params=params)
    elif method == 'PATCH':
        response = requests.patch(url, verify=False, headers=headers, data=data, params=params)
    else:
        raise RuntimeError, 'Unkown http method'
    # end if-else
    return response
# end function send_http_request


def send_http_request_auth(url, headers, method, data = None, params = {}, user_name = '', password = ''):
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
    if method == 'GET':
        response = requests.get(url, verify=False, headers=headers, data=data, params=params, auth=(user_name, password))
    elif method == 'POST':
        response = requests.post(url, verify=False, headers=headers, data=data, params=params, auth=(user_name, password))
    elif method == 'PUT':
        response = requests.put(url, verify=False, headers=headers, data=data, params=params, auth=(user_name, password))
    elif method == 'DELETE':
        response = requests.delete(url, verify=False, headers=headers, data=data, params=params, auth=(user_name, password))
    elif method == 'PATCH':
        response = requests.patch(url, verify=False, headers=headers, data=data, params=params)
    else:
        raise RuntimeError, 'Unkown http method'
    # end if-else
    return response
# end function send_http_request_auth

def run_bash(cmd):
    try:
        result = subprocess.check_output(["bash", "-c", cmd])
    except subprocess.CalledProcessError:
        raise RuntimeError, "Fail to run bash command '" + cmd + "'"
    # end try-catch
    # remove last '\n' char
    return result[:-1]
#end function run_bash

def parse_parameter(parameters, index):
    """
    Parse rep1#pull_number1 rep2#pull_number2 ... to a directory
    Args:
        parameters: parameters array
        index: index number of parameters
    Returns:
        A dictionary of repo and pull request key value pair
    TODO:
        This case should been taken care of:
        Can not have two same repos regardless of pull_number
    """
    if len(parameters) <= index:
        return {}
    # end if
    dict = {}
    for var in parameters[index].split(';'):
        if var == "":
            continue
        var = var.replace('=','#')
        check(len(var.split('#')) == 2, RuntimeError,
                "Invalid argument: " + var)
        repo = var.split('#')[0].lower().strip()
        pull_req = var.split('#')[1].strip()
        if repo in dict:
            raise validateFail, "can not have mutliple " + repo
        dict[repo] = pull_req
    # end for
    return dict
# end function parse_parameter

def prepare_log(directory, log_name):
    """
    Create log file and rename it with timestamp
    Args:
        directory: directory of log file
        log_name: log file name
    Returns:
        log file path
    """
    directory = os.path.expanduser(directory)
    if not(os.path.isdir(directory)):
        os.makedirs(directory)
    # end if
    log_file = directory + "/" + log_name
    if os.path.isfile(log_file):
        time = str(datetime.datetime.now()).replace(' ', '.')
        os.rename(log_file, log_file + '.' + time)
    # end if
    return log_file
# end function prepare_log

def get_branch_sha_dict(text):
    """
    Get branch sha values from given log file by filtering related lines
    Args:
        text: log text
    Returns:
        a dict of sha values, e.g.: {"gle": "771e133b719eb037b7b990566f451939d77c4b22"}
    """
    lines = text.split("\n")
    lines = [line.split() for line in lines if len(line.split()) >= 3 and re.match("^[a-z0-9]{40}$", line.split()[2])]
    res = {}
    for t in lines:
        res[t[0]] = t[2]
    return res
# end function get_branch_sha_dict


def read_config_file():
    """
    Args:
        read config json file
    """
    jenkins_id = os.environ.get("JENKINS_ID")
    jenkins_config_file = '../../config/config.json' if not jenkins_id or "prod_sv4" in jenkins_id else "../../config/config_" + jenkins_id +".json"
    config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), jenkins_config_file)
    configs = {}
    with open(config_file) as config_json:
        configs = json.load(config_json)
    return configs


def read_test_config_file(config_file):
    """
    Args:
        read config json file
    """
    configs = {}
    with open(config_file) as config_json:
        configs = json.load(config_json)
    return configs

def dict_overwrite(dictFrom, dictTo):
    # if key not in dictTo, append to it, otherwise ignore it
    for key, val in dictFrom.iteritems():
        if key not in dictTo:
            dictTo[key] = dictFrom[key]
            continue
        if type(val) is dict and val != {} :
            dict_overwrite(dictFrom[key], dictTo[key])


def read_total_config():
    configs = read_config_file()
    if 'log_dir' not in configs:
        print 'error: config file miss log_dir'
        return
    raw_test_config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../config/test_config.json')
    old_test_configs = read_test_config_file(raw_test_config_file)
    # abandon test_config under mitLog for protection
    dict_overwrite(old_test_configs, configs)
    return configs
    new_test_config_file = configs['log_dir'] + '/config/test_config/test_config.json'
    if not os.path.isfile(new_test_config_file):
        print 'WARNING: can not find test_config.json in mnt'
        dict_overwrite(old_test_configs, configs)
        return configs
    new_test_configs = read_test_config_file(new_test_config_file)
    dict_overwrite(old_test_configs, new_test_configs)
    dict_overwrite(new_test_configs, configs)
    return configs



def get_default_branches(repo_list_file, branch='default'):
    branches = {}
    for repo in open(repo_list_file).read().split():
        if repo.strip():
            branches[repo.strip()] = branch
    return branches
# end function get_default_branches


def decode_token(fake_token):
    token = ""
    for char in fake_token:
        if char.isdigit():
            token += str(9 - int(char))
        elif char.isalpha():
            token += chr(ord('a') + ord('z') - ord(char.lower()))
    return token[::-1]


def do_git_clone(repo, folder_name, branch_name='', git_option=''):
    GIT_USER = os.getenv('MIT_GIT_USER', '')
    GIT_TOKEN = os.getenv('MIT_GIT_TOKEN', '')
    if branch_name != '':
        branch_name = "-b " + branch_name
    git_cmd = "rm -rf {}; git clone {} --quiet {} https://{}:{}@github.com/TigerGraph/{}.git {}".format(
            folder_name, branch_name, git_option, GIT_USER, GIT_TOKEN, repo, folder_name)
    run_bash(git_cmd)


# QA-2698
def encode_str(s, charset="utf-8"):
    return unicode(s).encode(charset)
