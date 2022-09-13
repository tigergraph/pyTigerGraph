import os, sys, re, json
from os.path import dirname, abspath
# import util


failed_test = [[], []]
passed_test = [[], []]
uncompleted_test = [[], []]
running_test = [[], []]

U2I = None
ALL_TYPES = None

def read_any_json(fname):
    path =  dirname(abspath(__file__))

    if fname == "its_config":
        path = dirname(path) + "/config/" + fname
    elif fname == "u2i_transform":
        path = dirname(path) + "/config/" + fname
    else:
        if not (path/fname).is_file():
            return ""

    with open("{}.json".format(path), 'r') as f:
        content = json.load(f)

    return content

def read_json(line, index, config_dict):
    contents = line.split()
    if not contents or not config_dict:
        return
    if index == 0: # it is unittest
        if contents[0] in config_dict:
            config_dict.pop(contents[0], None)
    else: # it is integration test
        if contents[0] in config_dict and contents[1] in config_dict[contents[0]]:
            config_dict[contents[0]].pop(contents[1], None)
            if len(config_dict[contents[0]]) == 0:
                config_dict.pop(contents[0], None)


def read_summary(summary_file, json_file, index):
    global failed_test, passed_test, uncompleted_test, running_test
    config_dict = {}
    if os.path.exists(json_file):
        with open(json_file) as json_data:
            config_dict = json.load(json_data)
    with open(summary_file) as summary:
        machine_name = ''
        lines = summary.read().split('\n')
        for line in lines:
            if not line or 'Total' in line:
                continue
            elif 'On machine' in line:
                machine_name = line.split()[2][:-1]
            else:
                arr_ref = passed_test
                if '(failed)' in line:
                    arr_ref = failed_test
                elif '(uncompleted)' in line:
                    arr_ref = uncompleted_test
                elif '(running)' in line:
                    arr_ref = running_test

                if machine_name:
                    arr_ref[index].append("%s (%s)" %(line, machine_name))
                else:
                    arr_ref[index].append("%s" %(line))
                read_json(line, index, config_dict)

    if index == 0: # it is unittest
        for unit, time in config_dict.items():
            if unit not in U2I:
                uncompleted_test[index].append("%s (unstarted)" %(unit))
    else: # it is integration test
        for inte, regress in config_dict.items():
            for name, time in regress.items():
                if inte not in ALL_TYPES['gle']:
                    uncompleted_test[index].append("%s %s (unstarted)" %(inte, name))

def print_summary_arr(arr, name, silence = False):
    if len(arr[0]) == 0 and len(arr[1]) == 0:
        return
    if not silence:
        print('\n-----------------------%s-----------------------' %(name))
    if len(arr[0]) != 0:
        if not silence:
            print('Unittest:')
        for line in arr[0]:
            print(line)
    if len(arr[1]) != 0:
        if not silence:
            print('\nIntegration test:')
        for line in arr[1]:
            print(line)
    if not silence:
        print('-----------------------%s-----------------------\n' %('-' * len(name)))


def main(parameters):
    global U2I, ALL_TYPES

    U2I = read_any_json("u2i_transform")
    ALL_TYPES = read_any_json("its_config")

    # util.check(len(parameters) >= 2, RuntimeError,
    #    "Invalid arguments: " + str(parameters[1:]))

    unit_test_summary = parameters[1]
    integration_test_summary = parameters[2]
    info = parameters[3]
    unit_test_json = ''
    integration_test_json = ''
    if len(parameters) >= 5:
        unit_test_json = parameters[4]
    if len(parameters) >= 6:
        integration_test_json = parameters[5]

    if os.path.exists(unit_test_summary):
        read_summary(unit_test_summary, unit_test_json, 0)
    if os.path.exists(integration_test_summary):
        read_summary(integration_test_summary, integration_test_json, 1)

    if info.find('failed') != -1 or info.find('all') != -1 :
        print_summary_arr(failed_test, 'Failed Tests', info == 'failed')
    if info.find('running') != -1 or info.find('all') != -1 :
        print_summary_arr(running_test, 'Running Tests')
    if info.find('passed') != -1  or info.find('all') != -1 :
        print_summary_arr(passed_test, 'Passed Tests')
    if info.find('uncompleted') != -1 or info.find('all') != -1 :
        print_summary_arr(uncompleted_test, 'Uncompleted Tests')

##############################################
# Arguments:
#   0: this script name
#   1: unittest summary
#   2: integration test summary
#   3: unit test config file
#   4: integration config file
#   5: info to print, failed or uncompleted or passed
##############################################
if __name__ == "__main__":
    main(sys.argv)
