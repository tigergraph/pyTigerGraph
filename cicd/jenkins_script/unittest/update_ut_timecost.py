#!/usr/bin/python3                                                                                                     
# transfer time_cost original file to json file                                                                        

import sys, os.path, json

def main(parameters):
    time_cost_file = parameters[1]
    time_cost_json_file = parameters[2]
    data_len = 10
    test_dict = {}
    prev_dict = {}
    with open(time_cost_file) as origianl_file:
        lines = origianl_file.read().split('\n')
        for line in lines:
            contents = line.split()
            if not contents or '(' in line or 'On machine' in line or 'Total' in line:
                continue
            test_dict[contents[0]] = contents[1]
    with open(time_cost_json_file) as origianl_file:
        prev_dict = json.load(origianl_file)

    for ut, cost in test_dict.items():
        if ut not in prev_dict:
            prev_dict[ut] = []
        while len(prev_dict[ut]) >= data_len:
            prev_dict[ut].pop(0)
        prev_dict[ut].append(cost)

    with open(time_cost_json_file, 'w') as res_file:
        json.dump(prev_dict, res_file, indent=2, sort_keys=True)
# end function main        

##############################################                                                                         
# Arguments:                                                                                                           
#   0: this script name                                                                                                
#   1: time_cost origianl file                                                                                         
#   2: time_cost json file                                                                                             
##############################################                                                                         
if __name__ == "__main__":
    main(sys.argv)