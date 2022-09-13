#!/usr/bin/python3                                                                                                                                    
# read integration time_cost json file and group each regress                                                                                        

import sys, os.path, json, math, random

class Namespace: pass

# ff decreasing algorithm                                                                                                                            
def ffd_partition(arr, costDict, remain_arr):
    num_group = len(remain_arr)
    arr = sorted(arr, key=lambda x: costDict[x], reverse=True)
    res_arr = [[] for i in range(0, num_group)]
    total_arr = list(remain_arr)
    for elem in arr:
        min_v, min_i = min((v, i) for (i, v) in enumerate(total_arr))
        res_arr[min_i].append(elem)
        total_arr[min_i] += costDict[elem]
    return res_arr, total_arr

# Find the nearest array to target                                                                                                                   
def find_nearest(arr, costDict, target):
    ns = Namespace()
    ns.nearest = 0
    ns.nearest_arr = []
    # two different backtracking to implement the algorithm                                                                                          
    def find_nearest_bt(arr, costDict, target, tmp_arr, tmp_total, start_i):
        if start_i == len(arr):
            if abs(tmp_total - target) < abs(ns.nearest - target):
                ns.nearest = tmp_total
                ns.nearest_arr = list(tmp_arr)
            return
        find_nearest_bt(arr, costDict, target, tmp_arr, tmp_total, start_i + 1)
        if tmp_total >= target:
            return
        tmp_arr.append(arr[start_i])
        find_nearest_bt(arr, costDict, target, tmp_arr,
                tmp_total + costDict[arr[start_i]], start_i + 1)
        tmp_arr.pop()
    #end def                                                                                                                                         

    def find_nearest_bt2(arr, costDict, target, tmp_arr, tmp_total, start_i):
        if start_i == len(arr):
            if abs(tmp_total - target) < abs(ns.nearest - target):
                ns.nearest = tmp_total
                ns.nearest_arr = list(tmp_arr)
            return
        for i in range(start_i, len(arr)):
            if tmp_total >= target:
                continue
            tmp_arr.append(arr[i])
            find_nearest_bt2(arr, costDict, target,
                    tmp_arr, tmp_total + costDict[arr[i]], i + 1)
            tmp_arr.pop()
    #end def              

    tmp_arr = []
    find_nearest_bt2(arr, costDict, target, tmp_arr, 0, 0)
    return ns.nearest_arr, ns.nearest

# use backtrack to find nearest to the dynamic avg                                                                                                   
def nearest_partition(arr, costDict, remain_arr):
    num_group = len(remain_arr)
    arr = sorted(arr, key=lambda x: costDict[x])
    total = sum([costDict[x] for x in arr]) + sum(remain_arr)
    res_arr = [[] for i in range(0, num_group)]
    total_arr = list(remain_arr)
    if num_group > 0:
        for i in range(0, num_group - 1):
            target = total / float(num_group - i) - remain_arr[i]
            if target <= 0:
                nearest_arr, nearest = [[], 0]
            else:
                nearest_arr, nearest = find_nearest(arr, costDict, target)
            res_arr[i] = nearest_arr
            total_arr[i] += nearest
            arr = [x for x in arr if x not in nearest_arr]
            total -= nearest + remain_arr[i]
        res_arr[num_group - 1] = arr
        total_arr[num_group - 1] = total
    return res_arr, total_arr

# Brute force to traversal all possible solutions                                                                                                    
#   and get the one with minimum of maximum group sum                                                                                                
def bruteforce_partition(arr, costDict, remain_arr):
    ns = Namespace()
    num_group = len(remain_arr)
    arr = sorted(arr, key=lambda x: costDict[x])
    ns.res_arr = [[] for i in range(0, num_group)]
    ns.total_arr = list(remain_arr)
    ns.min_maximum_total = sys.maxsize

    # binary search to get the first one larger than num                                                                                             
    def binary_search_find_larger(arr, num):
        low = 0
        high = len(arr) - 1
        while low < high:
            mid = low + (high - low) / 2
            if arr[mid] > num:
                high = mid
            else:
                low = mid + 1
        if arr[low] > num:
            return low
        return len(arr)
    #end def                                                                                                                                         

    def find_possible(arr, costDict, tmp_arrs, tmp_totals, tmp_max_total, start_i):
        if start_i == len(arr):
            if tmp_max_total < ns.min_maximum_total:
                ns.min_maximum_total = tmp_max_total
                ns.res_arr = list(tmp_arrs)
                ns.total_arr = list(tmp_totals)
            return
        cost = costDict[arr[start_i]]
        # sorted_arrs = sorted(enumerate(tmp_arrs), key=lambda x: tmp_totals[x[0]])                                                                  
        # tmp_arrs = [x[1] for x in sorted_arrs]
        # tmp_totals = [tmp_totals[x[0]] for x in sorted_arrs]                                                                                       
        for i in range(0, len(tmp_arrs)):
            if tmp_totals[i] + cost >= ns.min_maximum_total:
                continue
            tmp_arrs[i].append(arr[start_i])
            tmp_totals[i] += cost
            new_cost = tmp_totals[i]
            new_index = binary_search_find_larger(tmp_totals, new_cost)
            tmp_arrs.insert(new_index - 1, tmp_arrs.pop(i))
            tmp_totals.insert(new_index - 1, tmp_totals.pop(i))
            find_possible(arr, costDict, tmp_arrs, tmp_totals,
                    max(tmp_max_total, new_cost), start_i + 1)
            tmp_arrs.insert(i, tmp_arrs.pop(new_index - 1))
            tmp_totals.insert(i, tmp_totals.pop(new_index - 1))
            tmp_totals[i] -= cost
            tmp_arrs[i].pop()
        #end def 

        tmp_arrs = [[] for i in range(0, num_group)]
        tmp_totals = list(remain_arr)
        find_possible(arr, costDict, tmp_arrs, tmp_totals, sys.maxsize, 0)
        return ns.res_arr, ns.total_arr

# can choose three different algorithm                                                                                                               
def split_groups(arr, costDict, remain_arr, ag_opt = 1):
    if ag_opt == 1:
        write_log('ffd algorithm')
        return ffd_partition(arr, costDict, remain_arr)
    if ag_opt == 2:
        write_log('nearest algorithm')
        return nearest_partition(arr, costDict, remain_arr)
    if ag_opt == 3:
        write_log('brute force algorithm')
        return bruteforce_partition(arr, costDict, remain_arr)

def average_arr(arr):
    total = 0
    for elem in arr:
        total += float(elem)
    return round(total / float(len(arr)), 1)

def swap_groups(i, j, arr):
    arr[i], arr[j] = arr[j], arr[i]

def check_special(ut_groups, sut_set, os_dict):
    # The logic here only works for a 4-OS setup or all nodes have special UTs allocated                                                             
    usable_indexs = []
    invalid_indexs = []
    for index, group in enumerate(ut_groups):
        special_number = 0
        for ut in group:
            if ut.split("_")[0] in sut_set:
                special_number += 1
        if special_number == 0 and os_dict[index] != 'centos6' and not os_dict[index].startswith('k8s'):
            usable_indexs.append(index)
        if special_number != 0 and (os_dict[index] == 'centos6' or os_dict[index].startswith('k8s')):
            invalid_indexs.append(index)
    for i in range(0, len(invalid_indexs)):
        if i < len(usable_indexs):
            swap_groups(invalid_indexs[i], usable_indexs[i], os_dict)
    return invalid_indexs[len(usable_indexs):]


def write_log(msg):
    global log_f
    log_f.write(msg)
    log_f.flush()

def main(parameters):
    if len(parameters) < 9:
        print("Invalid arguments: " + str(parameters[1:]))
        sys.exit(1)
    ut_cost_file = parameters[1]
    it_cost_file = parameters[2]
    unittests = parameters[3].strip()
    integration_tests = parameters[4].strip()
    num_group = int(parameters[5])
    os_strs = parameters[6]
    special_unittests = parameters[7]
    log_file = parameters[8]

    default_ag_op = 1
    global log_f
    log_f = open(log_file, 'w')

    ut_dict = {}
    if os.path.isfile(ut_cost_file):
        with open(ut_cost_file) as ut_cost_data:
            ut_dict = json.load(ut_cost_data)

    it_dict = {}
    if os.path.isfile(it_cost_file):
        with open(it_cost_file) as it_cost_data:
            it_dict = json.load(it_cost_data)

    # from pprint import pprint                                                                                                                      
    # pprint(it_dict)                                                                                                                                
    # exit()                                                                                                                                         

    write_log('start to parse tests from parameters\n')

    sut_set = set()
    for sut in special_unittests.split(","):
        sut_set.add(sut.strip())

    os_raw_dict = os_strs.split(",")
    os_dict = []
    for i in range(0, num_group):
        os_dict.append(os_raw_dict[i % len(os_raw_dict)])

    # Parse unittests                                                                                                                                
    total = 0
    ut_arr = []
    tmp_dict = {}
    special_total = 0
    special_ut_arr = []
    special_ut_dict = {}
    if unittests != "none":
        for ut in unittests.split():
            tmp_dict[ut] = average_arr(ut_dict[ut]) if ut in ut_dict else 1
            total += tmp_dict[ut]
            #ut_arr.append(ut)                                                                                                                       
            if ut.split("_")[0] in sut_set:
              special_total += tmp_dict[ut]
              special_ut_arr.append(ut)
            else:
              # ut_arr is now remaining ut                                                                                                           
              ut_arr.append(ut)
    ut_dict = tmp_dict
    write_log('special ut array:' + str(special_ut_arr) + '\n')
    write_log('special unit tests total is ' + str(special_total) + '\n')
    write_log('other ut array:' + str(ut_arr) + '\n')
    write_log('all unit tests total is ' + str(total) + '\n')

    # Parse integration tests                                                                                                                        
    total = 0

    it_arr = []
    tmp_dict = {}
    if integration_tests != "none":
        for its in integration_tests.split(';'):
            if not its:
                continue
            it = its.split(':')
            type = it[0].strip()
            for regress_num in it[1].strip().split():
                name = ("regress" if type not in ('gap', 'gst', 'gus') else "") + str(regress_num)
                new_name = type + " " + str(regress_num)
                if type in it_dict and name in it_dict[type]:
                    tmp_dict[new_name] = average_arr(it_dict[type][name])
                else:
                    tmp_dict[new_name] = 1
                total += tmp_dict[new_name]
                it_arr.append(new_name)
    it_dict = tmp_dict
    write_log('it array:' + str(it_arr) + '\n')
    write_log('all integration tests total is ' + str(total) + '\n')

    write_log('\nstart to split unittest costs\n')

    # split special ut to valid OS first - non-centos6/k8s                                                                                           
    remain_arr = []
    special_num_group = 0
    for special_os in os_dict:
        if not (special_os == "centos6" or special_os.startswith("k8s")):
            special_num_group += 1
    if special_num_group > 0:
        remain_arr = [0] * special_num_group
        special_ut_groups, special_ut_total = split_groups(special_ut_arr, ut_dict, remain_arr, default_ag_op)
        # update used capacity                                                                                                                         
        remain_arr = special_ut_total

    # split remaining ut to all OS                                                                                                                   
    for i in range(special_num_group, num_group):
      remain_arr.append(0)
    ut_groups, ut_total = split_groups(ut_arr, ut_dict, remain_arr, default_ag_op)

    # combine the groups. special group should be no more than all groups                                                                            
    for i in range(0, special_num_group):
      ut_groups[i].extend(special_ut_groups[i])
    write_log(str(ut_groups) + '\n')
    write_log(str(ut_total) + '\n')

    write_log('start to handle special unit tests\n')

    special_try = 1
    invalid_indexs = check_special(ut_groups, sut_set, os_dict)
    write_log('invalid_indexs: ' + str(invalid_indexs) + '\n')
    # following section should be useless now                                                                                                        
    ut_arr.extend(special_ut_arr)
    while len(invalid_indexs) != 0 and special_try < 4:
        write_log('Specail cases are not satisified, so split ut group again\n')
        random.shuffle(ut_arr)
        ut_groups, ut_total = split_groups(ut_arr, ut_dict, remain_arr, default_ag_op)
        write_log(str(ut_groups) + '\n')
        write_log(str(ut_total) + '\n')
        invalid_indexs = check_special(ut_groups, sut_set, os_dict)
        write_log('invalid_indexs: ' + str(invalid_indexs) + '\n')
        special_try += 1

    for s_index in invalid_indexs:
        new_index = (s_index + 1) % num_group
        while new_index in invalid_indexs and new_index != s_index:
            new_index = (new_index + 1) % num_group
        i = 0
        while i < len(ut_groups[s_index]):
            ut = ut_groups[s_index][i]
            if ut.split("_")[0] in sut_set:
                ut_groups[s_index].pop(i)
                ut_total[s_index] -= ut_dict[ut]
                ut_groups[new_index].append(ut)
                ut_total[new_index] += ut_dict[ut]
            else:
                i += 1
    
    write_log('\nstart to split integration tests costs\n')
    it_groups, all_total = split_groups(it_arr, it_dict, ut_total, default_ag_op)
    write_log(str(it_groups) + '\n')
    write_log(str(all_total) + '\n')

    test_groups = []
    for i in range(0, num_group):
        random.shuffle(ut_groups[i])
        random.shuffle(it_groups[i])
        test_groups.append({"os": os_dict[i], "ut": ut_groups[i], "it": it_groups[i], "total": all_total[i]})
    random.shuffle(test_groups)
    write_log('\ntest groups:\n')
    write_log(json.dumps(test_groups, indent=2) + '\n')

    # combine partitioned group result to string                                                                                                     
    write_log('\nstart to generate result string\n')
    res_str = ''
    max_ut_time = 0
    max_it_time = 0
    max_total_time = 0

    for index, group in enumerate(test_groups):
        write_log('Group ' + str(index) + ' , os ' + group["os"] + ' :\n')
        res_str += "# " if res_str else " "
        ut_str = ''
        it_str = ''

        # unit tests str generation                                                                                                                  
        write_log('unit test:\n')
        ug = group["ut"]
        group_total = 0
        if len(ug) == 0:
            ut_str += "none "
            write_log('none\n')
        else:
            for name in ug:
                ut_str += name + " "
                group_total += ut_dict[name]
                write_log(name + ' : ' + str(ut_dict[name]) + '\n')
        max_ut_time = max(max_ut_time, group_total)
        write_log('unit test total is ' + str(group_total) + '\n\n')

        # integration tests str generation                                                                                                           
        write_log('integration test:\n')
        ig = group["it"]
        group_total = 0
        if len(ig) == 0:
            it_str += "none "
            write_log('none\n')
        else:
            tmp_dict = {}
            for test in ig:
                type = test.split()[0]
                name = test.split()[1]
                if type not in tmp_dict:
                    tmp_dict[type] = {}
                tmp_dict[type][name] = it_dict[test]
            for type, regress in tmp_dict.items():
                it_str += type + ": "
                for name, cost in regress.items():
                    it_str += name + " "
                    group_total += tmp_dict[type][name]
                    write_log(type + " " + name + ' : ' + str(tmp_dict[type][name]) + '\n')
                it_str += "; "
        res_str += group["os"] + " $$$ " + ut_str + " $$$ " + it_str
        max_it_time = max(max_it_time, group_total)
        max_total_time = max(max_total_time, group["total"])
        write_log('integration test total is ' + str(group_total) + '\n')
        write_log('Total is ' + str(group["total"]) + '\n\n\n')
    print(res_str)
    write_log('max ut time: ' + str(max_ut_time) + '\n')
    write_log('max it time: ' + str(max_it_time) + '\n')
    write_log('max total time: ' + str(max_total_time) + '\n\n')
    write_log(res_str + '\n')
    log_f.close()
# end function main

##############################################                                                                                                       
# Arguments:                                                                                                                                         
#   0: this script name                                                                                                                              
#   1: time_cost json file                                                                                                                           
#   2: group number                                                                                                                                  
#   3: unittests                                                                                                                                     
##############################################                                                                                                       
if __name__ == "__main__":
    main(sys.argv)
