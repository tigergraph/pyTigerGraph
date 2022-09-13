import os, sys, re, json

def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj

# def is_equal_obj(obj1, obj2):
#     if type(obj1) != type(obj2):
#         return False
#     if not isinstance(obj1, dict) and not isinstance(obj1, list):
#         return obj1 == obj2
#     if len(obj1) != len(obj2):
#         return False
#     if isinstance(obj1, list):
#         obj1 = sorted(obj1)
#         obj2 = sorted(obj2)
#         for k, v in enumerate(obj1):
#             if not is_equal_obj(v, obj2[k]):
#                 return False
#     else:
#         for k, v in obj1.items():
#             if k not in obj2 or not is_equal_obj(v, obj2[k]):
#                 return False
#     return True


def read2dict(text):
    lines = text.split("\n")
    query_name = ''
    query_content = ''
    query_dict = {}
    for line in lines:
        if 'RUN QUERY' in line:
            query_name = line.split()[2].split('(')[0]
            query_content = ''
        elif query_name != '':
            query_content += line + '\n'
            if line == "}":
                query_dict[query_name] = json.loads(query_content)
    return query_dict


def compare_file(parameters):
    if len(parameters) < 3:
        print("Invalid arguments: " + str(parameters[1:]))
        sys.exit(1)
    """
    compare base file and output file
    return Boolean
    """
    base_dict = read2dict(open(parameters[1]).read())
    out_content = open(parameters[2]).read()
    out_dict = read2dict(out_content)
    if len(base_dict) != len(out_dict):
        print('The number of queries is different')
        sys.exit(2)
    for key, obj in base_dict.items():
        # ignore version if it is "v1", so it can be compatible with old output which has no version info
        if key == "version" and "api" in obj and obj.api == "v1":
            continue
        if key not in out_dict or ordered(obj) != ordered(out_dict[key]):
            print('xxxxxxxxxx ' + key + ' starts xxxxxxxxxxx')
            print(obj)
            print('--------------------')
            if key in out_dict:
                print(out_dict[key])
            print('xxxxxxxxxxxxxxxxxxxxxxxxxxxx')
            sys.exit(3)
    print('The output and base file have no diff')

##############################################
# Arguments:
#   0: this script name
#   1: base file path
#   2: output file path
##############################################
if __name__ == "__main__":
    compare_file(sys.argv)
