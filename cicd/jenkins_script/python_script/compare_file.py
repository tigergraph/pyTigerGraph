import os, sys, re
import util

keys = ["Vertex Types", "Edge Types", "Graphs", "Jobs", "Queries"]
# name(vertex, edge, graph, job, query) position after '-'
name_pos = [2, 3, 2, 4, 1]

def getLines(text):
    """
    get an array for names of vertex, edge, graphs
    like [['vertex1', 'vertex2'], ['edge1', 'edge2'], .....]
    Args:
        text: the text of gsql ls
    return and 2d array
    """
    lines = text.split("\n")
    stage = -1
    res = []
    for line in lines:
      for i, key in enumerate(keys):
        if key + ':' in line:
          # if key is met, mark current stage
          stage = i
          res.append([])
          break
      # if a new line start with -
      if len(line.split()) >= 1 and line.split()[0] == '-':
        # split this line by space and (
        words = re.split(' |\(', line)
        # get the schema(vertex, edge, job ...) name
        res[stage].append(words[words.index('-') + name_pos[stage]])
    return res


def compare(parameters):
    util.check(len(parameters) == 3, RuntimeError,
        "Invalid arguments: " + str(parameters[1:]))
    """
    compare arrays of two "gsql ls" schema
    Args:
        parameters: parameters of the input. parameters[1]: old file, parameters[2]: new file
    return and 2d array
    """
    prev_schema = getLines(open(parameters[1]).read())
    new_schema = getLines(open(parameters[2]).read())
    for i in range(0, len(prev_schema)):
        for ps in prev_schema[i]:
            if i >= len(new_schema) or ps not in new_schema[i]:
                return False
    return True

##############################################
# Arguments:
#   0: this script name
#   1: old file path
#   2: new file path
##############################################
if __name__ == "__main__":
    if not compare(sys.argv):
        print 'two schema compare failed. compatible test failed'
        sys.exit(1)
    print 'two schema compare successed'    
