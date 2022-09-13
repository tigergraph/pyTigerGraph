#!/usr/bin/python
import util
import sys

def main(parameters):
    dict = util.parse_parameter(parameters, 1)
    repo_name = parameters[2]
    for repo, num in dict.iteritems():
        if repo.lower() == repo_name:
            branch = util.get_pull_request_branch_name(repo.lower(), num)
            return branch
        # end if
    # end for
    return parameters[3]
# end function main

##############################################
# Arguments:
#   0: this script name
#   1: jenkins parametes, include repo name and pull request number
#   2: repo name
#   3: base/default branch
##############################################
if __name__ == "__main__":
    print main(sys.argv)
