#!/usr/bin/python3                                                                                                                                   \
                                                                                                                                                     
import sys
import util

##############################################                                                                                                      \
                                                                                                                                                     
# Arguments:                                                                                                                                        \
                                                                                                                                                     
#   0: this script name                                                                                                                             \
                                                                                                                                                     
#   1: repo list                                                                                                                                    \
                                                                                                                                                     
#   2: default branch                                                                                                                               \
                                                                                                                                                     
##############################################                                                                                                      \
                                                                                                                                                     

if __name__ == "__main__":
    parameters = sys.argv
    if len(parameters) < 3:
        print('Parameter format is not corrected. Usage: python script "repo_list" default_branch')
        sys.exit(1)
    repos = parameters[1]
    default_branch = parameters[2]
    for repo in repos.split(' '):
        if repo:
            print('change default branch for ' + repo)
            util.github_api.change_default_branch(repo, default_branch)
    print('All repo successfully done')
