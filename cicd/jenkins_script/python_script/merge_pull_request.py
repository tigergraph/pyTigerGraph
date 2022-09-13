#!/usr/bin/python
import util
import sys, os.path, time

def mergeable_check(repo_dict):
    """
    check each pull request in repo_dict if it is mergeable.
    If any pull request is not mergeable, abort the program.
    Args:
        repo_dict: a dictionary of repo -> pull requst
    """
    for repo, num in repo_dict.iteritems():
        mergeable,mergeable_state = util.check_pull_request_mergeable(repo, num)
        if not mergeable:
            raise util.MergeFail, repo + '#' + str(num) + ' is not mergeable! Please merge your feature branch with master.mergeable_state from Github: ' + mergeable_state
    # end for
# end function mergeable_check

def merge_pull_request(repo_dict, url, version_file):
    """
    get commit number
    Args:
        repo_dict: a dict of repo -> pull request
        url: jenkins url
        version_file: a file with information of repo, branch, commit
    """
    repo_commits = util.get_branch_sha_dict(open(version_file).read())
    for repo, num in repo_dict.iteritems():
        branch_name = util.get_pull_request_branch_name(repo, num)
        if repo in repo_commits:
            sha = repo_commits[repo]
        elif repo == 'tmd' or repo == 'bigtest':
        # for repos not in gworkspace.sh -c output
            sha = util.get_branch_lastest_commit(repo, branch_name)
        else:
            continue
        print 'merging pull request ' + repo + '#' + num + ' with sha: ' + sha
        util.merge_pull_request(repo, num, url, sha)
        util.delete_branch(repo, branch_name)


    # end for
# end

def main(parameters):
    util.check(len(parameters) == 4, RuntimeError,
        "Invalid arguments: " + str(parameters[1:]))

    dict = util.parse_parameter(parameters, 1)
    # make sure all pull reuqests are mergeable
    mergeable_check(dict)
    # then merge one by one
    merge_pull_request(dict, parameters[2], parameters[3])
# end function main


##############################################
# Arguments:
#   0: this script name
#   1: jenkins parametes, include repo name and pull request number
#   2: jenkins url, this is to record in commit msg
#   3: version file, which contains sha, the commit number
#      we use to test, to ensure no commit after test
##############################################
if __name__ == "__main__":
    try:
        main(sys.argv)
    except Exception, msg:
        # print error to stderr and not exit 1 for Jenkins to check stderr
        util.print_err(str(msg))
