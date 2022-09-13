#!/usr/bin/python3                                                                                                                                    
# Github manager. 1. Create/delete branch/tag.  2. Add/delete protection for branch                                                                  
#                                                                                                                                                    

import util
import sys, os.path, re

def get_commits_dict(input_format, input_content, repo_list_file, method):
    branch_signatures = {}
    print(('Get commits from ' + input_format))
    if input_format == 'file':
        if not os.path.exists(input_content[0]):
            raise util.AdvanceTagFail("version file does not exist")
        branch_signatures = util.get_branch_sha_dict(open(input_content[0]).read())
    elif input_format in ['branch', 'tag', 'params']:
        branches = util.get_default_branches(repo_list_file, input_content[0])

        # if method is not 'create' or 'create_release', do not need get sha                                                                         
        if method != 'create' and method != 'create_release':
            return branches

        if input_format == 'params' and len(input_content) > 1:
            p_dict = util.parse_parameter(input_content, 1)
            print('Repository and pull requests: ' + str(p_dict))

            for repo, num in p_dict.items():
                if repo in branches:
                    branches[repo] = util.get_pull_request_branch_name(repo, num)
            print('Repository and branches: ' + str(branches))

        regPtn = '^[0-9a-f]{40}$'
        for repo, input_ct in branches.items():
            print('To get commit for ' + repo + ' ' + input_format + ' ' + input_ct)
            commit = ""
            if input_format == "branch" or input_format == "params":
              reg = re.match(regPtn, input_ct, re.M)
              if reg:
                print('Using commit number as branch name')
                commit = input_ct
              else:
                commit = util.get_branch_lastest_commit(repo, input_ct)
            elif input_format == "tag":
                commit = util.get_tag_sha(repo, input_ct)
                if commit == None:
                    raise util.AdvanceTagFail("No " + input_ct + " for " + repo)
            print('Fetched commits for ' + repo + ': ' + commit)
            branch_signatures[repo] = commit
            print('Fetched commits for branches: ' + str(branch_signatures))
    else:
        raise util.AdvanceTagFail("Input format is Invalid")
    return branch_signatures


def branch_manager(branch_signatures, method, branch_name):
    for repo, sha in branch_signatures.items():
        print('To ' + method + ' for ' + repo + ' branch ' + branch_name)
        if method == 'create' or method == 'create_release':
            util.create_branch(repo, sha, branch_name)
            if method == 'create_release' and repo == 'product':
                tmp_product_fd="/tmp/cloned_product"
                print('Start to do git clone for product')
                util.do_git_clone(repo, tmp_product_fd, branch_name=branch_name, git_option='--depth=1')
                print('Start to change config/proj.config')
                util.run_bash("""                                                                                                                    
                    cd '{}';                                                                                                                         
                    sed -E -i 's/(- [^ ]*[ ]*[^ ]*[ ]*)([^ ]*)([ ]*)([^ ]*)([ ]*src)/\\1{}\\3\\4\\5/g' config/proj.config;                           
                    version=$(echo '{}' | grep -e grep -e '^tg_[0-9]\+\.[0-9]\+\(\.[0-9]\+\)\?_dev$' | cut -d'_' -f2) && [ -n "$version" ] && echo "\
$version" > product_version && git add product_version                                                                                               
                    git commit -am "Update config" --no-verify;                                                                                      
                    git push origin {};                                                                                                              
                """.format(tmp_product_fd, branch_name, branch_name, branch_name))
        elif method == 'delete':
            util.delete_branch(repo, branch_name)
        elif method == "add_protection":
            util.add_branch_restriction(repo, branch_name)
        elif method == "check_protection":
            util.get_branch_restriction(repo, branch_name)
        elif method == "delete_protection":
            util.remove_branch_restriction(repo, branch_name)
        elif method == "enable_approval":
            util.enable_branch_approval(repo, branch_name)
        else:
            raise util.AdvanceTagFail("Invalid method argument")


def tag_manager(branch_signatures, method, tag_name):
    for repo, sha in branch_signatures.items():
        print('To ' + method + ' for ' + repo + ' tag ' + tag_name)
        if method == 'create':
            util.tag_branch_as_stable(repo, sha, tag_name)
        elif method == 'delete':
            util.delete_tag(repo, tag_name)
        else:
            raise util.AdvanceTagFail("Invalid method argument")


def main(parameters):
    util.check(len(parameters) >= 7, RuntimeError,
        "Invalid arguments: " + str(parameters[1:]))
    method = parameters[1]
    repo_list_file = parameters[2]
    ref_type = parameters[3]
    ref_name = parameters[4]
    input_format = parameters[5]
    input_content = parameters[6:]

    if len(input_content) < 1:
        raise util.AdvanceTagFail("Invalid input_content argument")

    print('start github_manager')
    # get commits dict from input (version file or tag name or branch name)                                                                          
    branch_signatures = get_commits_dict(input_format, input_content, repo_list_file, method)
    print('branch commits are obtained')

    if ref_type == "tag":
        tag_manager(branch_signatures, method, ref_name)
    elif ref_type == "branch":
        branch_manager(branch_signatures, method, ref_name)
    else:
        raise util.AdvanceTagFail("Invalid ref_type argument")
# end function main               

##############################################                                                                                                       
# Arguments:                                                                                                                                         
#   0: this script name                                                                                                                              
#   1: ref type (tag or branch)                                                                                                                      
#   2: ref name                                                                                                                                      
#   3: input_format (file or branch or tag)                                                                                                          
#   4: input_content (version file path or branch name or tag name)                                                                                  
#   5: method                                                                                                                                        
##############################################                                                                                                       
if __name__ == "__main__":
    try:
        main(sys.argv)
    except Exception as msg:
        # print error to stderr and not exit 1 for Jenkins to check stderr                                                                           
        util.print_err(str(msg))
