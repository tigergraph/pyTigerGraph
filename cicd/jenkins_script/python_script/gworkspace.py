#!/usr/bin/python
import util
import sys, os.path, re, os
from github_manager import tag_manager

def update_project_config(branches, tag):
    cmd = "sed -E -i '"
    cmd += "s/lib\/gle/src\/gle/;s/glelib/gle/;"
    for repo, branch in branches.iteritems():
        cmd += "s/(- [^ ]*[ ]*{}[ ]*)[^ ]*([ ]*)[^ ]*([ ]*src)/\\1{}\\2{}\\3/;".format(repo, branch, tag)
    # end for
    cmd = cmd + "' config/proj.config"
    util.run_bash(cmd)
# end function update_project_config

def write_tag_sha_to_version_file(repo, tag, version_file):
    """
    write gium sha to version file
    Args:
        repo: repo name
        tag: tag name
        version_file: version_file path
    """
    print 'write ' + repo + ' ' + tag + ' to version_file'
    sha = util.get_tag_sha(repo, tag)
    date = util.get_commit_from_sha(repo, sha)['commit']['committer']['date']
    info = repo + '                 ' + \
            tag + '               ' + \
            sha + ' ' + date
    util.run_bash('echo "' + info + '" >> ' + version_file)
# end function write_gium_sha_to_version_file

def write_branch_sha_to_version_file(repo, branch, version_file):
    """
    write gium sha to version file
    Args:
        repo: repo name
        branch: branch name
        version_file: version_file path
    """
    print 'write ' + repo + ' ' + branch + ' to version_file'
    sha = util.get_branch_lastest_commit(repo, branch)
    date = util.get_commit_from_sha(repo, sha)['commit']['committer']['date']
    info = repo + '                 ' + \
            branch + '               ' + \
            sha + ' ' + date
    util.run_bash('echo "' + info + '" >> ' + version_file)
# end function write_gium_sha_to_version_file

def main(parameters):
    util.check(len(parameters) >= 6, RuntimeError,
        "Invalid arguments: " + str(parameters[1:]))

    log_dir = parameters[1]
    log_file = util.prepare_log(log_dir, 'gworkspace.log')
    version_file = parameters[2]
    mark_tag_name = parameters[3]
    base_branch = parameters[4]
    repo_list_file = parameters[5]

    #Get gworkspace credentials from Jenkins
    git_user = os.getenv('MIT_GW_USER', '')
    git_token = os.getenv('MIT_GW_TOKEN', '')

    cur_dir = os.path.dirname(os.path.abspath(__file__))
    product_dir = os.environ['PRODUCT']
    if not product_dir:
        product_dir = os.path.expanduser("~") + "/product"
    #Remove product directory if no gworkspace.sh detected in it (i.e. it's product from preivious gtestspace)
    util.run_bash("sudo rm -rf /tmp/cmake_build; [ -d {}/cmakd_build ] && mv {}/cmake_build /tmp; sudo rm -rf {} || true".format(product_dir,product_dir,product_dir))
    if not os.path.isdir(product_dir) or not os.path.isfile(product_dir + '/gworkspace.sh'):
        #Use graphTester user and not qa-tigergraph for gworkspace to avoid hitting GitHub API limit
        #No need to make product beforehand as git clone wil fail if it's already there.
        util.run_bash("GIT_TRACE=1 git clone --progress https://{}:{}@github.com/tigergraph/product.git {} &>> {}".format(git_user, git_token, product_dir, log_file))

    #Make sure tigergraph has write access to all files under product directory
    #Add sudo as some files on tester machines are NOT owned by graphsql
    #(will remove when gworkspace is remove from testing machines)
    util.run_bash("sudo chmod u+w -R {} &>> {}".format(product_dir, log_file));
    os.chdir(product_dir)

    util.check(os.path.isfile('gworkspace.sh'), RuntimeError,
        "Invalid working directory, gworkspace.sh doesn't exist")

    branches = util.get_default_branches(repo_list_file, base_branch)

    # update product
    cmd = '(git reset --hard && git prune && git fetch --all --prune -ft && git checkout ' + \
        mark_tag_name + ') &>> ' + log_file
    util.run_bash(cmd)
    util.run_bash("git branch &>> {}".format(log_file))

    update_project_config(branches, mark_tag_name)
    util.run_bash("rm -rf ./src/gle_compile")
    # restore cmake_build
    util.run_bash("[ ! -d ./cmake_build ] && [ -d /tmp/cmake_build ] && mv /tmp/cmake_build /tmp || true")

    #Use graphTester user and not qa-tigergraph for gworkspace to avoid hitting GitHub API limit
    #Use gworkspace -m (clone new repos and update existing) in mit instead of -r
    util.run_bash("./gworkspace.sh -m https {} {} &>> {}".format(git_user, git_token, log_file))

    if not os.path.exists(version_file):
        util.run_bash('./gworkspace.sh -c &> ' + version_file)
        #TODO: revise
        if base_branch == "master":
            major_version = "3"
        else:
            major_version = base_branch.split("_")[1].split(".")[0]
        print 'Product version' + major_version
        if int(major_version) <= 2:
            # print gium commit to version file so that merge_job can get the gium sha
            write_tag_sha_to_version_file('gium', mark_tag_name, version_file)

    #Try to merge with base branch
    #merge_branch_script = os.path.join(cur_dir, '../shell_script/merge_branch.sh')
    #util.run_bash("bash {} {} &>> {}".format(merge_branch_script, os.getenv("BASE_BRANCH", mark_tag_name), log_file))

    #w/a for gle compilation conflicts in gle between 2.4.x and 2.5.0
    util.run_bash("git clean -df -e cmake_build")

    # Check if ProGuard has any note/warning/error. If so, raise RuntimeError.
    # gle < 2.5.0 has ProGuard warning that cannot be fixed easily so we simply skip this check.
    # gradlew exists under gle >= 2.5.0
    if os.path.exists(os.path.join(product_dir, 'src/gle/gradlew')):
      # This is part of ProGuard's Troubleshooting page url.
      proguard_troubleshoot = 'proguard.sourceforge.net/manual/troubleshooting.html'
      # If ProGuard has any note/warning/error, it leaves the url in gworkspace.log
      util.check(proguard_troubleshoot not in open(log_file).read(), RuntimeError,
          'Problem with ProGuard configuration; check gworkspace.log for more detail')
# end function main

##############################################
# Arguments:
#   0: this script name
#   1: log directory
#   2: version file
#   3: mark_tag_name
#   4: base_branch
##############################################
if __name__ == "__main__":
    try:
        main(sys.argv)
    except Exception, msg:
        # print error to stderr and not exit 1 for Jenkins to check stderrp
        print str(msg)
        util.print_err(str(msg))
