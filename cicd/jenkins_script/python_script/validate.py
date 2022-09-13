#!/usr/bin/python

import util
import sys, os, os.path, re
from util.jira_api import get_ticket


def validate_path_keyword(repo, num, config):
    diff = util.get_pull_request_diff(repo, num)

    blacklist = config["diff_blacklist"]
    # Due to atlassian migration issue, the site name is still
    # graphsql. We shouldn't block the use of links in atlassian
    whitelist = config["diff_whitelist"]

    diff = diff.lower()
    # remove the word in whitelist so that it does not need to be checked
    for keyword in whitelist:
        diff = diff.replace(keyword, '')

    for line in diff.split('\n'):
        if line.startswith('+'):
            for keyword in blacklist:
                if keyword in line:
                    return False
    return True


def validatePullRequestTitle(repo, num):
    pr = util.get_pull_request_info(repo, num)
    tickets = []
    if 'title' not in pr:
        raise util.validateFail, 'Github response does not contain title, validation failed'
    pr_title = pr['title'].strip().encode("ascii", "replace")
    pull_request_msg = "pull request " + repo + "#" + num
    reg_err = pull_request_msg + " title " + pr_title + " does not match the pattern: " + \
            "ticket type(scope): msg;"
    regPtn = '\[?(?P<ticket>\w+\-\d+)\]?\s+(?P<type>\w+)\s*\((?P<scope>[\w\s]+)\)\s*(:(?P<description>(?:(?!;).)*))?;'
    mutiRegPtn = '^(\s*' + regPtn + ')+$'
    valid_types = ['build', 'ci', 'chore', 'docs', 'feat', 'fix', 'perf', 'refactor', 'style', 'test']

    reg = re.match(mutiRegPtn, pr_title, re.M)
    util.check(reg != None, util.validateFail, reg_err)
    reg = re.compile(regPtn, re.M)
    pr_infos = [m.groupdict() for m in reg.finditer(pr_title)]
    print 'Pull request title is ' + pr_title
    print 'Pull request pattern match info is ' + str(pr_infos)

    for pr_info in pr_infos:
        pr_type = pr_info['type'].lower()
        if pr_type not in valid_types:
            raise util.validateFail, pull_request_msg + " title type should be in " + str(valid_types)
        tickets.append(pr_info['ticket'].upper())
    if not tickets:
        raise util.validateFail, pull_request_msg + " does not contain JIRA ticket number"
    return " ".join(tickets)

def branch_validate(repo, branch, base_branch, config, force):
    error_msg = ''
    regPtn = '^[0-9a-f]{40}$'
    reg = re.match(regPtn, branch, re.M)
    if reg:
      print 'Found commit number as branch name'
      commit = branch
    else:
      commit = util.get_branch_lastest_commit(repo, branch)
    if not commit:
      error_msg = 'Branch ' + branch + ' is not found'
    if base_branch == "default" and repo != "tmd":
      base_branch = branch
    return base_branch, error_msg

# QA-2698
def validate_release_labels(tickets, base_branch):
    labels = {}

    release_label = re.search('^tg_\d+(\.\d+)+', base_branch)
    release_label = release_label.group(0) if release_label else base_branch

    for ticket in tickets.split():
        labels[ticket] = get_ticket(ticket, fields="labels")["labels"]

        tg_labels = set(filter(lambda n: re.match('^tg_\d+(\.\d+)+', n), labels[ticket]))

        # check: multiple release labels
        error_msg = ticket + " JIRA ticket has multiple release labels: " + " ".join(tg_labels)
        util.check(len(tg_labels) <= 1, util.validateFail, error_msg)

        # check: release tag is the same as the expected one
        error_msg = ticket + " JIRA ticket has different release label: " + " ".join(tg_labels) + ', release label: ' + release_label
        util.check(release_label in tg_labels, util.validateFail, error_msg)


def base_validate(repo, num, base_branch, config, force):
    warning_msg = ''
    error_msg = ''
    pr = util.get_pull_request_info(repo, num)
    pull_request_msg = "pull request " + repo + "#" + num

    # check repo name exists and pull request open
    util.check(pr['state'] == 'open', util.validateFail, pull_request_msg + " is not open.")

    # base branch must be correct
    if repo == "tmd":
        util.check(pr['base']['ref'] == "master",
                util.validateFail, pull_request_msg + " base branch must be master")
    else:
        if base_branch == "default":
            base_branch = pr['base']['ref']
        util.check(pr['base']['ref'] == base_branch,
                util.validateFail, pull_request_msg + " base branch must be " + base_branch)

    # validate the pull request contains the base_branch's lastest commit
    if force != "MERGE":
        if not util.check_feature_branch_merged_base(repo, num):
            error_msg += pull_request_msg + " feature branch was not merged with the base branch. "
            #if not util.check_pull_request_mergeable(repo, num):
            #    error_msg += pull_request_msg + " feature branch was not merged with the base branch. "
            #else:
            #    warning_msg += pull_request_msg + " feature branch was not merged with the base branch but will try to mit. "

    # validate the pull request, new code should not contains keyword in blacklist
    if not validate_path_keyword(repo, num, config):
        error_msg += pull_request_msg + " can not contain 'graphsql' keyword. "

    return base_branch, warning_msg, error_msg


def mit_validate(repo, num, base_branch, config, force):
    """
    pre-test validate check, which is done when merge request received
    caller: merge_request_pipeline, 'validate' stage
    1. repo name exists
    2. pull request open
    3. pull request approved by person other than qa
    4. no commit after approved (not checked now)
    5. base branch is correct
    6. pull request is approved by the appropriate code owners (if applicable)

    Args:
        repo: repo name
        num: pull request number
        base_branch: expected base branch to be merged into
    """
    base_branch, warning_msg, error_msg = base_validate(repo, num, base_branch, config, force)

    pull_request_msg = "pull request " + repo + "#" + num

    # last commit is approved by person other than qa

    pr_owner = util.get_pull_request_owner(repo, num)
    if ( pr_owner == "" ):
        error_msg += pull_request_msg + " does not have owner. "
    approved, change_requests = util.get_pull_request_approvers(repo, num)
    approved_by_code_owners, pr_files = util.check_pull_request_approved_by_code_owners(repo, num)
    special_approvers = config['special_approvers']
    repo_approvers = config['repo_approvers']
    repo_file_approvers = config['repo_file_approvers']
    if force != "WIP":
      ticket_list = validatePullRequestTitle(repo, num)

      # QA-2698
      validate_release_labels(ticket_list, base_branch)

      if len(approved) == 0:
          error_msg += pull_request_msg + " is not approved. "
      if len(change_requests) != 0:
          change_request_str = ''
          for i, request in enumerate(change_requests):
              if len(change_requests) == 1:
                  change_request_str += str(request)
              elif i != len(change_requests) - 1:
                  change_request_str += str(request) + ", "
              else:
                  change_request_str += "and/or " + str(request)

          #w/a some old reviews
          if change_request_str != "zzhuang6":
             warning_msg += pull_request_msg + " still has change requests from " + change_request_str + \
             " that has not been resolved yet. Please either dismiss their review or have them approve the pull request again."

      if len (approved_by_code_owners) != 0:
          needed_codeowners_str = ''
          for i, codeowner in enumerate(approved_by_code_owners):
              if  len(approved_by_code_owners) == 1:
                  needed_codeowners_str += str(codeowner)
              elif i != len(approved_by_code_owners) - 1:
                  needed_codeowners_str += str(codeowner) + ", "
              else:
                  needed_codeowners_str += "and/or " + str(codeowner)

          error_msg += pull_request_msg + " still requires approval from code owners " + needed_codeowners_str + ". "

    special_approved = False
    #Repo approver only applies to internal release tg_yy.mm_dev
    repo_approved = False

    approved.append(pr_owner)
    irPtn = "^tg_\d+\.\d+_dev$"
    reg = re.match(irPtn, base_branch, re.M)
    for ap in approved:
        if ap in special_approvers:
            special_approved = True
        if reg != None and repo in repo_approvers and ap in repo_approvers[repo]:
            repo_approved = True
    if special_approved == True or reg != None and repo_approved == True:
        return base_branch, warning_msg, error_msg, special_approved, repo_approved, ticket_list

    needed_repo_approvers = ""
    if reg != None:
        needed_file_approvers = ""
        if repo in repo_approvers:
            needed_repo_approvers = ", ".join(repo_approvers[repo])
        if repo_approved == False and pr_files and repo in repo_file_approvers:
            #pr_files = util.get_pull_request_files(repo, num)
            for rule in repo_file_approvers[repo]:
                match_res, match_set = util.check_rule_match(rule, pr_files)
                if match_res:
                    if any(item in approved for item in repo_file_approvers[repo][rule]):
                        pr_files = pr_files.difference(match_set)
                    else:
                        needed_file_approvers += " " + rule + ": " + ", ".join(repo_file_approvers[repo][rule])
            if not pr_files:
                repo_approved = True
        if repo_approved == False:
            if needed_repo_approvers and needed_file_approvers:
                needed_repo_approvers += " or" + needed_file_approvers
            else:
                needed_repo_approvers += needed_file_approvers

    if special_approved == False and repo_approved == False:
        if reg != None and needed_repo_approvers:
            error_msg += pull_request_msg + " still requires approval from repo approvers " + needed_repo_approvers + ". "
        else:
            error_msg += pull_request_msg + " still requires approval from special approvers " + ", ".join(special_approvers) + ". "

    return base_branch, warning_msg, error_msg, special_approved, repo_approved, ticket_list
# end function mit_validate

def wip_validate(repo, num, base_branch, config, force):
    """
    wip-test validate check, which is done when merge request received
    caller: merge_request_pipeline, 'validate' stage
    1. repo name exists
    2. pull request open

    Args:
        repo: repo name
        num: pull request number
    """
    if num.isdigit():
        branch, warn_msg, err_msg =  base_validate(repo, num, "default", config, force)
        if base_branch == "default":
            base_branch = branch
        msg = warn_msg + err_msg
    else:
        branch, msg =  branch_validate(repo, num, "default", config, force)
        if base_branch == "default":
            base_branch = branch
    return base_branch, msg
#end function wip_validate

# validate check, which is done when merge request start to test
# caller: gworkspace.py
# 1. repo name exists
# 2. pull request open
# 3. pull request approved by person other than qa
# 4. no commit after approved
# 5. also tag a 'pending' status to the head commit

# post-test validate check, which is done after test pass before merge
# caller: merge_pull_request_job
# 1. pull request approved by qa
# 2. no commit after last 'pending' status, and change status to 'success'


def validate(parameters):
    """
    Args:
    0: this script name
    1: jenkins parameters
    2: validate state: PRE IN POST WIP
    3: base_branch
    """
    util.check(len(parameters) >= 6, RuntimeError,
        "Invalid arguments: " + str(parameters[1:]))
    dict = util.parse_parameter(parameters, 1)
    print('Repository and pull requests: ' + str(dict))

    state = parameters[2]
    force = parameters[3]
    base_branch = parameters[4]
    mark_tag_file = parameters[5]
    warning_msg = ""
    error_msg = ""
    tickets = ""

    config = util.read_total_config()

    special_approved = True
    repo_approved = True
    for repo, num in dict.iteritems():
        print("Checking '%s#%s':"%(repo,num))
        if state == 'MIT' or force == 'ALL':
            base_branch, wm, em, sap, rap, tickets = mit_validate(repo, num, base_branch, config, force)
            special_approved = special_approved & sap
            repo_approved = repo_approved & rap
            warning_msg +=wm
            error_msg += em
        elif state == 'IN':
            return
        elif state == 'POST':
            return
        elif state == 'WIP':
            base_branch, wm = wip_validate(repo, num, base_branch, config, force)
            warning_msg += wm
        else:
            raise RuntimeError, "Invalid argument: " + state
        # end if
    # end for

    if base_branch == "default":
        base_branch = "master"
        warning_msg += " No base branch specified, using master by default."
    #util.check(base_branch != "default", util.validateFail,
    #        "You must specify base branch because no pull request is specified")

    util.run_bash('echo "' + base_branch + '" > ' + mark_tag_file)
    util.run_bash('echo "' + str(special_approved) + '" >> ' + mark_tag_file)
    util.run_bash('echo "' + str(repo_approved) + '" >> ' + mark_tag_file)
    util.run_bash('echo "' + str(tickets) + '" >> ' + mark_tag_file)

    if state == 'MIT':
        if error_msg != "":
            util.check(force == 'true', util.validateFail, error_msg)
            warning_msg += error_msg + " But this will not block you due to force = true"
    if state == 'WIP':
        if warning_msg != "":
            warning_msg += " But this will not block this WIP"

    return warning_msg
# end function validate

##############################################
# Arguments:
#   0: this script name
#   1: jenkins parametes, include repo name and pull request number
#   2: validate state: PRE IN POST WIP
#   3: base branch name: e.g. master
##############################################
if __name__ == "__main__":
    try:
        warning_msg = validate(sys.argv)
        if warning_msg != "":
            util.print_err("WARNING: " + str(warning_msg))
    except Exception, msg:
        # print error to stderr and not exit 1 for Jenkins to check stderr
        util.print_err(str(msg))
