#!/usr/bin/python3

import util
import sys, os, os.path, re, base64
from copy import deepcopy
from util.jira_api import JiraApi


jira = None

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
    tickets = set()
    if 'title' not in pr:
        raise util.validateFail('Github response does not contain title, validation failed')
    # QA-2698
    pr_title = pr['title'].strip()   # .encode("ascii", "replace")
    pull_request_msg = "pull request " + repo + "#" + num
    reg_err = pull_request_msg + " title " + pr_title + " does not match the pattern: " + \
            "ticket type(scope): msg;"
    # QA-2722
    regPtn = '(?P<tickets>((\[)?(\w+\-\d+)\s*(\]|,)?\s*)+)\s+(?P<type>\w+)\s*\((?P<scope>[\w\s]+)\)\s*(:(?P<description>(?:(?!;).)*))?;'
    
    mutiRegPtn = '^(\s*' + regPtn + ')+$'
    valid_types = ['build', 'ci', 'chore', 'docs', 'feat', 'fix', 'perf', 'refactor', 'style', 'test']

    reg = re.match(mutiRegPtn, pr_title, re.M)
    util.check(reg != None, util.validateFail, reg_err)
    reg = re.compile(regPtn, re.M)
    pr_infos = [m.groupdict() for m in reg.finditer(pr_title)]

    # QA-2722
    regPtn = '\s*(\[|)?\s*(?P<ticket>\w+\-\d+)(\s*(\]|,)?\s*)'
    reg = re.compile(regPtn, re.M)
    for i, d in enumerate(pr_infos):
        if "tickets" in d:
            pr_infos[i]["tickets"] = [m.group("ticket") for m in re.finditer(regPtn, d["tickets"])]

    print('Pull request title is ' + pr_title)
    print('Pull request pattern match info is ' + str(pr_infos))

    for pr_info in pr_infos:
        pr_type = pr_info['type'].lower()
        if pr_type not in valid_types:
            raise util.validateFail(pull_request_msg + " title type should be in " + str(valid_types))
        for ticket in pr_info['tickets']:
            tickets.add(ticket.upper())
    if not tickets:
        raise util.validateFail(pull_request_msg + " does not contain JIRA ticket number")
    return tickets

def branch_validate(repo, branch, base_branch, config, force):
    error_msg = ''
    regPtn = '^[0-9a-f]{40}$'
    reg = re.match(regPtn, branch, re.M)
    if reg:
      print('Found commit number as branch name')
      commit = branch
    else:
      commit = util.get_branch_lastest_commit(repo, branch)
    if not commit:
      error_msg = 'Branch ' + branch + ' is not found'
    if base_branch == "default" and repo != "tmd":
      base_branch = branch
    return base_branch, error_msg

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
            error_msg += " " + pull_request_msg + " needs to merge/rebase with the latest " + base_branch + " branch."
            #if not util.check_pull_request_mergeable(repo, num):
            #    error_msg += pull_request_msg + " feature branch was not merged with the base branch. "
            #else:
            #    warning_msg += pull_request_msg + " feature branch was not merged with the base branch but will try to mit. "

    # validate the pull request, new code should not contains keyword in blacklist
    if not validate_path_keyword(repo, num, config):
        error_msg += pull_request_msg + " can not contain 'graphsql' keyword. "

    return base_branch, warning_msg, error_msg

# QA-2698
def validate_release_labels(tickets, jira_contents, product_version):
    error_msg = ""
    labels = {}

    if not isinstance(product_version, str):
        product_version = str(product_version)

    #release_label = re.search('^tg_\d+(\.\d+)+', base_branch)
    #release_label = release_label.group(0) if release_label else base_branch
    release_label = "tg_" + product_version
    
    for ticket_id in tickets:
        labels[ticket_id] = jira_contents[ticket_id]["labels"]

        tg_labels = set(filter(lambda n: re.match('^tg_\d+(\.\d+)+', n), labels[ticket_id]))

        if tg_labels:
            # check: multiple release labels
            if len(tg_labels) > 1:
                error_msg = ticket_id + " JIRA ticket has multiple release labels: " + " ".join(tg_labels) + ". "

            # check: release tag is the same as the expected one
            if release_label not in tg_labels:                
                error_msg = ticket_id + " JIRA ticket has different release label: " + " ".join(tg_labels) + ', expected release label is: ' + release_label + ". "

    return error_msg

# QA-2736
def validate_ticket_status(tickets, jira_contents):
    error_msg = ""
    d_status = {}

    for ticket_id in tickets:
        d_status[ticket_id] = jira_contents[ticket_id]["status"]['name'].upper()

        # check: status is 'NO TEST' | 'TEST DONE'
        if d_status[ticket_id].upper() not in {'NO TEST', 'TEST DONE'}:
            error_msg = ticket_id + " JIRA ticket needs to be in status NO TEST or TEST DONE to merge, current status is " + d_status[ticket_id]  + ". "
    
    return error_msg

# QA-2738
# https://graphsql.atlassian.net/rest/api/3/issuetype
def validate_associated_ticket(tickets, jira_contents):
    error_msg = ""
    type_set = {'EPIC', 'NEW FEATURE', 'STORY', 'IMPROVEMENT'}
    status_set = {'NO TEST'}

    qa_tests = deepcopy(tickets)
    for ticket_id in tickets:
        if ticket_id.startswith("QA"):
          qa_tests.remove(ticket_id)
          continue
        ticket = jira_contents[ticket_id]

        if (ticket['issuetype']['name'].upper() not in type_set) or (ticket['status']['name'].upper() in status_set):
            qa_tests.remove(ticket_id)
            continue

        issuelinks = ticket["issuelinks"]

        linked_tickets = []        
        for issuelink in issuelinks:
            if 'inwardIssue' in issuelink:
                issue = issuelink['inwardIssue']
            elif 'outwardIssue' in issuelink:
                issue = issuelink['outwardIssue']

            linked_tickets.append(issue['key'])

            if 'fields' in issue:
                if 'issuetype' in issue['fields']:
                    issuetype_name = issue['fields']['issuetype']['name']

                    if issuetype_name.upper() == 'QA TEST':
                        if ticket_id in qa_tests:
                            qa_tests.remove(ticket_id)

    # check: there is at least one linked ticket of type QA Test
    if qa_tests:        
        error_msg = " ".join(qa_tests) + " feature ticket needs a QA ticket in type QA Test linked in order to merge. "

    return error_msg

def validate_jira_tickets(tickets, product_version, force):
    error_msg = ""
    jira_contents = {}

    blocker_ticket = True if tickets else False
    for ticket_id in tickets:
        jira_contents[ticket_id] = jira.get_issue_content(ticket_id)
        if jira_contents[ticket_id]['priority']['name'] not in {'Blocker', 'Customer-Blocker'}:
           blocker_ticket = False
    if blocker_ticket:
        error_msg += "(Waived for blocker tickets.)"

    if force != "MERGE":
        # QA-2698
        error_msg += validate_release_labels(tickets, jira_contents, product_version)
        # QA-2736
        error_msg += validate_ticket_status(tickets, jira_contents)
        # QA-2738
        error_msg += validate_associated_ticket(tickets, jira_contents)

    return error_msg, blocker_ticket

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
    approved_by_code_owners = util.check_pull_request_approved_by_code_owners(repo, num)
    special_approvers = config['special_approvers']
    repo_approvers = config['repo_approvers']
    repo_file_approvers = config['repo_file_approvers']
    if force != "WIP":
      ticket_set = validatePullRequestTitle(repo, num)

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
    reg = re.match(irPtn, base_branch, re.M) != None or base_branch == "master"
    for ap in approved:
        if ap in special_approvers:
            special_approved = True
            repo_approved = True
        if reg == True and repo in repo_approvers and ap in repo_approvers[repo]:
            repo_approved = True
    if special_approved == True or reg != None and repo_approved == True:
        return base_branch, warning_msg, error_msg, special_approved, repo_approved, ticket_set

    needed_repo_approvers = ""
    if reg == True:
        needed_file_approvers = ""
        if repo in repo_approvers:
            needed_repo_approvers = ", ".join(repo_approvers[repo])
        pr_files = util.get_pull_request_files(repo, num)
        if repo_approved == False and pr_files and repo in repo_file_approvers:            
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
        if reg == True and needed_repo_approvers:
            error_msg += pull_request_msg + " still requires approval from repo approvers " + needed_repo_approvers + ". "
        else:
            error_msg += pull_request_msg + " still requires approval from special approvers " + ", ".join(special_approvers) + ". "

    return base_branch, warning_msg, error_msg, special_approved, repo_approved, ticket_set
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
        branch, warn_msg, err_msg = base_validate(repo, num, "default", config, force)
        if base_branch == "default":
            base_branch = branch
        msg = warn_msg + err_msg
    else:
        branch, msg = branch_validate(repo, num, "default", config, force)
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

    global jira
    jira = JiraApi()

    state = parameters[2]
    force = parameters[3]
    base_branch = parameters[4]
    mark_tag_file = parameters[5]
    warning_msg = ""
    error_msg = ""
    tickets = set()
    product_branch = ""

    config = util.read_total_config()

    special_approved = True
    repo_approved = True
    if state == 'MIT' or force == 'ALL':
        for repo, num in dict.items():
            print("Checking '%s#%s':"%(repo,num))
            base_branch, wm, em, sap, rap, ticket_set = mit_validate(repo, num, base_branch, config, force)
            special_approved = special_approved & sap
            repo_approved = repo_approved & rap
            tickets |= ticket_set
            warning_msg +=wm
            error_msg += em
            if repo == "product":
                product_branch = base_branch
        # end for

    elif state == 'IN':
        return
    elif state == 'POST':
        return
    elif state == 'WIP':
        for repo, num in dict.items():
            print("Checking '%s#%s':"%(repo,num))
            base_branch, wm = wip_validate(repo, num, base_branch, config, force)
            warning_msg += wm
            if repo == "product":
                product_branch = base_branch
        # end for
    else:
        raise RuntimeError("Invalid argument: " + state)
    # end if

    if base_branch == "default":
        base_branch = "master"
        warning_msg += " No base branch specified, using master by default."
    #util.check(base_branch != "default", util.validateFail,
    #        "You must specify base branch because no pull request is specified")
    if product_branch == "":
        product_branch = base_branch
    print("Fetching product version from product repo using branch: " + product_branch)
    version_file = util.get_file_content("product", product_branch, "product_version")
    print("Version: " + version_file)
    # Need to remove trailing newline
    product_version = ''.join(version_file.splitlines())

    if state == 'MIT' or force == 'ALL':
        em, sap = validate_jira_tickets(tickets, product_version, force)
        special_approved = special_approved | sap
        error_msg += em

    util.run_bash(f'echo {base_branch} > {mark_tag_file}')
    util.run_bash(f'echo {product_version} >> {mark_tag_file}')
    util.run_bash(f'echo {special_approved} >> {mark_tag_file}')
    util.run_bash(f'echo {repo_approved} >> {mark_tag_file}')
    util.run_bash(f'echo {" ".join(tickets)} >> {mark_tag_file}')

    if state == 'MIT':
        if error_msg != "":
            util.check(force == 'true', util.validateFail, error_msg)
            warning_msg += error_msg + " But this will not block you due to force = true"
    if state == 'WIP':
        if warning_msg != "":
            warning_msg += " It will not block this WIP but could cause build or test failure."

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
    except Exception as msg:
        # print error to stderr and not exit 1 for Jenkins to check stderr
        util.print_err(str(msg))
