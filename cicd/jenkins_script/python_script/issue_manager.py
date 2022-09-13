#!/usr/bin/python
import util
from util.github_api import get_commits, get_tag_sha
from util.jira_api import open_issue, get_open_issues
import sys, os.path, json, logging

def check_issue_with_label(project, issue_type, label, logger='none'):
    """
    retrieve all the opening issues of given repo with given labels
    Args:
        project: a string, the project name
        issue_type: a string representing the issue type
        label: a string representing the issue label
        logger: logging system to use (must be
                instance of python logging class)
                or 'None' to print output to screen.
                If NOT specified 'None' will be used.
    """
    issues = get_open_issues(project, issue_type, label, logger)
    return issues
# end function check_issue_with_label

def create_issue_if_not_exists(project, title, body, issue_type, label, assignee, top_content, **kwargs):
    """
    create an issue in repo if no issues with the given labels  exist.
    Args:
        project: a string, the project name
        title: a string, the title
        body: a string, the issue content
        issue_type: a string representing the issue type
        label: a string representing the issue label,
        assignee: issue assignee
        top_content: the content be prepended on top of description
    """
    issues = check_issue_with_label(project, issue_type, label)
    # if blocking issue already exists, do not create
    if issues:
        return issues
    # end if
    open_issue(project, issue_type, label, title, top_content + '\n' + body, assignee)
    print ("create QA issues")
    return check_issue_with_label(project, issue_type, label)
# end function create_issue_if_not_exists

def prepare_issue_content(version_filepath, stable_tag_name):
    """
    create issue content.
    1. Read version file
    2. For each repo, get the recent 100 commits and the commit with stable_tag_name
    3. If it does not have the commit with stable_tag_name, record no stable tag
    4. Otherwise, find recent commit author name and date before the stable commit
    Args:
        version_file_path: the path of version_file
        stable_tag_name: stable tag name
    Returns:
        The issue content
    """
    current_commits = open(version_filepath).read()

    # current commits
    content = "Current commits:\n" + current_commits + "\n"

    # get the commits between current commit and last stable tag
    content += 'Commits since last stable of current branch :\n'
    repo_current_commits = util.get_branch_sha_dict(current_commits)

    for repo, sha in repo_current_commits.items():
        recent_commits = get_commits(repo, sha, 100)
        stable_commit = get_tag_sha(repo, stable_tag_name)
        if stable_commit == None:
            content += 'Repo ' + repo + ' has no stable tag yet.\n'
            continue
        # end if

        delta = ''
        for commit in recent_commits:
            if commit['sha'] == stable_commit:
                break
            # end if
            delta += '  ' + commit['sha'] + '\t' + commit['commit']['author']['date'] + \
                '\t' + commit['commit']['author']['name'] + '\n'
        # end for

        if len(delta) > 0:
            content += 'Repo ' + repo + '\n' + delta
    # end for

    return content
# end function prepare_issue_content

def get_issue_urls(issues):
    urls = ''
    for issue in issues:
        urls += '  https://grAphsqL.atlassian.net/browse/' + issue.key + '\n'
    # end for
    return urls
# end function get_issue_urls

def main(parameters):
    '''
    Args:
        0. this file itself
        1. operation type, "check" or "create"

        check:
            2. project name
            3  issue type
            4. labels, separated by commas
            5. logger: logging system to use (must be
                       instance of python logging class)
                       or 'None' to print output to screen.
                       If NOT specified 'None' will be used.
        create:
            2. stable tag name
            3. project name
            4. title
            5. version file path, we use the commit number as body
            6. issue type
            7. label
            8. issue assignee
            9. content on the top
    '''
    operation_type = ["check", "create"]
    util.check(len(parameters) >= 2 and parameters[1] in operation_type, RuntimeError,
        "Invalid arguments: " + str(parameters[1:]))

    if parameters[1] == "check":
        util.check(len(parameters) >= 5, RuntimeError,
           "Invalid arguments: " + str(parameters[1:]))
        if len(parameters) == 5 or not isinstance(parameters[5], logging.Logger):
          parameters.append('none')
        issues = check_issue_with_label(*parameters[2:])
        util.check(len(issues) == 0, util.IssueExistanceFail,
           "QA hourly failure issue not resolved: \n" + get_issue_urls(issues))
        print ("Blocking issue check pass, continue...")
    else:
        util.check(len(parameters) >= 9, RuntimeError,
           "Invalid arguments: " + str(parameters[1:]))
        stable_label = parameters[2]
        params = list(parameters[3:])

        params[2] = prepare_issue_content(params[2], stable_label)

        issues = create_issue_if_not_exists(*params)
        print ("Blocking issue: \n" + get_issue_urls(issues))
    # end if-else
# end function main

##############################################
# Arguments:
#   0: this script name
#   1: tag name
#   2: version_file path
##############################################
if __name__ == "__main__":
    try:
        main(sys.argv)
    except Exception, msg:
        # print error to stderr and not exit 1 for Jenkins to check stderr
        util.print_err(str(msg))
