#!/usr/bin/python3


from util.jira_api import JiraApi
import util
import argparse
from subprocess import check_output
from os import path


def commit_diff(version_filepath, stable_version_filepath):
    command = f"diff {version_filepath} {stable_version_filepath} | grep '>'"
    diff = check_output(["bash", "-c", command],universal_newlines=True)
    return diff

def prepare_issue_content(version_filepath, diff_stable_file_path):
    """
    create issue content.
    1. Read version file
    2. Read version diff file with stable tag
    Args:
        version_file_path: the path of version_file
        diff_stable_file_path: the path of diff_commit_file, default under build*/diff_stable
    Returns:
        The issue content
    """
    current_commits = open(version_filepath).read()

    # current commits
    content = "*Current Commits*:\n {noformat} \n" + current_commits + "\n {noformat} \n"

    # get the commits between current commit and last stable tag
    content += "*New Commits after Stable Tag*:\n {noformat} \n"
    diff_commits = open(diff_stable_file_path).read()
    content += diff_commits + "\n {noformat} \n"

    return content
# end function prepare_issue_content

def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest='action', help='Please choose a command: check or create')
    ## arguments used to create Jira issues
    create_parsers = subparsers.add_parser('create', help='"create jira issues" help')
    create_parsers.add_argument('summary', type=str, help='Jira issue summary')
    create_parsers.add_argument('description', type=str, help='Jira issue description')
    create_parsers.add_argument('version', type=str, help='TigerGraph base branch version')
    create_parsers.add_argument('path', type=str, help='TigerGraph version log path')
    ## Optional argument
    create_parsers.add_argument('--project', default='', help='Jira project key, default QA')
    create_parsers.add_argument('--issuetype',default='', help='Jira issue type, default Bug')
    create_parsers.add_argument('--assignee',type=str, default='', help='Jira issue assignee')
    create_parsers.add_argument('--labels',type=str, default='', help='Jira issue labels')
    create_parsers.add_argument('--priority',type=str, default='', help='Jira issue priority')

    ## arguments used to qeury Jira issue
    query_parsers = subparsers.add_parser('query', help='"query jira issues" help')
    query_parsers.add_argument('issues', type=str, help='Jira issue key to update')
    ## Optional argument
    query_parsers.add_argument('--labels',type=str, default='', help='Jira issue labels')

    ## arguments used to update Jira issue
    update_parsers = subparsers.add_parser('update', help='"update jira issues" help')
    update_parsers.add_argument('issues', type=str, help='Jira issue key to update')
    update_parsers.add_argument('field', type=str, help='Jira field name to update')
    update_parsers.add_argument('value', type=str, help='Jira field value to update')
    ## Optional argument
    update_parsers.add_argument('--labels',type=str, default='', help='Jira issue labels')

    ## arguments used to query Jira block issue
    check_parsers = subparsers.add_parser('check', help='"check jira issues" help')
    check_parsers.add_argument('version', type=str, help='TigerGraph base branch version')
    ## Optional argument
    check_parsers.add_argument('--project', default='', help='Jira project key, default QA')
    check_parsers.add_argument('--issuetype',default='', help='Jira issue type, default Bug')
    check_parsers.add_argument('--labels',type=str, default='', help='Jira issue labels')
    args = parser.parse_args()
    labels = [] if not args.labels else args.labels.split(" ")

    jira = JiraApi()
    if args.action == "check":
        issues = jira.get_filter_issues(args.project, args.issuetype,labels,args.version)
        if not issues or len(issues) == 0 :
            print ("Blocking issue check pass, continue...")
        else:
            try:
                raise RuntimeError('Issue exist')
            except Exception:
                util.util.print_err("QA hourly failure issue is not resolved: \n" + jira.get_urls(issues))
    elif args.action == "query":
        for isu in args.issues.split(' '):
            issue = jira.get_issue(isu)
            print(str(issue.raw))
    elif args.action == "update":
        for isu in args.issues.split(' '):
            if not isu:
                continue
            issue = jira.get_issue(isu)
            if args.field == "label":
                update_label = False
                for label in args.value.split(' '):
                    if label not in issue.fields.labels:
                        issue.fields.labels.append(label)
                        update_label = True
                if update_label:
                    issue.update(fields={"labels": issue.fields.labels})
            elif args.field == "fixVersions":
                #for fvs in issue.fields.fixVersions:
                #    fixvs.append({'name':fvs.name})
                #fixvs.append({'name': args.value})
                issue.update(fields={args.field: [{'name': args.value}]})
            elif args.field == "priority":
                issue.update(fields={'priority': {'name': args.value}})
            else:
                issue.update(fields={args.field: args.value})
            print("Issue %s has been updated."%(isu))
    else:
        version = args.version
        stable_version_filepath = f'/mnt/nfs_datapool/mitLogs/config/stable_version_{version}'
        version_filepath = args.path
        job_dir = path.dirname(version_filepath)
        diff_stable_file_path = path.join(job_dir,"diff_stable")
        diff_stable = ""
        if not path.exists(stable_version_filepath):
            diff_stable = f"{stable_version_filepath} not exist, none commits diff."
        if path.exists(diff_stable_file_path):
            diff_stable = prepare_issue_content(version_filepath,diff_stable_file_path)
        else:
            diff_stable = commit_diff(version_filepath,stable_version_filepath)
        summary = args.summary
        description = args.description + "\n" + diff_stable
        exist_issues = jira.get_filter_issues(args.project, args.issuetype, labels, args.version)
        if exist_issues:
            new_comment = "Test failed again in below case: \n" + description
            comment_id = jira.create_comment(exist_issues[0],new_comment)
            print(f"Add comment with id {comment_id} to existing issue:\n {jira.get_url(exist_issues[0])}")
        else:
            labels += ["MIT_BLOCK"]
            issue = jira.create_issue(summary,description,version,args.project,args.issuetype,args.assignee,labels,args.priority)
            print("Blocking issue: \n" + jira.get_url(issue))

if __name__ == "__main__":
    main()
