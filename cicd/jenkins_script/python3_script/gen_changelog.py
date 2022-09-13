#!/usr/bin/python3
from util.jira_api import JiraApi
import util
import sys, os.path, json, re

def main(params):
    util.check(len(params) >= 3, RuntimeError,
        "Invalid arguments: " + str(params[1:]))
    base_branch = params[1].strip()
    repo_list = params[2].strip().replace(",", " ").split(" ")
    util.check(base_branch and repo_list, RuntimeError,
        "Invalid arguments: " + str(params[1:]))

    if len(params) >= 4:
      gen_mode = params[3].strip().lower()
    else:
      gen_mode = "changelog"

    regPtn = '\[?(?P<ticket>\w+\-\d+)\]?\s+(?P<type>\w+)\s*\((?P<scope>[\w\s]+)\)\s*(:(?P<description>(?:(?!;).)*))?;'
    mutiRegPtn = '^(\s*' + regPtn + ')+$'
    valid_types = {'fix': "Bug Fix", 'feat': "New Feature"}
    invalid_scopes = ['regression', 'test']
    result = {}

    for repo in repo_list:
        for pr in util.get_pull_request_closed(repo, base_branch):
            if pr["merged_at"] and pr["title"]:
                pr_title = pr['title'].strip().encode("ascii", "replace").decode('ascii')
                reg = re.match(mutiRegPtn, pr_title, re.M)
                if gen_mode == "all" or reg:
                    #print(pr["title"] + " Merged at " + pr["merged_at"] + " by " + pr["user"]["login"])
                    reg = re.compile(regPtn, re.M)
                    pr_infos = [m.groupdict() for m in reg.finditer(pr_title)]
                    for pr_info in pr_infos:
                        pr_type = pr_info['type'].lower()
                        pr_scope = pr_info['scope'].lower()
                        pr_jira = pr_info['ticket']
                        if gen_mode == "all" or pr_type in valid_types and pr_scope not in invalid_scopes:
                            if pr_type not in result:
                                result[pr_type] = {}
                            if repo not in result[pr_type]:
                                result[pr_type][repo] = []
                            result[pr_type][repo].append("[" + pr_jira + "]" + "(https://graphsql.atlassian.net/browse/" + pr_jira + ")" + " by " + pr["user"]["login"] + " on " + pr["merged_at"] + "\n" + "  * " + "[" + pr['title'] + "]" + "(" + pr['url'] + ")"+ "\n")

    #(https://graphsql.atlassian.net/browse/CORE-855)
    for key, value in result.items():
        if key in valid_types:
            key = valid_types[key]
        print("## " + key + "\n")
        for repo, prs in value.items():
            print("### "+ repo.upper() + "\n")
            for pr in prs:
                print(pr)



if __name__ == "__main__":
     main(sys.argv)

#MIT_GIT_USER=xxx MIT_GIT_TOKEN=yyy python3 ./gen_changelog.py tg_3.4.0_dev "$(cat ~/repo_list.3.4)" > ~/changelog_340.md
