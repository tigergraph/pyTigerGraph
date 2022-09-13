#!/usr/bin/python
import util
from util import send_http_request
import requests, json, time, re, os, os.path, base64, pathspec

def send_github_api_request(url_tail, method='GET', data = None, silent = False,
        params = {}, retry=30, retry_interval=60, plain = False, github_token = ""):
    """
    Args:
        url_tail: a string, the part of url after http://api.github.com/repos/TigerGraph/
        method: a string, specifying the request type
        data: a dict, specify the data needed for POST
        silent: boolean value, print request result if it's set to False
        params: a dict, the data needed for GET
        retry: an int, the maximum number of retry
        retry_interval: an int, the interval between each retry
    Return:
        a dict, the request result from github
    """
    if github_token == "":
        github_token = os.getenv('MIT_GIT_TOKEN', '')
    headers = {
        'Accept' : 'application/vnd.github.black-cat-preview+json, application/vnd.github.luke-cage-preview+json',
        'Authorization': 'token ' + github_token #qa token
        }
    url = 'https://api.github.com/repos/TigerGraph/' + url_tail
    while retry >= 0:
        try:
            response = send_http_request(url, headers, method, data, params)
            break
        except requests.exceptions.ConnectionError:
            print ("Unable to connect to github, retrying in {} seconds".format(retry_interval))
            time.sleep(retry_interval)
            retry -= 1
            if retry < 0:
                print ("Failed to connect to github, request aborted.")
                raise requests.exceptions.ConnectionError
            # end if
        # end try-catch
    # end while

    # HTTP request returned an unsuccessful status code
    if response.status_code >= 400:
        if silent == False:
            print response.json()
        if response.json()['message'] == 'Not Found':
            msg = "Http returns status code " + \
                  str(response.status_code) + \
                   ". Please check repository name and pull request number."
            raise util.GithubAPIFail, msg
    if response.status_code == 204:
        return response
    return response if plain else response.json()
# end function send_github_api_request


class Enum(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError
# end class Enum
STATE = Enum(['APPROVE', 'REQUEST_CHANGES', 'COMMENT'])

########### pull reqest api ###########
def get_pull_request_info(repo, num):
    url_tail = repo + '/pulls/' + num
    return send_github_api_request(url_tail)
# end function get_pull_request_info

def get_pull_request_branch_name(repo, num):
  if num.isdigit():
    return get_pull_request_info(repo, num)['head']['ref']
  else:
    return num
# end function get_pull_request_branch_name

def get_pull_request_base_branch(repo, num):
  if num.isdigit():
    return get_pull_request_info(repo, num)['base']['ref']
  else:
    base_branch = os.getenv("BASE_BRANCH")
    if base_branch is not None:
      return base_branch
    else:
      return num
# end get_pull_request_base_branch

def get_pull_request_commits(repo, num):
    """
    get all commits of a pull request into an array
    Args:
        repo: repo name
        num: pull request number
    Returns:
        An array of all commits
    """
    if num.isdigit():
      url_tail = repo + '/pulls/' + num + '/commits'
    else:
      url_tail = repo + '/commits/' + num
    commits = send_github_api_request(url_tail)
    commit_shas = []
    for commit in commits:
        commit_shas.append(commit['sha'])
    # end for
    return commit_shas
# end function get_pull_request_commits

def get_pull_request_reviews(repo, num, page_num):
    """
    get the latest commit of base branch which is in compared branch.
    Args:
        repo: the repo name of pull request
        num: pull request number
        page_num: page number
    """
    url_tail = repo + '/pulls/' + num + '/reviews?page=' + str(page_num)
    return send_github_api_request(url_tail, plain = True)
# end function get_pull_request_reviews

def get_pull_request_owner(repo, num):
    """
    get the owner of a pull request
    Args:
        repo: repo name
        num: pull request number
    Returns:
        the owner of the pull request or empty string if pull request does not exist
    """
    if num.isdigit():
        return get_pull_request_info(repo, num)["user"]["login"]
    else:
        return ""
# end function get_pull_request_owner

def get_pull_request_files(repo, num):
    """
    get the files in a given pull request
    Args:
        repo: the repo name
        num: the pull request number
    """
    included_files = set()

    #get first page since there could be multiple pages in file list
    url_tail = repo + '/pulls/' + num + '/files'
    files_list = send_github_api_request(url_tail + '?page=1', plain = True)

    #assume one page first
    total_num = 1

    #if there are multiple pages get the page number of the last page
    if files_list.links:
        # get last page url
        last_url = files_list.links['last']['url']
        # get the last page number from last page url
        total_num = int(re.search(r"page=(\d+).*$", last_url).group(1))
    # end if

    for i in range(1, total_num + 1):
        # get files in pull request from page i
        files_list = send_github_api_request(url_tail + '?page=' + str(i), plain = True).json()

        for item in files_list:
            included_files.add(item["filename"])
        #end for
    #end for

    #print(included_files)
    print("Number of files included: " + str(len(included_files)))
    return included_files
# end function get_pull_request_files

def get_file_content(repo, branch, loc):
    url_tail = repo + '/contents/' + loc + '?ref=' + branch
    return send_github_api_request(url_tail, plain = True)

def get_codeowners_rules(repo, branch):
    """
    get the codeowners rules for a given repo on the specified branch
    Args:
        repo: the repo name
        branch: the branch name
    return:
        a list containing all rules in the current CODEOWNERS file or empty list if CODEOWNERS file not in use
        a dict containing the codeowner rule for the given repo or "none" if code owner is not in use
    """
    exists = False
    rules = {}
    rule_final = []
    print("Checking owners of " + repo + "#" + branch)
    possible_locations=['CODEOWNERS', 'docs/CODEOWNERS', '.github/CODEOWNERS']
    for loc in possible_locations:
        try:
            codeowners_response = get_file_content(repo, branch, loc)
            exists = True
            break
        except util.GithubAPIFail:
            print ("File " + str(loc) + " does not exist. Trying next location...")
        #end try-catch
    #end for

    if exists == False:
        print ("Code owners not in use for repo " + repo + " on branch " + branch + ". Skipping...")
        return "none", rule_final
    #end if

    rule_list_raw = base64.b64decode(codeowners_response.json()['content']).splitlines()

    for item in rule_list_raw:
        if item and "#" not in item:
            rule_raw = item.split()
            rule_final.append(rule_raw[0])
            rules[rule_raw[0]] = rule_raw[1:]
        #end if
    #end for

    rule_final.reverse()

    print(json.dumps(rules, indent=2, sort_keys=True))

    return rules, rule_final
# end function get_codeowners_rules

def check_rule_match(rule, files):
    """
    check if any of the given files match the given codeowner rule
    Args:
        rule: the given codeowners rule
        files: the given files to match
    return:
        True if there was a match, false otherwise
        An set of matched files or an empty set if there were no matches
    """
    to_match = pathspec.PathSpec.from_lines(pathspec.patterns.GitWildMatchPattern, rule.splitlines())

    matched_set = set(to_match.match_files(files))

    if matched_set:
        return True, matched_set
    #end if

    return False, matched_set
# end function check_rule_match

def check_pull_request_approved_by_code_owners(repo, num):
    """
    check if the given pull request is approve by the appropriate code owners
    Args:
        repo: the repo name
        num: the pull request number
    return:
        a set containing the codeowners that still need to approve the pull request,
        or an empty set if all appropriate code owners have apporve or if code owners
        are not in use.
    """
    print("Checking approval status of " + repo + "#" + num)
    base_branch = get_pull_request_base_branch(repo, num)
    pull_request_files = get_pull_request_files(repo, num)
    pr_files = pull_request_files.copy()
    codeowners_rules, rule_array = get_codeowners_rules(repo, base_branch)
    final_codeowners = set()
    if (codeowners_rules == "none" ):
        return final_codeowners, pr_files
    #end if

    needed_codeowners = set()
    approving_reviews, change_requests = get_pull_request_approvers(repo, num)
    pull_request_owner = get_pull_request_owner(repo, num)

    print("Approving reviews: " + str(approving_reviews))

    for rule in rule_array:
        match_res, match_set = check_rule_match(rule, pull_request_files)
        if match_res:
            print ("Matched rule " + str(rule))
            #print ("Matched files " + str(match_set))
            needed_codeowners.add(tuple(codeowners_rules[rule]))
            pull_request_files = pull_request_files.difference(match_set)
        #end if
        if not pull_request_files:
            break
        #end if
    #end for

    print("File codeowners: " + str(needed_codeowners))

    #copy codeowners to new so remove function in loop will work properly
    needed_codeowners_copy = set(needed_codeowners)

    for reviewer in approving_reviews:
        for owners in needed_codeowners_copy:
            print ("Checking reviewer: " + str(reviewer))
            if '@' + str(reviewer) in owners:
                needed_codeowners.discard(owners)
            #end if
        #end for
    #end for

    for owners in needed_codeowners:
        for item in owners:
            if item[1:] not in final_codeowners and item[1:] != pull_request_owner:
                final_codeowners.add(item[1:])
            #enf if
        #end for
    #end for

    print("Needed codeowners: " + str(final_codeowners))

    return final_codeowners, pr_files

# end function get_pull_request_approved_by_code_owner


def get_pull_request_approvers(repo, num):
    """
    get all reviews (except for qa_kevin) that approved the given pull request
    and all reviews (except fot qa_kevin) that requested changes on the pull
    request that didn't approve them later and that wern't dismissed.
    Args:
        repo: repo name
        num: pull request number
    Returns:
        an array of reviewers that approved the current pull request
    """
    approved = []
    changes_requested = []
    reviews_res = get_pull_request_reviews(repo, num, 1)
    total_num = 1
    qa_account = os.getenv('MIT_GIT_USER', '')
    if reviews_res.links:
        # get last page url
        last_url = reviews_res.links['last']['url']
        # get the last page number from last page url
        total_num = int(re.search(r"page=(\d+).*$", last_url).group(1))
    # end if
    last_commit = get_pull_request_info(repo, num)['head']['sha']
    for i in range(1, total_num + 1):
        # get reviews from page i
        reviews = get_pull_request_reviews(repo, num, i).json()
        for review in reviews:
            user = review['user']['login']
            commit = review['commit_id']
            state = review['state']
            #user is not qa
            if state == 'CHANGES_REQUESTED' and user != qa_account and changes_requested.count(user) == 0:
                changes_requested.append(user)
            elif state == 'APPROVED' and user != qa_account:
                approved.append(user)
                try:
                    changes_requested.remove(user)
                except ValueError:
                    pass
            elif state == 'DISMISSED' and user != qa_account:
                try:
                    approved.remove(user)
                    changes_requested.remove(user)
                except ValueError:
                    pass
            # end if
        # end for
    # end for
    return approved, changes_requested
# end function check_pull_request_approved

def get_review_id_by_user(repo, num, review_user, silent = False):
    reviews_res = get_pull_request_reviews(repo, num, 1)
    total_num = 1
    if not review_user:
        review_user = os.getenv('MIT_GIT_USER', '')
    if reviews_res.links: 
        #TODO: verify it works
        # get last page url
        last_url = reviews_res.links['last']['url']
        # get the last page number from last page url
        total_num = int(re.search(r"page=(\d+).*$", last_url).group(1))
    # end if
    for i in range(1, total_num + 1):
        # get reviews from page i
        reviews = reviews_res.json()
        for review in reviews:
            user = review['user']['login']
            #user is not qa
            if user == review_user:
                return review["id"]
        if i < total_num:
            reviews_res = get_pull_request_reviews(repo, num, i)
    return ""
# end function get_review_id_by_user

def update_comment_to_pull_request(repo, num, msg, review_id, silent = False):
    if num.isdigit():
        url_tail = repo + '/pulls/' + num + '/reviews/' + str(review_id)
        data = { "body": msg}
        response = send_github_api_request(url_tail, 'PUT', json.dumps(data), silent = silent)
        return response
# end function update_comment_to_pull_request

def push_comment_to_pull_request(repo, num, msg, state, silent = False):
    if num.isdigit():
        qa_review_id = get_review_id_by_user(repo, num, "", silent = silent)
        if qa_review_id and state == STATE.COMMENT:
            return update_comment_to_pull_request(repo, num, msg, qa_review_id, silent = silent)
        else:
            url_tail = repo + '/pulls/' + num + '/reviews'
            data = { "body": msg, "event":  state}
            return send_github_api_request(url_tail, 'POST', json.dumps(data), silent = silent)
# end function push_comment_to_pull_request

def merge_pull_request(repo, num, url, sha):
    """
    Merge pull request.
    Args:
        repo: repo name
        num: pull request number
        url: build url
        sha: The commit number that pull request head must match to allow merge.
    """
    url_tail = repo + '/pulls/' + num + '/merge'
    commit_msg = 'Merged by QE@TigerGraph: ' + url
    data = {
        'sha': sha,
        #'commit_message': commit_msg,
        'merge_method': 'squash'
        }
    response = send_github_api_request(url_tail, 'PUT', json.dumps(data))
    if response.get('merged', False) != True:
        raise util.GithubAPIFail, response.get('message')
    else:
        print response.get('message')
    # end if-else
# end function merge_pull_request


def get_pull_request_merge_base_commit(repo, num):
    '''
    get the latest commit of base branch which is in compared branch.
    Args:
        repo: the repo name of pull request
        num: pull request number
    '''
    url_tail = repo + '/compare/' + get_pull_request_base_branch(repo, num) + \
        '...' + get_pull_request_branch_name(repo, num)
    return send_github_api_request(url_tail)['merge_base_commit']['sha']
# end get_pull_request_base_commit

def check_pull_request_mergeable(repo, num):
    pr = get_pull_request_info(repo, num)
    # if mergeable is null, it means github is not ready, we retry
    if pr['mergeable'] is None:
        if pr['state'] == "open":
            time.sleep(10)
            check_pull_request_mergeable(repo, num)
        else:
            return False,pr['mergeable_state']
    else:
        return pr['mergeable'],pr['mergeable_state']

def check_feature_branch_merged_base(repo, num):
    """
    check if the latest commit of base branch which is in compared branch is
    equal to the latest commit of base branch.
    Args:
        repo: the repo name of pull request
        num: pull request number
    Returns:
        boolean value
    """
    base_lastest_commit = get_branch_lastest_commit(repo, get_pull_request_base_branch(repo, num))
    pull_request_merge_base_commit = get_pull_request_merge_base_commit(repo, num)
    return base_lastest_commit == pull_request_merge_base_commit
# end function check_feature_branch_merged_base

def get_pull_request_diff(repo, num):
    url_tail = repo + '/pulls/' + num + '/files'
    response = send_github_api_request(url_tail)
    diff = ''
    for filee in response:
        if 'patch' in filee:
            diff += filee['patch']
    # end for
    return diff
# end function get_pull_request_diff


################################# branch api ############################################

def delete_branch(repo, branch_name):
    url_tail = repo + '/git/refs/heads/' + branch_name
    response = send_github_api_request(url_tail, 'DELETE')
    if "message" in response:
        print "Failed to delete " + repo + " branch " + branch_name + ", due to " + response.get('message')
        return
    print repo + " branch " + branch_name + " was successfully deleted"


def create_branch(repo, sha, branch_name):
    """
    Create the branch. If it exists, just update its commits header
    Args:
        repo: the repo to create branch
        sha: the commit number
        branch_name: the branch name to create
    """
    url_tail = repo + '/git/refs'
    data = {
        'ref': "refs/heads/" + branch_name,
        'sha': sha,
        'force': True
    }
    response = send_github_api_request(url_tail, 'POST', json.dumps(data), True)
    if "message" in response and response.get("message") != "Reference already exists":
        print(repo + " branch " + branch_name + " already exists, skipping...")
        #raise util.GithubAPIFail, response.get('message')
    # end if

    # Patch the branch
    url_tail = repo + '/git/refs/heads/' + branch_name
    data = {
        'sha': sha,
        'force' : True
    }
    response = send_github_api_request(url_tail, 'PATCH', json.dumps(data))
    if "message" in response:
        print(repo + " branch " + branch_name + " returned message \"" + response.get('message') + "\"")
        #raise util.GithubAPIFail, response.get('message')
    # end if
    print repo + " branch " + branch_name + " was successfully created"


def enable_branch_approval(repo, branch_name):
    """
    Add restrictions for the branch.
    Args:
        repo: the repo of the branch
        branch_name: the branch name to add restrictions
    """
    url_tail = repo + '/branches/' + branch_name + '/protection'
    data = {
      "required_status_checks": None,
      "enforce_admins": False,
      "required_pull_request_reviews": {
        "dismissal_restrictions": {
        },
        "dismiss_stale_reviews": True,
        "require_code_owner_reviews": False,
        "required_approving_review_count": 1
      },
      "restrictions": {
        "users": [
          "qe-tigergraph"
        ],
        "teams": []
      }
    }
    enforce_owner = [ "gle", "blue_features", "gse", "gst", "gap", "gus", "bigtest" ]
    if repo in enforce_owner:
      data["required_pull_request_reviews"]["require_code_owner_reviews"] = True
      data["required_pull_request_reviews"]["dismissal_restrictions"] = {"users": [], "teams": []}

    fake_token = "8ZX30UY870WZU719627518363ZY249WY47W13799"
    github_token = util.decode_token(fake_token)
    response = send_github_api_request(url_tail, 'PUT', json.dumps(data), github_token = github_token)
    if "message" in response:
        print "Failed to add restrictions for " + repo + " branch " + branch_name
        raise util.GithubAPIFail, response.get('message')
    # end if
    print repo + " branch " + branch_name + " added restrictions successfully"


def add_branch_restriction(repo, branch_name):
    """
    Add restrictions for the branch.
    Args:
        repo: the repo of the branch
        branch_name: the branch name to add restrictions
    """
    url_tail = repo + '/branches/' + branch_name + '/protection'
    data = {
      "required_status_checks": None,
      "enforce_admins": False,
      "required_pull_request_reviews": None,
      "restrictions": {
        "users": [
          "qe-tigergraph"
        ],
        "teams": []
      }
    }
    fake_token = "8ZX30UY870WZU719627518363ZY249WY47W13799"
    github_token = util.decode_token(fake_token)
    response = send_github_api_request(url_tail, 'PUT', json.dumps(data), github_token = github_token)
    if "message" in response:
        print "Failed to add restrictions for " + repo + " branch " + branch_name
        raise util.GithubAPIFail, response.get('message')
    # end if
    print repo + " branch " + branch_name + " added restrictions successfully"

def get_branch_restriction(repo, branch_name):
    """
    Add restrictions for the branch.
    Args:
        repo: the repo of the branch
        branch_name: the branch name to add restrictions
    """
    url_tail = repo + '/branches/' + branch_name + '/protection'
    fake_token = "8ZX30UY870WZU719627518363ZY249WY47W13799"
    github_token = util.decode_token(fake_token)
    response = send_github_api_request(url_tail, github_token = github_token)
    if "message" in response:
        print "Failed to add restrictions for " + repo + " branch " + branch_name
        raise util.GithubAPIFail, response.get('message')
    # end if
    print "Fetching restriction for " + branch_name + " of " + repo
    print json.dumps(response)

def remove_branch_restriction(repo, branch_name):
    url_tail = repo + '/branches/' + branch_name + '/protection'
    fake_token = "8ZX30UY870WZU719627518363ZY249WY47W13799"
    github_token = util.decode_token(fake_token)
    response = send_github_api_request(url_tail, 'DELETE', github_token = github_token)
    if "message" in response:
        print "Failed to remove restrictions for " + repo + " branch " + branch_name
        return
    print repo + " branch " + branch_name + " restrictions was removed successfully"

def get_default_branch(repo):
    return send_github_api_request(repo)['default_branch']

def change_default_branch(repo, branch_name):
    fake_token = "8ZX30UY870WZU719627518363ZY249WY47W13799"
    github_token = util.decode_token(fake_token)
    data = {
      "name": repo,
      "default_branch": branch_name
    }
    response = send_github_api_request(repo, 'PATCH', json.dumps(data), github_token = github_token)
    if "message" in response:
        print "Failed to change default branch for " + repo + " to branch " + branch_name
        return
    print repo + " successfully changes default branch to " + branch_name + " successfully"


def get_branch_lastest_commit(repo, branch):
    url_tail = repo + '/branches/' + branch
    return send_github_api_request(url_tail)['commit']['sha']
# end get_branch_lastest_commit


################################# commits api #################################
def get_commit_from_sha(repo, sha):
    url_tail = repo + '/commits/' + sha
    return send_github_api_request(url_tail)
# end function get_commit_from_sha

def get_commits(repo, sha, size):
    """
    Get recent commits
    Args:
        repo: the repo name
        sha: the last commit sha
        size: how many commits to retrieve
    """

    url_tail = repo + '/commits'
    params = {
        'per_page' : size,
        'sha' : sha
    }
    response = send_github_api_request(url_tail, 'GET', params=params)
    if "message" in response:
        raise util.GithubAPIFail, response.get('message')
    # end if
    return response
# end function get_commits


################################# tag api #################################
def get_tag_sha(repo, tag_name):
    """
    Get the sha of given tag name
    Args:
        repo: the repo name
        tag_name: retrieve sha of tag name
    """

    url_tail = repo + '/git/refs/tags'

    response = send_github_api_request(url_tail, 'GET')
    if "message" in response:
        raise util.GithubAPIFail, response.get('message')
    # end if

    for tag in response:
        if tag['ref'] == 'refs/tags/' + tag_name:
            return tag['object']['sha']
        # end if
    # end for

    return None
# end function get_tag_sha

def tag_branch_as_stable(repo, sha, tag_name):
    """
    Create the tag. If it exists, ignore the response. No influence anyway.
    Args:
        repo: the repo to tag
        sha: the commit number
        tag_name: the tag name
    """
    url_tail = repo + '/git/refs'
    data = {
        'ref': "refs/tags/" + tag_name,
        'sha': sha
    }
    response = send_github_api_request(url_tail, 'POST', json.dumps(data), True)
    if "message" in response and response.get("message") != "Reference already exists":
        print "Failed to create tag " + tag_name + " for " + repo
        raise util.AdvanceTagFail, response.get('message')
    # end if

    # Patch the tag
    url_tail = repo + '/git/refs/tags/' + tag_name
    data = {
        'sha': sha,
        'force' : True
    }
    response = send_github_api_request(url_tail, 'POST', json.dumps(data))
    if "message" in response:
        print "Failed to update tag " + tag_name + " for " + repo
        raise util.AdvanceTagFail, response.get('message')
    # end if
    print repo, tag_name + " tag advanced"
# end function tag_branch_as_stable


def delete_tag(repo, tag):
    url_tail = repo + '/git/refs/tags/' + tag
    response = send_github_api_request(url_tail, 'DELETE')
    if "message" in response:
        print "Failed to delete " + repo + " tag " + tag + ", due to " + response.get('message')
        return
    print repo + " tag " + tag + " was successfully deleted"



################################# issues api #################################
def open_issue(repo, title, body, labels, **kwargs):
    """
    Open a github issue
    Args:
        repo: the repo to open an issue
        title: issue title
        body: issue content
        labels: issue labels
        **kwargs: other params
    """
    url_tail = repo + '/issues'
    data = {
        'repo': repo,
        'title': title,
        'body': body,
        'labels': labels
    }
    data.update(kwargs)

    response = send_github_api_request(url_tail, 'POST', json.dumps(data), retry=20)
    if "message" in response:
        raise util.OpenIssueFail, response.get('message')
    # end if
# end function open_issue

def get_issues(repo, labels, state):
    """
    Retrieve github issue
    Args:
        repo: the repo name from where to retrieve issues
        labels: only retrieve issues with given labels
        state: only retrieve issues of given state
    Return:
        Array of issues satisfying given conditions
    """
    url_tail = repo + '/issues'
    params = {
        'repo': repo,
        'labels': labels,
        'state': state
    }
    response = send_github_api_request(url_tail, 'GET', params=params, retry=20)
    if "message" in response:
        raise util.OpenIssueFail, response.get('message')
    # end if
    return response
# end function get_issues
