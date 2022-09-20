#!/usr/bin/python
import requests, json, time, sys

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
        github_token = "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"
    headers = {
        'Accept' : 'application/vnd.github.black-cat-preview+json, application/vnd.github.luke-cage-preview+json',
        'Authorization': 'token ' + github_token #qa token
        }
    url = 'https://api.github.com/repos/tigergraph/' + url_tail
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
            print(response.json())
        if response.json()['message'] == 'Not Found':
            msg = "Http returns status code " + \
                  str(response.status_code) + \
                   ". Please check repository name and pull request number."
            print(msg)
    if response.status_code == 204:
        return response
    return response if plain else response.json()
# end function send_github_api_request

def push_comment_to_pull_request(repo, num, msg, state, token,silent = False):
    if num.isdigit():
            url_tail = repo + '/pulls/' + num + '/reviews'
            data = { "body": msg, "event":  state}
            return send_github_api_request(url_tail, 'POST', json.dumps(data), silent = silent, github_token=token)

def send_http_request(url, headers, method, data=None, params={}):
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
    if method == 'GET':
        response = requests.get(url, verify=False, headers=headers, data=data, params=params)
    elif method == 'POST':
        response = requests.post(url, verify=False, headers=headers, data=data, params=params)
    elif method == 'PUT':
        response = requests.put(url, verify=False, headers=headers, data=data, params=params)
    elif method == 'DELETE':
        response = requests.delete(url, verify=False, headers=headers, data=data, params=params)
    elif method == 'PATCH':
        response = requests.patch(url, verify=False, headers=headers, data=data, params=params)
    else:
        print('Unkown http method')
    # end if-else
    return response
  
if __name__ == "__main__":
    param=sys.argv
    push_comment_to_pull_request(param[1],param[2],param[3],param[4],param[5])
