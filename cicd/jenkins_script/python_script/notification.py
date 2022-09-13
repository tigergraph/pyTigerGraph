#!/usr/bin/python
import util
import sys, os.path, json

def send_chat_notification(body, user_name, stream_name, topic_name):
    if user_name != None:
        for user in user_name.split(","):
            util.notify_person(user, body)
    # end if
    if stream_name != None:
        util.notify_stream(stream_name, topic_name, body)
    # end if
# end function send_chat_notification

def test_pass_notification(repo_dict, user_name, stream_name, topic_name, msg):
    send_chat_notification(msg, user_name, stream_name, topic_name)
    for repo, num in repo_dict.iteritems():
        util.push_comment_to_pull_request(repo, num, msg, util.STATE.APPROVE, silent = True)
    # end for
# end function test_pass_notification

def test_fail_notification(repo_dict, user_name, stream_name, topic_name, msg):
    send_chat_notification(msg, user_name, stream_name, topic_name)
    for repo, num in repo_dict.iteritems():
        util.push_comment_to_pull_request(repo, num, msg, util.STATE.REQUEST_CHANGES)
    # end for
# end function test_fail_notification

def test_start_notification(repo_dict, user_name, stream_name, topic_name, msg):
    send_chat_notification(msg, user_name, stream_name, topic_name)
    for repo, num in repo_dict.iteritems():
        util.push_comment_to_pull_request(repo, num, msg, util.STATE.COMMENT)
    # end for
# end function test_start_notification

def test_status_notification(repo_dict, user_name, stream_name, topic_name, msg):
    send_chat_notification(msg, user_name, stream_name, topic_name)
    for repo, num in repo_dict.iteritems():
        util.push_comment_to_pull_request(repo, num, msg, util.STATE.COMMENT)
    # end for
# end function test_status_notification

def main(parameters):
    util.check(len(parameters) >= 5, RuntimeError,
        "Invalid arguments: " + str(parameters[1:]))

    dict = util.parse_parameter(parameters, 1)

    state = parameters[2]
    user_name = parameters[3]
    if user_name.lower() == 'none':
        user_name = None
    # end if

    stream_name = parameters[4]
    if stream_name.lower() == 'none':
        stream_name = None
    # end if

    topic_name = parameters[5]
    if topic_name.lower() == 'none':
        topic_name = 'main'

    msg_json = json.loads(parameters[6])
    msg = '%s %s' %(msg_json['name'], state)
    if 'Reason' in msg_json:
        msg += '\n%s:  %s' %('Reason', msg_json['Reason'])
    msg += '\nJenkins Job:  [Check jenkins job](%s)' %(msg_json['url'])
    for title, content in msg_json.iteritems():
        if content and title not in ["name", "url", "Reason", "Comment"]:
            msg += '\n%s:  %s' %(title, content)
    if 'Comment' in msg_json:
        msg += '\n%s:  %s' %('Comment', msg_json['Comment'])

    config = util.read_total_config()
    if state == 'START':
        if 'emoj' in config and 'start_emoj' in config['emoj']:
            msg = config['emoj']['start_emoj'] + '&nbsp;&nbsp;' + msg
        test_start_notification(dict, user_name, stream_name, topic_name, msg)

    elif state == 'STATUS':
        if 'emoj' in config and 'status_emoj' in config['emoj']:
            msg = config['emoj']['status_emoj'] + '&nbsp;&nbsp;' + msg
        test_status_notification(dict, user_name, stream_name, topic_name, msg)

    elif state == 'PASS':
        if 'emoj' in config and 'pass_emoj' in config['emoj']:
            msg = config['emoj']['pass_emoj'] + '&nbsp;&nbsp;' + msg
        test_pass_notification(dict, user_name, stream_name, topic_name, msg)

    elif state == 'FAIL':
        if 'emoj' in config and 'fail_emoj' in config['emoj']:
            msg = config['emoj']['fail_emoj'] + '&nbsp;&nbsp;' + msg
        test_fail_notification(dict, user_name, stream_name, topic_name, msg)

    else:
        raise RuntimeError, "Invalid argument: " + state
    # end if
# end function main

##############################################
# Arguments:
#   0: this script name
#   1: jenkins parameters, include repo name and pull request number
#   2: notification state: START STATUS PASS FAIL
#   3: user name. If not send to a user, use 'none'
#   4: stream name. If not send to a stream, use 'none'
#   5: topic name. Must send to a topic if sending to a stream, otherwise set 'main'
#   6: addition message (optional)
##############################################
if __name__ == "__main__":
    main(sys.argv)
