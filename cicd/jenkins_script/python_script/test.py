#!/usr/bin/python
import util
import sys, os.path, json


def test_pass_notification(repo_dict, user_name, stream_name, topic_name, msg):
    for repo, num in repo_dict.iteritems():
        util.push_comment_to_pull_request(repo, num, msg, util.STATE.APPROVE, silent = True)

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
 
    if 'emoj' in config and 'pass_emoj' in config['emoj']:
        msg = config['emoj']['pass_emoj'] + '&nbsp;&nbsp;' + msg
    test_pass_notification(dict, user_name, stream_name, topic_name, msg)



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
    # main(sys.argv)
    main(['ci-test#8;', "PASS", "guande.li@tigergraph.com", "none", "none" '{"Result":"WIP Test passed!","timecost":"3 hours, 2 minutes, 38.704 seconds","Instance Type":"cluster","Tested OS":"ubuntu18,ubuntu20,centos7,centos8","url":"http://192.168.99.101:30080/job/wip_test/20676","name":"[WIP#20676](http://192.168.99.101:30080/job/wip_test/20676) on master(tools#404)"}'])