#!/usr/bin/python
import util
import sys, os.path, json

def send_chat_notification(body, user_name, stream_name, topic_name):
    if user_name != None:
        util.notify_person(user_name, body)
    # end if
    if stream_name != None:
        util.notify_stream(stream_name, topic_name, body)
    # end if
# end function send_chat_notification

def main(parameters):
    util.check(len(parameters) >= 4, RuntimeError,
        "Invalid arguments: " + str(parameters[1:]))

    user_name = parameters[1]
    if user_name.lower() == 'none':
        user_name = None
    # end if

    stream_name = parameters[2]
    if stream_name.lower() == 'none':
        stream_name = None
    # end if

    topic_name = parameters[3]
    if topic_name.lower() == 'none':
        topic_name = 'main'

    msg_json = json.loads(parameters[4])
    send_chat_notification(msg_json, user_name, stream_name, topic_name)
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
