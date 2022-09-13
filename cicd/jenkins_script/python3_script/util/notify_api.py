#!/usr/bin/python
from .util import send_http_request_auth,NotifyFail,read_total_config
import requests, json, time


def send_notification_api_request(params, method='POST', retry=5, retry_interval=60):
    headers = {
        'content-type': 'application/json'
        }
    url = 'https://zulip.graphtiger.com/api/v1/messages'
    config = read_total_config()
    qebot_name = config["qebot_name"]
    qebot_token = config["qebot_token"]
    while retry >= 0:
        try:
            response = send_http_request_auth(url, headers, method, {},
                    params, qebot_name, qebot_token)
            break
        except requests.exceptions.ConnectionError:
            print("Unable to connect to zulip server, retrying in {} seconds".format(retry_interval))
            time.sleep(retry_interval)
            retry -= 1
            if retry < 0:
                print("Failed to connect to zulip server, request aborted.")
                raise requests.exceptions.ConnectionError

    if response.status_code >= 400:
        raise NotifyFail(response.json()['msg'])
    # end if
    return response
# end function send_notification_api_request

def notify_stream(stream_name, topic_name, msg):
    """
    Notify a steam with zulip message
    Args:
        stream_name: stream name to notify
        topic_name: specify the topic of the stream
        msg: message content
    Returns:
        http response result
    """
    params = { 'type': 'stream', 'to': stream_name, 'subject': topic_name, 'content': msg }
    return send_notification_api_request(params)
# end function notify_stream

def notify_person(user_email, msg):
    """
    Notify a person with zulip message
    Args:
        user_email: user email address of zulip
        msg: message content
    Returns:
        http response result
    """
    params = { 'type': 'private', 'to': user_email, 'content': msg }
    return send_notification_api_request(params)
# end function notify_person

if __name__ == "__main__":
    notify_person("wenbing.sun@tigergraph.com","hello")
