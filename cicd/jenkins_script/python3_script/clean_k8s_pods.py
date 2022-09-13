#!/usr/bin/python3

import sys,time,json
import requests
import util

def parse_realpodname(pods_list):
    '''
    The pod name from jenkins pipeline usually should be as PODNAME_PODIP, examples as 'k8s-ubuntu16-qs7s4_10.244.0.247'
    This function is to truncate the ip address from it
    '''
    if len(pods_list) < 1:
        util.print_err('No Pod Name can be found')
        exit(0)
    pods_name_list = []
    for pod in pods_list:
        if "_" in pod:
            pods_name_list.append(pod.split("_")[0])
    return pods_name_list

# parse the argv
if len(sys.argv) < 5:
   print("Usage: \t{} log_dir image_version pool_tag podName1 ... podName_n".format(sys.argv[0]))
   exit(0)
log_dir = sys.argv[1]
image_version = sys.argv[2]
pool_tag = sys.argv[3]
pods_list = sys.argv[4:]

pods_name = parse_realpodname(pods_list)
timestamp = time.strftime('%Y-%m-%d_%H:%M:%S', time.localtime())
log_name = 'clean_k8s_pods_{}.log'.format(timestamp)
log = util.prepare_log(log_dir,log_name)
util.run_bash("touch {0} && chmod 777 {0}".format(log))
util.run_bash("echo 'Log file create at {0}' >> {1}".format(timestamp,log))
util.run_bash("echo 'image_version recieved: {0}' >> {1}".format(image_version,log))
util.run_bash("echo 'pool_tag recieved: {0}' >> {1}".format(pool_tag,log))
util.run_bash("echo 'pods_name recieved: {0}' >> {1}".format(pods_name,log))

#call Jenkins api and run the cleanup-k8s jobs
config = util.read_total_config()
jenkins_ip = config["jenkins_ip"]
jenkins_port = config["jenkins_port"]
jenkins_user = config['jenkins_account']
jenkins_token = config['jenkins_pwd']
jenkins_job = "cleanup-k8s"
url = "http://{0}:{1}/job/{2}/buildWithParameters".format(jenkins_ip,jenkins_port,jenkins_job)
headers={'Accept': 'application/vnd.github.v3.text-match+json'}
data = {"POD_NAME": " ".join(pods_name),"POOL_TAG": f"cleanAgent-{pool_tag}"}
util.run_bash("echo 'url: {0}, data: {1}' >> {2}".format(url,data,log))
try:
   response = util.send_http_request_auth(url, headers, "POST",data,{},jenkins_user,jenkins_token)
   util.run_bash("echo 'response from server : {0}' >> {1}".format(response,log))
except requests.exceptions.ConnectionError:
   util.print_err("Unable to connect Jenkins Server") 
