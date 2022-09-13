#This script cleans up zombie jobs (jobs marked as running on the server but are acturally not
#running) on the server. It also takes online machines on Jenkins that are taken offline my
#qa_master, but NOT marked as offline on the server.

import jenkins
import json
import math
import re
import requests
import time
import argparse

from util.util import read_total_config
from util.notify_api import notify_stream, notify_person

#This function checks to see if the given IP is a valid IP
def check_ip(ipToCheck):
  pattern = re.compile("(([0-9]+)\.){3}([0-9]+)")
  if pattern.match(ipToCheck):
    return True;
  else:
    return False;

#This function gets the status of the given job from jenkins.
#It returns a dictionary containing wheather or not the job is running,
#Its current status (success, failed, aborted), its start time in milliseconds,
#and its duration in milliseconds.
def get_jenkins_status(jenkinsIP, jenkinsPort, username, password, jobName, buildNumber):
  if not check_ip(jenkinsIP):
    raise ValueError("Error: Invalid IP ", serverIP)

  server = jenkins.Jenkins('http://' + jenkinsIP + ":" + jenkinsPort, username = username, password = password)

  try:
    buildInfo = server.get_build_info(jobName, buildNumber)
  except:
    resultDict = {
      "running": False,
    }
  else:
    resultDict = {
      "running": buildInfo['building'],
      "status": buildInfo['result'],
      "start_time": buildInfo['timestamp'],
      "duration": buildInfo['duration']
    }

  return resultDict

#This function send a given request to a given rest server
#Currently only GET, PUT, and POST requests are supported.
def sendToRESTPP(serverIP, serverPort, reqType, endPoint, data={}):
  print('Server IP: ' + serverIP)
  if not check_ip(serverIP):
    raise ValueError("Error: Invalid IP ", serverIP)
  url = 'http://' + serverIP + ':' + serverPort + endPoint
  print('URL: ' + url)

  if reqType == 'GET':
    response = requests.get(url);
    return response.json()
  elif reqType == 'POST':
    if data != {}:
      print('Data:')
      print(data)
      response = requests.post(url, data=data);
      return response
    else:
      raise ValueError("Error: Data is required for POST request")
  elif reqType == 'PUT':
    if data != {}:
      print('Data:')
      print(data)
      response = requests.put(url, data=data)
    else:
      response = requests.put(url)
    return response
  else:
    raise ValueError("Error: Invalid request type ", reqType, ". Supported types: GET, POST, PUT")

#This function gets all running jobs from the MIT server
#with the given ip
def get_running(serverIP, serverPort):
  result = sendToRESTPP(serverIP, serverPort, "GET", '/query/getRunning')['results']
  running_jobs = []
  for res in result:
    if "running_mwh" in res:
      running_jobs += res['running_mwh']
    elif "running_job" in res:
      running_jobs += res['running_job']
    else:
      continue

  return running_jobs

#This function updates the zombie jobs on the MIT server with the correct status
def update_status(serverIP, serverPort, v_type, v_id, status, endtime):
  #This dictionary keeps a mapping between the status on Jenkins to the status on the MIT server
  jenkins_server = {
    "RUNNING":"RUNNING"
  }
  end_datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(endtime))
  data = {
    "vertices": {
      v_type: {
        v_id: {
          "status": {
            "value": status
          },
          "end_t": {
            "value": end_datetime
          },
          "message": {
            "value": "clean up zombie that could have been caused by server inaccessible"
          }
        }
      }
    }
  }
  json_data = json.dumps(data)
  return sendToRESTPP(serverIP, serverPort, "POST", '/graph', json_data)

def clean_up_job(mitServerIP, mitServerPort, jenkinsIP, jenkinsPort, jenkinsUsername, jenkinsPassword, force):
  #Dictionary mapping each job type to the appropriate pipeline
  vtypes = {
    "build_job": "build_test",
    "test_job": "parallel_test",
    "hourly": "hourly_test",
    "mit": "mit_test",
    "wip": "wip_test"
  }

  job_status = ""
  asso_mit_wip_status = ""

  if force == True:
    print('Force enabled! Will clean zombie jobs that ended more than 5 minutes ago')

  #Get running jobs from MIT server
  print('Getting running jobs from MIT Server...')
  running_jobs = get_running(mitServerIP, mitServerPort)
  print('Running jobs received!\n')

  print('Processing running jobs...')
  for job in running_jobs:
    #Get job status on MIT server
    print('Processing vertex ' + job['v_id'] + '...')
    print('Vertex type: ' + job['v_type'])

    #Get job status on Jenkins
    if "job_type" in job['attributes']:
      job_name = job['attributes']['job_type']
    else:
      job_name = job['v_type']
    job_id = job['attributes']['job_id']
    job_status = get_jenkins_status(jenkinsIP, jenkinsPort, jenkinsUsername, jenkinsPassword, vtypes[job_name], job_id)
    print('Jenkins Status:')
    print(job_status)


    #Get associated MIT/WIP/HOURLY status from Jenkins if needed
    if job['v_type'] != "mwh_request":
      assoMitName = job['attributes']['log_dir'].split('/')[4].rsplit('_', 1)[0].strip()
      assoMitID = job['attributes']['log_dir'].split('/')[4].rsplit('_', 1)[1].strip()
      asso_mit_wip_status = get_jenkins_status(jenkinsIP, jenkinsPort, jenkinsUsername, jenkinsPassword, assoMitName, int(assoMitID))

    #Calculate end time based on start time and duration from Jenkins as end_t on MIT server is inaccurate
    endtime = math.floor((job_status['start_time'] + job_status['duration'])/1000)
    currTime = time.time()

    #if both jenkins test job and associated mit is not running and it have been 2 hours (or 5 minutes
    #if force is enabled) update the status on jenkins server.
    if job_status["running"] == False and (asso_mit_wip_status == "" or asso_mit_wip_status["running"] == False) \
        and ((currTime - endtime) >= 7200 or (force == True and (currTime-endtime) >= 300)):
      print('Job has status RUNNING on MIT server but is NOT running on Jenkins. Updating status...')
      notify_stream('TigerGraph Testing Status', 'zombie', 'Zombie job ' + str(job_id) + ' found.')
      result = update_status(mitServerIP, mitServerPort, job['v_type'], job['v_id'], job_status['status'], endtime)
      notify_stream('TigerGraph Testing Status', 'zombie', 'Zombie job ' + str(job_id) + ' killed.')
      print('Result: ')
      print(result.text)

    #if both jenkins test job and associated mit is not running and force is enabled but it has NOT been 5 minute
    #since the job ended print message indicating time is not up yet and skip node.
    elif job_status["running"] == False and (asso_mit_wip_status == "" or asso_mit_wip_status["running"] == False) \
        and force == True and (currTime-endtime) <= 300:
      print('Job has status RUNNING on MIT server but is NOT running on Jenkins. '
            + 'Force is in effect, but it has not been 5 minutes after the job end time. So job will NOT be cleaned.\n'
            + 'Current time: ' + str(currTime) + '\n'
            + 'Job end time: ' + str(endtime))

    #also do the print message indication time is not up and skip node if force is not enabled and it has not been 2 hours yet
    #since the job ended.
    elif job_status["running"] == False and (asso_mit_wip_status == "" or asso_mit_wip_status["running"] == False) \
        and (currTime - endtime) < 7200:
      print('Job has status RUNNING on MIT server but is NOT running on Jenkins. '
            + 'However, it has NOT been 2 hours after the job end time. So job will NOT be cleaned.\n'
            + 'Current time: ' + str(currTime) + '\n'
            + 'Job end time: ' + str(endtime))

    #print message and skip if job or associated MIT is still running on MIT server.
    else:
      print('Job has status RUNNING on MIT server and is running on Jenkins. Skipping...')
    print('\n')

#This function checks if a given node is taken offline by qa_master
def isOfflineByQa(node):
  if 'offlineCause' in node and node['offlineCause'] != None and 'description' in node['offlineCause']:
    offlineTaker = node['offlineCause']['description'].split(':')[0]
    if 'qa_master' in offlineTaker:
      return True
    else:
      return False
  else:
    return False

#This function checks if a given node is a slave node of its associated job
def isSlaveNode(jenkinsIP, jenkinsPort, jenkinsUsername, jenkinsPassword, node):
  if 'offlineCause' in node and node['offlineCause'] != None and 'description' in node['offlineCause']:
    #Dictionary for all avaliable pipelines
    pipelines = {
      "build_job": "build_test",
      "test_job": "parallel_test",
      "HOURLY": "hourly_test",
      "MIT": "mit_test",
      "WIP": "wip_test"
    }
    jobName = node['offlineCause']['description'].split(':')[1].split('#')[1].split()[1].strip()
    jobID = node['offlineCause']['description'].split(':')[1].split('#')[2].split()[0].strip()
    mitWipName = node['offlineCause']['description'].split(':')[1].split('#')[0].split()[-1].strip()
    mitWipID = node['offlineCause']['description'].split(':')[1].split('#')[1].split()[0].strip()
    print('Associated Job: ' + str(pipelines[jobName]) + str(jobID))
    print('Associated MIT/WIP/HOURLY: ' + str(pipelines[mitWipName]) + str(mitWipID))
    jobStatus = get_jenkins_status(jenkinsIP, jenkinsPort, jenkinsUsername, jenkinsPassword, pipelines[jobName], int(jobID))
    mitStatus = get_jenkins_status(jenkinsIP, jenkinsPort, jenkinsUsername, jenkinsPassword, pipelines[mitWipName], int(mitWipID))
    if jobStatus["running"] == True or mitStatus["running"] == True:
      return True
  return False

#This function returns the information on the MIT server for a given node
def getDebugNodes(serverIP, serverPort):
  debugNodes = []
  results = sendToRESTPP(serverIP, serverPort, "GET", '/query/getDebugNode')['results']
  for res in results:
    if 'debugging_nodes' in res:
      nodes = res['debugging_nodes']
      break

  for node in nodes:
    debugNodes.append(node['v_id'])
  return debugNodes


def clean_up_node(restServerIP, restServerPort, mitServerIP, mitServerPort, jenkinsIP, jenkinsPort, jenkinsUsername, jenkinsPassword, takeOnlineLogDir, forceAll):
  if not check_ip(jenkinsIP):
    raise ValueError("Error: Invalid IP ", serverIP)

  server = jenkins.Jenkins('http://' + jenkinsIP + ":" + jenkinsPort, username = jenkinsUsername, password = jenkinsPassword)
  nodes = server.get_nodes()
  node_prefixes = ('build', 'test', 'hourly')
  data = {
    'log_dir': takeOnlineLogDir
  }
  for node in nodes:
    name = node['name']
    if name == 'master':
      continue
    try:
      print('Checking node ' + str(name) + "...")
      info = server.get_node_info(name, 1)
      if info["displayName"].startswith(node_prefixes) and info["idle"] == True and info["offline"] == True and isOfflineByQa(info) == True \
        and isSlaveNode(jenkinsIP, jenkinsPort, jenkinsUsername, jenkinsPassword, info) == False:
        print('Offline info on Jenkins: ' + str(info['offlineCause']['description']))
        time.sleep(1) #sleep for 1 second to make sure server is updated if there are build/test jobs that happens to end at this time.
        debugNodes = getDebugNodes(mitServerIP, mitServerPort)
        print('Nodes currently in Debug: ' + str(debugNodes))
        if name not in debugNodes or forceAll:
          endpoint = '/api/nodes/' + str(name) + '/takeOnline'
          if name in debugNodes:
            #debug node could be forced online if forceAll is true
            #use correct API to make sure job is deregistered
            endpoint = '/api/nodes/' + str(name) + '/reclaim?force=true'
          #endif
          print('Zombie node detected or all debug nodes force online by QA. Taking online...')
          notify_stream('TigerGraph Testing Status', 'zombie', 'Zombie node ' + name + ' found.')
          sendToRESTPP(restServerIP, restServerPort, 'PUT', endpoint, data)
          notify_stream('TigerGraph Testing Status', 'zombie', 'Zombie node ' + name + ' brought back online.')
          print('Machine ' + name + ' online\n')
        else:
          print('Machine is currently in debug mode. Skipping...\n')
      else:
        print ('Node NOT MIT node, is NOT idle, NOT taken offline by qa_master, or a slave node of a running job. Skipping...\n')
    except jenkins.NotFoundException:
      print "jenkins.NotFoundException, node not found: %s" % name
    except IndexError as e:
      time.sleep(1) #sleep for 1 second to make sure vm status is updated on Jenkins
      if info["offline"] == False:
        print "Node has changed from offline to online. Skipping..."
      else:
        print "Exception: %s" % e
        raise

def main(force, takeOnlineLogDir, forceNodes):
  #Get Configs
  config = read_total_config()

  #Configs needed for clean up script
  restServerIP = config["rest_server_address"].split(":")[0]
  restServerPort = config["rest_server_address"].split(":")[1]
  mitServerIP = config["mit_server_address"].split(":")[0]
  mitServerPort = config["mit_server_address"].split(":")[1]
  jenkinsIP = config["jenkins_ip"]
  jenkinsPort = config["jekins_port"]
  jenkinsUsername = config["jenkins_account"]
  jenkinsPassword = config["jenkins_pwd"]

  #clean up zombie jobs
  print('Start zombie job cleanup...')
  clean_up_job(mitServerIP, mitServerPort, jenkinsIP, jenkinsPort, jenkinsUsername, jenkinsPassword, force)
  print('Zombie job cleaup done!\n')

  #clean up zombie nodes
  print('Start zombie node cleanup...')
  clean_up_node(restServerIP, restServerPort, mitServerIP, mitServerPort, jenkinsIP, jenkinsPort, jenkinsUsername, jenkinsPassword, takeOnlineLogDir, forceNodes)
  print('Zombie node cleanup done!')

if __name__ == "__main__":
  argParser = argparse.ArgumentParser(description='get the arguments')
  argParser.add_argument('-f','--force', action='store_true',
      help = 'Force option: Kills zombie jobs that have ended at least 5 minutes ago')
  argParser.add_argument('-l','--log_dir', default = None, type=str,
                    help = 'specify the directory for the takeOnline logs produced by cleaning up zombie nodes')
  argParser.add_argument('-a','--forceNodes', action='store_true',
          help = 'All online option: Forces all debug nodes online (CAUTION QA USE ONLY. PLEASE NOTIFY BEFORE USING).')
  args=argParser.parse_args()

  force=args.force
  allOnline=args.forceNodes
  takeOnlineLogDir=args.log_dir
  main(force, takeOnlineLogDir, allOnline)
