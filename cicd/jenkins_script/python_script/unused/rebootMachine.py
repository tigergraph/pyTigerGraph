#This script takes a vm in Jenkins offline and reboots it if there are no jobs running on it.
#Then it puts the vm back online.
#Author: Kaiyuan (Kevin) Liu
import jenkins
import xml.etree.ElementTree as ET
from subprocess import call
import time
import argparse

#parse the machine name from the command line
nameParser = argparse.ArgumentParser(description='get the name of the virtual machine to reboot')
nameParser.add_argument('vmName', metavar='name', type=str, nargs='*',
                    help='the name of the virtual machine to reboot')
args = nameParser.parse_args()
vmNames = vars(args)['vmName']

#connect to jenkins and reboot machine
server = jenkins.Jenkins('http://mit.graphsql.com:8080', username='qa_master', password='11df96c4e5d4423688e94773ac1e6b58b8')
nodes = server.get_nodes()
needToReboot = dict()

for k in nodes:
  name = k['name']
  status =  k['offline']

  if (not vmNames):
    if (name.find('slave_vm') != -1 and status == False):
      xml = str(server.get_node_config(k['name']))
      root = ET.fromstring(xml)
      ip = root.find('launcher').find('host').text
      needToReboot[name] = ip
  else:
    if (name in vmNames and status == False):
      xml = str(server.get_node_config(k['name']))
      root = ET.fromstring(xml)
      ip = root.find('launcher').find('host').text
      needToReboot[name] = ip


if(vmNames):
  for mechName in vmNames:
    if(mechName not in needToReboot):
      print "Error: Machine "+str(mechName)+" not found in Jenkins. Please check your spelling and try again."

if(not vmNames or len(vmNames) == len(needToReboot)):
  while len(needToReboot) != 0:

    for name, ip in needToReboot.items():
      jobRunning = False
      server.disable_node(name, 'Disconnected for daily reboot.')
      builds = server.get_running_builds()
      print builds

      for b in builds:
        if (b['node'] == name):
          jobRunning = True

      info = server.get_node_info(name)

      if (jobRunning == False and info["idle"] == True and info["offline"] == True):
        print 'Taking machine '+name+' offline for reboot.'
        call('ssh '+ip+' sudo reboot', shell=True)
        time.sleep(2)
        while(call('ping -c 1 '+ip, shell=True) != 0):
          print 'Waiting for machine '+name+' to reboot'
        server.enable_node(name)
        del needToReboot[name]

      else:
        print 'There is a build running on machine '+name+'. I will try to reboot it later.'

    time.sleep(1800)
