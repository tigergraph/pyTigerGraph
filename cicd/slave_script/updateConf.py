import jenkins
import argparse
import sys
import string
import subprocess

#usage: python updateConf.py -a IpToAdd -r IpToRemove [-b]
#IpToAdd: The ip to add to the config file. Can be one of the following:
#         all: adds the IPs of all machines used for MIT/WIP/Hourly from Jenkins to the config file
#         hourly: adds the IPs of all machines used for Hourly from Jenkins to the config file
#         test: adds the IPs of all testing machines used for MIT/WIP from Jenkins to the config file
#         build: adds the IPs of all machines used for cpkg during MIT/WIP/Hourly from Jenkins
#                to the config file
#         $sepecific_ip: adds the given ip to the config file
#IpToRemove: The ip to remove from the config file. Can be one of the following:
#         all: removes all IPs from the config file
#         hourly: removes the IPs of all machines used for Hourly from Jenkins from the config file
#         test: removes the IPs all testing machines used for MIT/WIP from Jenkins from the config file
#         build: removes the IPs all machines used for cpkg during MIT/WIP/Hourly from Jenkins
#                from the config file
#         $sepecific_ip: removes the given ip from the config file
#Both IpToAdd and IpToRemove can take more then one argument
#(eg. python updateConf.py -a hourly test, python updateConf.py -r build test)
#Also, both IpToAdd and IpToRemove can be used together in one call.
#(eg. python updateConfig.py -a hourly test, -r build test)
#In this case the remove will be performed first.
#If -b flag is set remove will be performed before add if both remove and add are specified

#delete a single line from a file
def deleteLine(fileName, lineToDelete):
  deleteCmd = 'sed -i \'/' + str(lineToDelete) + '/ d\' ' + str(fileName)
  subprocess.Popen(deleteCmd, shell=True, stdout=subprocess.PIPE).wait()

#get all the prefixes that machine name must start with
def allowedPrefix():
  prefixList = ['hourly', 'test', 'build', 'expansion', 'ttp']
  return prefixList

#Check to see if given machine name contains one of the required items
def contReqPrefix(name):
  prefixList = allowedPrefix()
  prefix = string.split(name, '_')[0]
  if (prefix in prefixList):
    return 1
  else:
    return -1

#returns the special list
def getSpecial():
  special = ['hourly', 'test', 'build', 'expansion', 'ttp', 'centos6', 'centos7', 'ubuntu14', 'ubuntu16', 'ubuntu18', 'centos', 'ubuntu']
  return special

#add the IPs given in ipsToAdd to the given config file configFile
def addIP(ipsToAdd, configFile):
  if(ipsToAdd):
    special = getSpecial()
    ipsToAddUniq = list(set(ipsToAdd))
    server = jenkins.Jenkins('http://192.168.55.21:8080', username='qa_build',
        password='682b2a447f8d15bdb9cf03bf5a8f7614')
    nodes = server.get_nodes()
    f = open(configFile, 'r+')
    lines = f.readlines()
    for item in ipsToAddUniq:
      if (item == 'all'):
        for node in nodes:
          name = node['name']
          if (contReqPrefix(name) == 1):
            ip = string.split(name, '_')[-1]
            if (ip + '\n' not in lines):
              f.write(ip+'\n')
            else:
              print 'IP ' + ip  + ' already exists in file ' + configFile + '. Skipping...'
        print 'Sucessfully added IPs of all slaves'
      elif (item in special):
        for node in nodes:
          name = node['name']
          if (contReqPrefix(name) == 1 and string.find(name, item) != -1):
            print name
            ip = string.split(name, '_')[-1]
            if (ip + '\n' not in lines):
              f.write(ip + '\n')
            else:
              print 'IP ' + ip  + ' already exists in file ' + configFile + '. Skipping...'
        print 'Sucessfully added IPs of all ' + item + ' machines'
      else:
        if (item + '\n' not in lines):
          f.write(item + "\n")
          print 'Sucessfully added machine ' + item + '\n'
        else:
          print 'IP ' + item + ' already exists in file ' + configFile + '. Skipping...'
    f.close()
  else:
    print 'Nothing to add. ' + configFile + ' is left unchanged'

#remove the IPs given in ipsToRemove from the given config file configFile
def remIP(ipsToRemove, configFile):
  if(ipsToRemove):
    special = getSpecial()
    server = jenkins.Jenkins('http://192.168.55.21:8080', username='qa_build',
        password='682b2a447f8d15bdb9cf03bf5a8f7614')
    nodes = server.get_nodes()
    f = open(configFile, 'r+')
    lines = f.readlines()
    for item in ipsToRemove:
      if (item == 'all'):
        for line in lines:
          deleteLine(configFile, line.strip('\n'))
        print 'Successfully removed all machines'
      elif (item in special):
        for node in nodes:
          name = node['name']
          if (string.find(name, "certificate") == -1 and string.find(name, item) != -1):
            ip = string.split(name, "_")[-1]
            deleteLine(configFile, ip)
        print 'Sucessfully removed IPs of all '+ item +' machines'
      else:
        deleteLine(configFile, item)
        print 'Sucessfully removed machine ' + item + '\n'
    f.close()
  else:
    print 'Nothing to remove. ' + configFile + ' is left unchanged'

#Main Program:

#parse the ip to add or remove from the command line
argumParser = argparse.ArgumentParser(description='get the arguments')
argumParser.add_argument('-r','--remove', default = None, nargs="+", type=str,
                    help = 'specify removing the machine with the given ip')
argumParser.add_argument('-a', '--add', default = None, nargs='+', type=str,
                    help = 'specify adding the machine with the given ip')
argumParser.add_argument('-b', '--backwards', default = False, action='store_true',
                    help = 'specify adding before removing instead of the default removing before adding')
args = argumParser.parse_args()

#Set up lists and call add and remove functions
initVmRemove = args.remove
initVmAdd = args.add
reverse = args.backwards
if( not reverse):
  remIP(initVmRemove, './IPs.conf')
  addIP(initVmAdd, './IPs.conf')
else:
  addIP(initVmAdd, './IPs.conf')
  remIP(initVmRemove, './IPs.conf')

