import jenkins
import sys, os
import argparse
import json
import time
import datetime
import lxml
from lxml import etree

server = jenkins.Jenkins('http://192.168.55.21:8080', username='qa_master', password='11df96c4e5d4423688e94773ac1e6b58b8')
nodes = server.get_nodes()

#check if elements of the given list are all substrings of the given string
def contains_list(sub_string_list, test_string):
  for item in sub_string_list:
    if item not in test_string and item.lower() not in test_string:
      return False
    #end if
  #end for
  return True
#end contains list

#parse the node prefixes and labels from the command line
argumParser = argparse.ArgumentParser(description='get the arguments')
argumParser.add_argument('-t','--tag', default = None, nargs="+", type=str,
                    help = 'specifies the machines labels to match')
argumParser.add_argument('-p', '--prefix', default = None, nargs='+', type=str,
                    help = 'specifies the machines prefixes to match')
parameters = argumParser.parse_args()

# node_prefixs is prefix of nodename in jenkins. It can be multi prefix separated by space
node_prefixs = 'test'
node_labels  = ''
pid = os.getpid()
timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H:%M:%S')
mech_conf_filename = '/tmp/mech_conf_' + str(pid) + "_" + str(timestamp)
inif=False
if parameters.prefix:
    node_prefixs = parameters.prefix
if parameters.tag:
    node_labels = parameters.tag

number = 0
machine_list = list()
for node in nodes:
  name = node['name']
  node_lab=''
  if (name == 'master'):
    continue
  try:
    info = server.get_node_info(name)
    # check if the node name prefix in node_prefixs
    for node_pf in node_prefixs:
      if node_labels:
        config = server.get_node_config(name)
        mech_conf = open(mech_conf_filename, 'w+')
        mech_conf.write(config)
        mech_conf.seek(0)
        root = etree.parse(mech_conf_filename)
        node_lab = root.find('label').text
        mech_conf.close()
      if (info["displayName"].startswith(node_pf) and info["idle"] == True and info["offline"] == False
          and (not node_labels or contains_list(node_labels, node_lab))):
        print info
        if node_labels:
          print('labels:' + node_lab)
        print 'this node: %s, is available for new MIT/WIP' % name
        machine_list.append(name)
        number += 1
        break
  except jenkins.NotFoundException:
    print "jenkins.NotFoundException, node not found: %s" % name
  except Exception as e:
    print "Exception: %s" % e

print "MachineList: %s" % ','.join(machine_list)
print "SlaveNumber: %d" % number
# if (number >= 16):
#   print "SlaveNumber: 8"
# else:
#   print "SlaveNumber: 4"
