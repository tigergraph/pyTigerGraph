import jenkins
import argparse
import sys

#parse the ip from the command line
argumParser = argparse.ArgumentParser(description='get the arguments')
argumParser.add_argument('Pairs', metavar='name', type=str, nargs='+',
                    help='the name of the virtual machine to disable')
args = argumParser.parse_args()

#disable node on Jenkins
vmName = vars(args)['Pairs'][1::2]
message = vars(args)['Pairs'][2::2]
print vmName
print message
if (len(vmName) != len(message)):
  print "vmName length not equal message length"
  sys.exit(1)
server = jenkins.Jenkins('http://192.168.55.21:8080', username='qa_master', password='11df96c4e5d4423688e94773ac1e6b58b8')

for i in range(len(vmName)):
  try:
    server.disable_node(name=vmName[i], msg='MIT/WIP offline for debug, ' + message[i])
    print "disable node successfully, vmName: " + vmName[i]
  except jenkins.NotFoundException as e:
    print "Exception: %s" % e
  except Exception as e:
    print "Exception: %s" % e
