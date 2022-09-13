import jenkins
import argparse

#parse the ip from the command line
ipParser = argparse.ArgumentParser(description='get the ip of the virtual machine to delete from Jenkins')
ipParser.add_argument('ipAddr', metavar='ip', type=str, nargs=1,
                    help='the ip of the virtual machine to delete')
args = ipParser.parse_args()

#delete node from Jenkins
vmName = vars(args)['ipAddr'][0]
print "vmName: " + vmName
server = jenkins.Jenkins('http://192.168.55.21:8080', username='qa_master', password='11df96c4e5d4423688e94773ac1e6b58b8')
try:
  server.delete_node(name=vmName)
except jenkins.NotFoundException:
  print "can NOT find slave: " + vmName
