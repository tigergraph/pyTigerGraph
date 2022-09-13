import jenkins
import argparse

#parse the ip from the command line
ipParser = argparse.ArgumentParser(description='get the ip of the virtual machine to add to Jenkins')
ipParser.add_argument('ipAddr', metavar='ip', type=str, nargs=1,
                    help='the ip of the virtual machine to add')
args = ipParser.parse_args()

#add virtural machine to Jenkins
vmName = vars(args)['ipAddr'][0]
print vmName
ip = vmName.split('_')[2]
print ip
server = jenkins.Jenkins('http://192.168.55.21:8080', username='qa_master', password='11df96c4e5d4423688e94773ac1e6b58b8')
params = {
  'port': '22',
  'username': 'graphsql',
  'credentialsId': 'b0b7a9a5-eb2a-4998-af59-f4926fc0aa02',
  'host': ip,
}
try:
  server.create_node(
    name=vmName,
    numExecutors=1,
    nodeDescription='This is VM for new MIT/WIP system, enjoy the fast engine!',
    remoteFS='/home/graphsql',
    labels='MIT',
    exclusive=False,
    launcher=jenkins.LAUNCHER_SSH,
    launcher_params=params)
except jenkins.JenkinsException as e:
  print ("JenkinsException: %s" % e)
  #print "node: [" + vmName + " ] already exists"
