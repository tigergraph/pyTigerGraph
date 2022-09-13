import jenkins
import argparse
import subprocess

#parse the ip from the command line
argumParser = argparse.ArgumentParser(description='get the arguments')
argumParser.add_argument('Name', metavar='name', type=str, nargs='+',
                    help='the name of the virtual machine to enable')
args = argumParser.parse_args()

#enable node on Jenkins
log = vars(args)['Name'][0]
image_version = vars(args)['Name'][1]
vmName = vars(args)['Name'][2:]
print vmName
server = jenkins.Jenkins('http://192.168.55.21:8080', username='qa_master', password='11df96c4e5d4423688e94773ac1e6b58b8')
for vm in vmName:
  print "vmName: " + vm
  try:
    print "before take online, do revert first"
    # subprocess.call("bash revert.sh %s %s %s" % (log, ip, image_version), shell=True, stdout=subprocess.PIPE)
    server.enable_node(name=vm)
  except jenkins.NotFoundException as e:
    print "Exception: %s" % e
  except Exception as e:
    print "Exception: %s" % e
