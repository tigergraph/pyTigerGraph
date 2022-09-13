import jenkins
import time
import util


class JenkinsApi(object):
    def __init__(self,jenkins_ip='',jenkins_port='',jenkins_user='',jenkins_token=''):
        config = util.read_total_config()
        self.jenkins_ip = config["jenkins_ip"] if jenkins_ip == '' else jenkins_ip
        self.jenkins_port = config["jenkins_port"] if jenkins_port == '' else jenkins_port
        self.jenkins_user = config['jenkins_account'] if jenkins_user == '' else jenkins_user
        self.jenkins_token = config['jenkins_pwd'] if jenkins_token== '' else jenkins_token
        self._jenkins_client= None

    @property
    def jenkins_client(self):
        if not self._jenkins_client:
            self._jenkins_client = jenkins.Jenkins('http://' + self.jenkins_ip + ":" + self.jenkins_port, username = self.jenkins_user, password = self.jenkins_token)
        return self._jenkins_client

    def cleanPods(self,pods_noip):
        if len(pods_noip) != 0:
            pods = " ".join(pods_noip)
            clean_job = "cleanup-k8s"
            params = {'POD_NAME': pods}
            print("Start cleaning now")
            self.jenkins_client.build_job(clean_job,params)
            next_build_number = self.jenkins_client.get_job_info(clean_job)['nextBuildNumber']
            time.sleep(60)
            res = self.jenkins_client.get_build_info(clean_job,next_build_number)['result']
            if res =='SUCCESS':
                print(f"Successfully cleaned pods {pods}")
            else:
                print(f"oops somthing wrong, please check http://mit.graphsql.com:8080/job/cleanup-k8s/{next_build_number}")
        else:
            print("Pods list is empty")


if __name__ == "__main__":
    jenkins_client = JenkinsApi().jenkins_client
    user = jenkins_client.get_whoami()
    print(user)

