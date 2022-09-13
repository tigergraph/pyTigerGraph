import time, warnings
warnings.filterwarnings("ignore")

from datetime import datetime,timedelta

import pytz
import re
from kubernetes import client, config, watch
from .log import Log

class K8SAPI(object):
    def __init__(self,namespace='default',k8s_config=None,inside_cluster=True):
        if not k8s_config:
            if inside_cluster:
                config.load_incluster_config()
            else:
                config.load_kube_config()
            self.api_client = None
        else:
            #load config from path
            kubeconfig = config.load_kube_config(config_file=k8s_config)
            self.api_client = client.api_client.ApiClient(configuration=kubeconfig)
        self.namespace = namespace
        self._k8s_api = None
        self.log = Log(log_name="k8s.log")

    @property
    def k8s_api(self):
        if not self._k8s_api:
            self._k8s_api = client.CoreV1Api(self.api_client)
        return self._k8s_api

    # list all pods in specified namespace
    def list_namespaced_all(self):
        pod_list = self.k8s_api.list_namespaced_pod(self.namespace).items
        return pod_list

    def list_pods_all(self):
        pod_list = self.k8s_api.list_namespaced_pod(self.namespace)
        pods = []
        for pod in pod_list.items:
            pods.append({"name": pod.metadata.name, "ip": pod.status.pod_ip})
        return pods

    # list all running pods in specified namespace
    def list_pods_running(self):
        pod_list = self.k8s_api.list_namespaced_pod(self.namespace)
        pods = []
        for pod in pod_list.items:
            if pod.status.phase == "Running": 
                pods.append({"name": pod.metadata.name, "ip": pod.status.pod_ip})
        return pods

    # list all running pods for k8s jnlp node, age older than 2 hours, check if it is in db
    def get_jnlp_running_pods(self,delta):
        pod_list = self.k8s_api.list_namespaced_pod(self.namespace)
        pods = []
        tz = pytz.timezone('utc')
        for pod in pod_list.items:
            if pod.status.phase == "Running" and "k8s-" in pod.metadata.name:
                two_hours_ago = datetime.now(tz=tz) - timedelta(hours=delta)
                split_list = pod.metadata.name.split("-")
                pattern = re.compile("^(m|ex)[1-9]$")
                within_db_rule = ["k8s-build" in pod.metadata.name,
                               not pattern.match(split_list[-1])]
                if pod.metadata.creation_timestamp.replace(tzinfo=pytz.timezone('utc')) < two_hours_ago:
                    if any(within_db_rule):
                        pods.append(f"{pod.metadata.name}_{pod.status.pod_ip}")
        return pods


    # list all pods with specified label
    def list_pods_by_tag(self,tag):
        tag_label = "job=%s" %tag
        pod_list = self.k8s_api.list_namespaced_pod(self.namespace,label_selector=tag_label)
        pods = []
        for pod in pod_list.items:
            pods.append({"name": pod.metadata.name, "ip": pod.status.pod_ip})
        return pods

    # get pod creation timestamp
    def get_pod_creation_time(self,name):
        selector = f"metadata.name={name}"
        pod_items = self.k8s_api.list_namespaced_pod(self.namespace,field_selector=selector).items
        if len(pod_items) == 0:
            self.log.error(f'No Pod can be found with name: {name}')
            return None
        else:
            return pod_items[0].metadata.creation_timestamp

    # get the pod image by name   
    def get_pod_image(self,name):
        self.log.info(f"Start retrieving the image of pod {name}")
        selector = f"metadata.name={name}"
        pod_items = self.k8s_api.list_namespaced_pod(self.namespace,field_selector=selector).items
        if len(pod_items) == 0:
            self.log.error(f'No Pod can be found with name: {name}')
            exit(1)
        else:
            return pod_items[0].spec.containers[0].image

    # get the pod ip by name
    def get_pod_ip(self,name):
        self.log.info(f"Start retrieving the ip of pod {name}")
        selector = f"metadata.name={name}"
        pod_items = self.k8s_api.list_namespaced_pod(self.namespace,field_selector=selector).items
        if len(pod_items) == 0:
            self.log.error(f'No Pod can be found with name: {name}')
            return None
        else:
            return pod_items[0].status.pod_ip
    
    # get the pod name by ip  
    def get_pod_name(self,ip):
        self.log.info(f"Start retrieving the name of pod by {ip}")
        selector = f"status.podIP={ip}"
        pod_items = self.k8s_api.list_namespaced_pod(self.namespace, field_selector=selector).items
        if len(pod_items) == 0:
            self.log.error(f'No Pod can be found with ip: {ip}')
            return None
        else:
            return pod_items[0].metadata.name

    def create_pod_from_yaml(self,pod):
        self.log.info(f"Start creating pod {pod.get('metadata')['name']}")
        self.k8s_api.create_namespaced_pod(body=pod, namespace=self.namespace)
        selector = f"metadata.name={pod.get('metadata')['name']}"
        create_timeout = 600
        w = watch.Watch()
        for event in w.stream(self.k8s_api.list_namespaced_pod,self.namespace,field_selector=selector,timeout_seconds=create_timeout):
                self.log.info(f"Event: {event['type']} {event['object'].metadata.name}")
                if event["object"].status.phase == "Running":
                    w.stop()
                    time.sleep(2)
                    self.log.info(f"POD {selector} created. status={event['object'].status.phase}" )
                    return True
        self.log.error(f"Failed to create pod {selector} in {create_timeout}s")
        return False
    # delete pod by name
    def delete_pod(self,name):
        self.log.info(f"Start deleting pod {name}")
        self.k8s_api.delete_namespaced_pod(
            name=name,
            namespace=self.namespace,
            body=client.V1DeleteOptions(
                propagation_policy='Foreground',
                grace_period_seconds=2)
            )
        delete_timeout = 300
        w = watch.Watch()
        selector = f"metadata.name={name}"
        for event in w.stream(self.k8s_api.list_namespaced_pod,self.namespace,field_selector=selector,timeout_seconds=delete_timeout):
                self.log.error(f"Event: {event['type']} {event['object'].metadata.name}")
                if event["type"] == "DELETED":
                    w.stop()
                    time.sleep(2)
                    self.log.error(f"POD {selector} deleted. status={event['object'].status.phase}" )
                    return True
        self.log.error(f"Failed to delete pod {selector} in {delete_timeout}s, please check the k8s log")
        return False

    def delete_pods_with_prefix(self,name_prefix):
        self.log.info(f"Start deleting all pods with same prefix {name_prefix}")
        all_jnlp_pods = self.k8s_api.get_jnlp_running_pods()
        pods_with_prefix = [ pod.split("_")[0] for pod in all_jnlp_pods if pod.startswith(name_prefix)]
        self.log.info(f"Get all pods in tg cluster {name_prefix} as: {pods_with_prefix}")
        self.log.info(f"Start deleting all pods in tg cluster {name_prefix}")
        result = []
        for pod_name in pods_with_prefix:
            res = self.k8s_api.delete_pod(pod_name)
            result.append(res)
        self.log.info(f"Finshed deleting all pods with same {name_prefix} with result: {result}")
        return True if all(res for res in result) else False


if __name__ == "__main__":
    k8s = K8SAPI()
    print(k8s.list_pods_all())
    ip = k8s.get_pod_ip("k8s-centos8-wip211-0")
    print(ip)
    k8s.delete_pod("k8s-centos8-wip211-0")