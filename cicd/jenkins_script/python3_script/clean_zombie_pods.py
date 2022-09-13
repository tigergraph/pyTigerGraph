import sys,os,re
from datetime import datetime,timedelta
from pytz import timezone, utc
import pytz
from util import MitApi,RestApi,K8SAPI
from util.notify_api import notify_stream

"""
Function: to get debugs nodes from mit api for force clean mode
Debug nodes: debug status true
"""
def get_debug_pods(mit_api):
    path = "/query/findDebugNodes"
    params = None
    resp = mit_api.get(path, params)
    debug_pods_withip = []
    if (len(resp['results']) != 0):
        for nodes in resp['results']:
            for node in nodes['debugging_nodes']:
                debug_pods_withip.append(node['v_id'])
    return debug_pods_withip

"""
Function: to get zombie slave nodes from mit api
Zombie type-1 Define:
if the slave nodes are not with deleted status in mit api, but the job status is with FAILURE(debug expired)/SUCCESS/ABORTED status
"""
def get_zombie_type1(mit_api,start_t='',end_t=''):
    path = "/query/getK8sZombie"
    params = {'start_t': start_t, 'end_t': end_t}
    resp = mit_api.get(path, params)
    zombie_nodes_withip = set()
    if len(resp['results']) != 0:
        for slave in resp['results']:
           for node in slave['nodes']:
               zombie_nodes_withip.add(node['v_id'])
    return zombie_nodes_withip

"""
Function: to get zombie pods from k8s api and mit api
Zombie type-2 Define:
if the pods is running in k8s, but it is not in database, or the job in database is with failure status.
need to exclude the expansion pods with postfix m2 or ex1/2
"""
def get_zombie_type2(k8s_api,mit_api):
    time_delta = 2
    running_pods_withip = k8s_api.get_jnlp_running_pods(time_delta)
    path = "/query/getJobByNode"
    params = {"node_name" : running_pods_withip}
    resp = mit_api.get(path, params)
    zombie_pods_withip = set()
    node_in_mit = []
    if 'results' in resp and len(resp['results']) != 0:
        for result in resp['results']:
            for job in result['job']:
                for node in job['attributes']["@vNode"]:
                    node_in_mit.append(node)
                if job['attributes']['status'] in ["SUCCESS","ABORTED"]:
                    for node in job['attributes']["@vNode"]:
                        if node in running_pods_withip:
                            zombie_pods_withip.add(node)
                if job['attributes']['status'] == "FAILURE":
                    debug_end = datetime.strptime(job['attributes']['debug_end'], '%Y-%m-%d %H:%M:%S')
                    tz = pytz.timezone('US/Pacific')
                    if debug_end.replace(tzinfo=tz) < datetime.now(tz=tz):
                        for node in job['attributes']["@vNode"]:
                            if node in running_pods_withip:
                               zombie_pods_withip.add(node)
    for pod in running_pods_withip:
        if pod not in node_in_mit:
            zombie_pods_withip.add(pod)
    return zombie_pods_withip

"""
Function: to get zombie pods from k8s api only
Zombie type-3 Define:
if expansion pods with postfix m2 or ex1/2 are running, but the parent m1 node has already been deleted
"""
def get_zombie_type3(k8s_api):
    pod_list = k8s_api.list_namespaced_all()
    pods_name_list = [pod.metadata.name for pod in pod_list]
    pods = []
    for pod in pod_list:
        if pod.status.phase == "Running" and "k8s-" in pod.metadata.name:
            split_list = pod.metadata.name.split("-")
            pattern = re.compile("^(m|ex)[1-9]$")
            zombie_rule = [pattern.match(split_list[-1]),
                           "-".join(split_list[:-1]) not in pods_name_list]  
            if all(zombie_rule):
                    pods.append(f"{pod.metadata.name}_{pod.status.pod_ip}")
    return pods


# clean zombie pods by takeOnline of rest api
def clean_zombie_from_rest(rest_api,zombie_list):
    for pod_withip in zombie_list:
        path = f"api/nodes/{pod_withip}/takeOnline"
        data = {"log_dir": "/mnt/nfs_datapool/mitLogs/k8sZombieLogs"}
        resp = rest_api.put(path,data)
        if resp.status_code == 200:
           print(f'Successful clean zombie {pod_withip}')
        else:
           print(f'oops somthing wrong, got code {resp.status_code} from rest server')

# clean zombie pods directly with k8s api
def clean_zombie_from_k8s(k8s_api,zombie_type3_list):
    for pod_withip in zombie_type3_list:
        pod_name = pod_withip.split("_")[0]
        resp = k8s_api.delete_pod(pod_name)
        if resp:
           print(f'Successful clean type3 zombie {pod_withip}')
        else:
           print(f'oops somthing wrong, failed to delete pod {pod_withip} in k8s')

def main():
    #init mit/rest/k8s api client
    mit_api = MitApi()
    rest_api = RestApi()
    k8s_api = K8SAPI()

    #parser zombie argv
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    force = True if (len(sys.argv) > 2 and sys.argv[2] == "1") else False
    #force to clean up all debug nodes
    if force:
        debug_pods_withip = get_debug_pods(mit_api)
        pods = " ".join(debug_pods_withip)
        print(f"Forced clean all debug Pods as {pods}")
        for debug_pod_withip in debug_pods_withip:
            clean_zombie_from_rest(rest_api,debug_pod_withip)
    else:
        #define clean zombie period
        days_ago = datetime.now(tz=pytz.utc) - timedelta(days=days)
        five_mins_ago = datetime.now(tz=pytz.utc) - timedelta(minutes=5)
        jenkins_id = os.environ.get("JENKINS_ID")
        if not jenkins_id in jenkins_id:
           #debug time in old db is pacific time.So change the time to pacific time and convert to string.
           start_t = days_ago.astimezone(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S')
           end_t = five_mins_ago.astimezone(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S')
        else:
           #debug time in new gke/sv4 db is utc time.
           start_t = days_ago.strftime('%Y-%m-%d %H:%M:%S')
           end_t = five_mins_ago.strftime('%Y-%m-%d %H:%M:%S')

        #find type1 zombies from mit db side
        zombie_type1 = get_zombie_type1(mit_api,start_t,end_t)
        if len(zombie_type1) > 0:
            print(f"find type-1 zombies from mit api: {zombie_type1}")

        #find type2 zombies from both k8s/mit
        zombie_type2 = get_zombie_type2(k8s_api,mit_api)
        if len(zombie_type2) > 0:
            zombie_type2_name = [pod.split("_")[0] for pod in zombie_type2 ]
            print(f"find type-2 zombies from mit/k8s api: {zombie_type2_name}")
        #find type3 zombies from k8s side
        zombie_type3 = get_zombie_type3(k8s_api)
        if len(zombie_type3) > 0:
            zombie_type3_name = [pod.split("_")[0] for pod in zombie_type3 ]
            print(f"find type-3 zombies from mit/k8s api: {zombie_type3_name}")
        
        #combine all zombie set
        zombie_pods_set = zombie_type1 | zombie_type2
        #convert to list, send message to zulip
        all_zombie_pods = list(zombie_pods_set) + zombie_type3
        if len(all_zombie_pods) > 0:
            pods = " ".join(all_zombie_pods)
            for i in range(0,len(all_zombie_pods),10):
                # max size message 1000 bytes, 10 pods
                notify_message = (
                f"Zombie pods in K8s are found as below table-{i+1}:  \n\n"
                "pod_name   |   pod_ip     | creation_time(UTC) \n"
                "---------- | ------------ | ------------- \n"
                )
                for zombie_pod in all_zombie_pods[i:i+10]:
                    zombie_pod_name = zombie_pod.split("_")[0]
                    zombie_pod_ip = zombie_pod.split("_")[1]
                    zombie_pod_creation = k8s_api.get_pod_creation_time(zombie_pod_name)
                    notify_message += f"{zombie_pod_name}  |  {zombie_pod_ip}  | {zombie_pod_creation} \n"
                notify_stream('TigerGraph Testing Status', 'zombie', notify_message)
            print("Start cleaning zombies now...")
            #clean zombie type1 and type2 from rest api
            clean_zombie_from_rest(rest_api,zombie_pods_set)
            #clean zombie type3 from k8s api
            clean_zombie_from_k8s(k8s_api,zombie_type3)

            notify_stream('TigerGraph Testing Status', 'zombie', 'All zombie pods in K8s are cleaned.')
        else:
            print("No Zombie Pod in k8s at this moment")

if __name__ == "__main__":
    main()


