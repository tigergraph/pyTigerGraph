from util import MitApi,RestApi,K8SAPI
from util.notify_api import notify_stream
from datetime import datetime
import pytz

# list all pods with k8s prefix
def list_pods_by_k8s(k8s_api):
    pod_list = k8s_api.list_pods_running()
    k8s_pods_withip = []
    for pod in pod_list:
        if pod['name'].startswith("k8s-"):
            k8s_pods_withip.append(pod['name']+"_"+pod['ip'])
    return k8s_pods_withip

# Abnormal pods which is running in k8s, but no record in database
def get_abnormal_pods(running_pods_withip,rest_api):
    abnormal_pods_withip = []
    for pod in running_pods_withip:
        path = f"api/nodes/{pod}"
        try:
            res = rest_api.get(path)
            if len(res['result']) == 0:
                abnormal_pods_withip.append(pod)
        except:
            abnormal_pods_withip.append(pod)
    return abnormal_pods_withip
# get abnormal pods age
def get_pod_age(pod_creation_timestamp):
    utc = pytz.UTC
    now = datetime.utcnow().replace(tzinfo=utc)
    diff_day = int((now - pod_creation_timestamp ).days)
    diff_hour = int((now - pod_creation_timestamp ).seconds//(60*60))
    diff_min = int((now - pod_creation_timestamp ).seconds//(60)) - diff_hour*60
    age = ""
    if diff_day !=0:
        age += f"{diff_day}D_"
    if diff_hour !=0:
        age += f"{diff_hour}H_"
    if diff_min !=0:
        age += f"{diff_min}M"
    return age


def main():
    rest_api = RestApi()
    k8s_api = K8SAPI()
    k8s_pods_withip = list_pods_by_k8s(k8s_api)
    abnormal_pods_withip = get_abnormal_pods(k8s_pods_withip,rest_api) if len(k8s_pods_withip) > 0 else []
    notify_header = (
            "Found abnormal pods running in k8s, but no record in database: \n\n"
            "pod_name   | pod_ip     | age     | status   \n"
            "-----------|------------| --------| -------  \n"
            )
    notify_message = ""
    if len(abnormal_pods_withip) > 0:
        print(f"Found abnormal pods running in k8s, but no record in database:{abnormal_pods_withip}")
        for pod in abnormal_pods_withip:
            pod_name = pod.split("_")[0]
            pod_ip = pod.split("_")[1]
            pod_creation = k8s_api.get_pod_creation_time(pod_name)
            pod_age = get_pod_age(pod_creation)
            # only notify the abnormal pods with age > 24h
            if "D" in pod_age:
                age_days = int(pod_age.split("D")[0])
                status = "Running" if age_days <=2 else "Deleted"
                notify_message += f"{pod_name}  |  {pod_ip}  | {pod_age} | {status}\n"
                if status == "Deleted":
                    k8s_api.delete_pod(pod_name)
    else:
        print("None abnormal pods")
    print(notify_header+notify_message)
    if notify_message != "" :
        notify_stream('TigerGraph Testing Status', 'zombie', notify_header+notify_message)
if __name__ == "__main__":
    main()
