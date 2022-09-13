import yaml,os,argparse, warnings
warnings.filterwarnings("ignore")

from util import K8SAPI,print_err
from pathlib import Path

def load_yaml_template():
    jenkins_id = os.getenv("JENKINS_ID")
    path = Path(__file__).absolute().parent
    if not jenkins_id or "prod_sv4" in jenkins_id:
       pod_yaml_file = f"{path}/template/pod_template.yaml"
    else:
       pod_yaml_file = f"{path}/template/pod_template_gke.yaml"
    with open(pod_yaml_file,'r') as f:
        manifest = yaml.safe_load(f)
    return manifest

def customize_manifest(template,label,image,pod_name):
    template['metadata']['labels']['job'] = label
    template['spec']['containers'][0]["image"] = image
    template['metadata']['name'] = f"{pod_name}"
    return template

def main(num,suffix):
    pods_name,pods_ip = [],[]
    slave_nodes, edges = {}, {}
    hostname = os.getenv("HOSTNAME") if os.getenv("HOSTNAME") else os.uname().nodename
    label = os.getenv("JOB_NAME")+"_"+os.getenv("BUILD_NUMBER")
    test_job_id = os.getenv("BUILD_NUMBER") + "_" + os.getenv("JENKINS_ID")
    k8s = K8SAPI()
    image = k8s.get_pod_image(hostname)
    template = load_yaml_template()
    for i in range(num):
        pod_name = f"{hostname}-ex{i+1}" if suffix == "ex" else f"{hostname}-m{i+2}"
        manifest = customize_manifest(template,label,image,pod_name)
        created = k8s.create_pod_from_yaml(manifest)
        if not created:
            print_err(f'[ERROR]: Failed to create new pod {pod_name}')
            exit(1)
        pod_ip = k8s.get_pod_ip(pod_name)
        if pod_ip != None:
            pods_name.append(pod_name)
            pods_ip.append(pod_ip)
            slave_nodes[f"{pod_name}_{pod_ip}"] = {
                "node_name": { "value": f"{pod_name}_{pod_ip}" },
                "status": { "value": "offline" },
                "offline_message": { "value": f"{label} node-expansion" }
            }
            edges[f"{pod_name}_{pod_ip}"] = {
                "test_node_info": {
                    "test_job": {
                        test_job_id: {}
                    }
                }
            }
        else:
            print_err(f'[ERROR]: Can not retrieve the IP address of pod {pod_name}')
            exit(1)
    if suffix == "ex" and os.getenv("BUILD_NUMBER"):
        cluster_info = ",".join(pods_name) + " " + ",".join(pods_ip)
    else:
        cluster_info = ",".join([ f"{pods_name[i]}_{pods_ip[i]}" for i in range(num) ])
    return cluster_info

##############################################
# Arguments:
#   0: this script name
#   1: create pods num
#   2: create pods name suffix
##############################################
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--num",type=int,default=2,help='the number of pods to be created')
    parser.add_argument("--suffix",type=str,default='ex',help='the suffix of new pod name')
    args = parser.parse_args()
    cluster = main(args.num, args.suffix)
    print(cluster)