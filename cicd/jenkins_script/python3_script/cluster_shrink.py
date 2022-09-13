from util import K8SAPI,print_err
import argparse, ipaddress

def main(pods_name, pods_ip):
    k8s = K8SAPI()
    if len(pods_ip) != 0:
       for pod_ip in pods_ip:
           pod_name = k8s.get_pod_name(pod_ip)
           if not pod_name:
               print_err(f'[ERROR]: failed to find the pod name for ip {pod_ip}')
               exit(1)
           pods_name.append(pod_name)
    for name in pods_name:
        delete = k8s.delete_pod(name)
        if not delete:
            print_err(f'[ERROR]: failed to delete the pod {name}')
            exit(1)
    print(f"The pods {pods_name} are successfully deleted...")

if __name__ == '__main__':
    # parser arguments
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--name',type=str, nargs="+",help='the name list of pods to be deleted')
    parser.add_argument('--ip',type=str, nargs="+",default="",help='the ip adress list of pods to be deleted')
    args = parser.parse_args()
    pods_ip = []
    pods_name = []
    ## parse ip address
    for item in args.ip:
        if "," in item:
            for i in item.split(","):
                try:
                    ip = ipaddress.ip_address(i)
                    pods_ip.append(ip)
                except ValueError:
                    print_err(f'[ERROR]: The input is not an valid ipv4 address {args.ip}')
                    exit(1)
        else:
            try:
                ip = ipaddress.ip_address(item)
                pods_ip.append(ip)
            except ValueError:
                print_err(f'[ERROR]: The input is not an valid ipv4 address {args.ip}')
                exit(1)
    ## parse pod name
    for name in args.name:
        if "," in name:
            for n in name.split(","):
                pods_name.append(n)
        else:
            pods_name.append(name)
    # delete pods by name or ip
    main(pods_name,pods_ip)
    