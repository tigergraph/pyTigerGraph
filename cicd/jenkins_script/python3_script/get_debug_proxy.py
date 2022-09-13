from util import K8SAPI,print_err

k8s_api = K8SAPI()
pods = k8s_api.list_namespaced_all()
proxy_ip = ""
for pod in pods:
    if pod.metadata.name.startswith("debug-server"):
        proxy_ip = f"{pod.status.host_ip} -p {pod.spec.containers[0].ports[0].host_port}"
        break
print(proxy_ip)

