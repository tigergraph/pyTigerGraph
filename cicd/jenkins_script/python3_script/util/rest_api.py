import requests
import util

class RestApi(object):
    # rest server, default http://192.168.55.250/8888
    def __init__(self,rest_host='',rest_port=''):
        config = util.read_total_config()
        if (rest_host == '' or rest_port == ''): 
            self.base_url = "http://" + config["rest_server_address"]
        else:
            self.base_url = "http://"  + rest_host + ":" + rest_port
    
    def get(self,path,params={},attempt_json=True):
        try:
            resp = requests.get(url=f"{self.base_url}/{path}",params=params)
            if resp.status_code == 200:
                if attempt_json:
                    resp_content = resp.json()
                else:
                    resp_content = resp.text
            else:
                raise ValueError(f"Unexpected get domain error: {resp.status_code}")
        except requests.RequestException as e:
            print(e)
        return resp_content

    def put(self,path,headers=None,data=None):
        if not headers:
            headers = {'content-type': 'application/json'}
        try:
            resp = requests.put(url=f"{self.base_url}/{path}",headers=headers,data=data)
        except requests.RequestException as e:
            print(e)
        return resp
    
if __name__ == "__main__":
    rest_api = RestApi()
    path = 'api/nodes/k8s-ubuntu16-85gzb_10.244.3.7/takeOnline'
    data = {"log_dir": "/mnt/nfs_datapool/mitLogs/k8sZombieLogs"}
    resp = rest_api.put(path,data)
    print(resp.status_code)