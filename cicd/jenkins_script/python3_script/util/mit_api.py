import json
import requests
import util
from .log import Log

class MitApi(object):
    # mit server, default http://192.168.55.250/9000
    def __init__(self,mit_host='',mit_port='',graphname="mwh_graph"):
        config = util.read_total_config()
        if (mit_host == '' or mit_port == ''):
            self.base_url = "http://" + config["mit_server_address"]
        else:
            self.base_url = "http://"  + mit_host + ":" + mit_port
        self.graphname = graphname
        self.log = Log(log_name="mit.log")
        

    def get(self,path,params={},attempt_json=True):
        try:
            url = self.base_url + path
            resp = requests.get(url=url,params=params)
            self.log.info(f"GET {url} with params {params}, Result: {resp}")
            if resp.status_code == 200:
                if attempt_json:
                    resp_content = resp.json()
                else:
                    resp_content = resp.text
            else:
                raise ValueError(f"Unexpected get response error: {resp.status_code}")
        except requests.RequestException as e:
            print(e)
        return resp_content

    def post(self,path,data,attempt_json=True):
        try:
            url = self.base_url + path
            resp = requests.post(url=url,data=data)
            self.log.info(f"GET {url} with data {data}, Result: {resp}")
            if resp.status_code == 200:
                if attempt_json:
                    resp_content = resp.json()
                else:
                    resp_content = resp.text
            else:
                print(resp.json())
                raise ValueError(f"Unexpected get response error: {resp.status_code}")
        except requests.RequestException as e:
            print(e)
        return resp_content

    def create_vertices_edges(self, vertex_type, vertices_data, edges_data):
        """
        create multiple vertices and its edges
        Args:
            vertex_type: vertex type
            vertices_data: dict of verteices, examples below slave_nodes
            edges_data: dict of edges, examples below edges
        return:
            restpp json result 
        """
        path = f"/graph/{self.graphname}"
        params = {
            "vertices": {
                vertex_type: vertices_data
            },
            "edges": {
                vertex_type: edges_data
            }
        }
        vertices_json = json.dumps(params, indent = 4)
        res = self.post(path=path, data=vertices_json)
        return res

    def create_vertex(self, vertex_type, vertex_data):
        """
        create single vertex
        Args:
            vertex_type: vertex type
            vertex_data: vertex attribute dict
        return:
            restpp json result 
        """
        path = f"/graph/{self.graphname}"
        params = {
            "vertices": {
                vertex_type: vertex_data
            }
        }
        vertices_json = json.dumps(params, indent = 4)
        res = self.post(path=path, data=vertices_json)
        return res   

    def create_edge(self, from_vertex, from_id, edge_name, to_vertex, to_id, edge_data):
        """
        create single edge
        Args:
            from_vertex: from vertex type
            from_vertex: from vertex id
            edge_name: the edge name
            to_vertex: to vertex type
            to_id: to vertex id
            edge_data: edge attributes dict 
        return:
            restpp json result 
        """
        path = f"/graph/{self.graphname}"
        params = {
            "edges": {
                from_vertex: {
                    from_id: {
                        edge_name: {
                            to_vertex: {
                                to_id: edge_data
                            }
                        }
                    }
                }
            }
        }
        edge_json = json.dumps(params, indent = 4)
        res = self.post(path=path, data=edge_json)
        return res


if __name__ == "__main__":
    mit_client = MitApi("35.224.57.140","9000")
    from_id="k8s-build-centos7-6xnk1_10.244.5.50"
    to_id="94498_stg_gke_020213"
    slave_nodes,edges = {}, {}
    slave_nodes[from_id] = {
        "node_name": { "value": from_id },
        "status": { "value": "offline" },
        "offline_message": { "value": "node-expansion" }
    }
    edges[from_id] = {
        "test_node_info": {
            "test_job": {
                to_id: {
                }
            }
        }
    }
    res = mit_client.create_vertices_edges(vertex_type="slave_node", vertices_data=slave_nodes, edges_data=edges)
    #res = mit_client.create_edge(from_vertex="slave_node",to_vertex="test_job",edge_name="test_node_info",from_id=from_id,to_id=to_id,edge_data={})
    print(res)