"""Featurizer
The Featurizer class provides methods for installing and running Graph Data Science Algorithms onto a TigerGraph server.
"""

from typing import TYPE_CHECKING, Any, List

if TYPE_CHECKING:
    from ..pyTigerGraph import TigerGraphConnection

from .utilities import random_string
from .utilities import add_attribute
from os.path import join as pjoin
import requests


class Featurizer:
    def __init__(
    self, 
    conn: "TigerGraphConnection"):
    
        """NO DOC: Class for feature extraction.

        The job of a feature extracter is to install and run algorithms in the Graph Data Science (GDS) libarary.
        Currently, a set of graph algorithms are moved to the `gsql` folder, which you can find in the same directory as this file,
         and have been saved into a dictionary along with their output type.
        To add a specific algorithm, it should be added both to the `gsql` folder and class variable dictionary. 
        Args:
            conn (TigerGraphConnection): 
                Connection to the TigerGraph database.
        """

        self.conn = conn
        self.queryResult_type_dict = {"tg_pagerank":"Float","tg_article_rank":"Float","tg_betweenness_cent":"Float","tg_closeness_cent":"Float","tg_closeness_cent_approx":"Float","tg_degree_cent":"INT","tg_eigenvector_cent":"Float","tg_harmonic_cent":"INT","tg_pagerank_wt":"Float","tg_scc":"INT","tg_kcore":"INT","tg_lcc":"Float","tg_bfs":"INT","tg_shortest_ss_no_wt":"INT","tg_fastRP":"LIST<DOUBLE>","tg_label_prop":"INT","tg_louvain":"INT"}#type of features generated by graph algorithms
        self.params_dict = {} #input parameter for the desired algorithm to be run
        self.query = ""
        self.algo_dict = {
            "Centrality": {
                    "pagerank": {
                        "global": {
                            "weighted":"https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Centrality/pagerank/global/weighted/tg_pagerank_wt.gsql",
                            "unweighted":"https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Centrality/pagerank/global/unweighted/tg_pagerank.gsql"
                        }
                    },
                "article_rank": "https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Centrality/article_rank/tg_article_rank.gsql",
                "Betweenness": "https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Centrality/betweenness/tg_betweenness_cent.gsql",
                "closeness":
                    {
                        "approximate": "https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Centrality/closeness/approximate/tg_closeness_cent_approx.gsql",
                        "exact": "https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Centrality/closeness/exact/tg_closeness_cent.gsql"
                    },
                "degree": 
                    {
                        "unweighted": "https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Centrality/degree/unweighted/tg_degree_cent.gsql",
                        "weighted": "https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Centrality/degree/weighted/tg_weighted_degree_cent.gsql"
                    },
                "eigenvector": "https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Centrality/eigenvector/tg_eigenvector_cent.gsql",
                "harmonic": "https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Centrality/harmonic/tg_harmonic_cent.gsql"
                },
            "Classification": {
                "maximal_independent_set": {
                    "deterministic":"https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Classification/maximal_independent_set/deterministic/tg_maximal_indep_set.gsql"
                    }
                },
            "Community": {
                'connected_components': {
                    'strongly_connected_components': {
                        'standard': 'https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Community/connected_components/strongly_connected_components/standard/tg_scc.gsql'
                        }
                    },
                'k_core': 'https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Community/k_core/tg_kcore.gsql',
                'label_propagation': 'https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Community/label_propagation/tg_label_prop.gsql',
                'local_clustering_coefficient': 'https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Community/local_clustering_coefficient/tg_lcc.gsql',
                'louvain': 'https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Community/louvain/tg_louvain.gsql',
                'triangle_counting': {
                    'fast': 'https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Community/triangle_counting/fast/tg_tri_count_fast.gsql'
                }
            },
            "Embeddings": {
                "FastRP":"https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/GraphML/Embeddings/FastRP/tg_fastRP.gsql"
            },
            "Path":{ 
                "bfs":"https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Path/bfs/tg_bfs.gsql",
                "cycle_detection": {
                    "count":"https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Path/cycle_detection/count/tg_cycle_detection_count.gsql"
                },
                "shortest_path": {
                    "unweighted":"https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Path/shortest_path/unweighted/tg_shortest_ss_no_wt.gsql"
                    }
            },
            "Topological Link Prediction": {
                "common_neighbors":"https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Topological%20Link%20Prediction/common_neighbors/tg_common_neighbors.gsql",
                "preferential_attachment":"https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Topological%20Link%20Prediction/preferential_attachment/tg_preferential_attachment.gsql",
                "same_community":"https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Topological%20Link%20Prediction/same_community/tg_same_community.gsql",
                "total_neighbors":"https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master/algorithms/Topological%20Link%20Prediction/total_neighbors/tg_total_neighbors.gsql"
            } 
        } #List of graph algorithms 

        
        
    def _print_dict(self,d:dict, category:str=None,indent:int=0):
        '''
        Print the specified category of algorithms if category is not None, otherwise will print all list of algorithmms
        Args:
            d (dict): 
                The nested dictionary of all algorithms in GDS.
            category (str): 
                The list of specified category of algorithmms to be printed like Centrality.
            indent (int):
                indentation for printing the list of categories.
        '''
        if category!=None:
            if category in d.keys():
                d = d[category]
            else:
                raise ValueError("There is no such algorithm category.")
        for key, value in d.items():
            print(' ' * indent + str(key)+": ")
            if isinstance(value, dict):
                self._print_dict(d=value, indent=indent+1)
            else:
                if value != "":
                    value = "Algorithm Name: " + value.split("/")[-1].split(".")[0] + "\n" + ' ' * (indent+1) + "Algorithm Link: https://github.com/tigergraph/gsql-graph-algorithms/blob/master"+value.split('master')[1]
                print(' ' * (indent+1) + str(value)+". ")
    
    def _get_values(self,d):
        '''
        Check if d is a nested dictionary and return values of the most inner dictionary 
        Args:
            d (dict): 
                The nested dictionary.
        '''
        for v in d.values():
            if isinstance(v, dict):
                yield from self._get_values(v)
            else:
                yield v

    def listAlgorithms(self,category:str=None):
        '''
        Print the list of available algorithms in GDS.
        
        Args:
            category (str):
                The class of the algorithms, if it is None the entire list will be printed out.
        
        Returns:
            Prints the information for algorithms within the specified category.
        '''
        if category!=None:
            print("Available algorithms for category", category,"in the GDS (https://github.com/tigergraph/gsql-graph-algorithms):")
        else:
            print("The list of the categories for available algorithms in the GDS (https://github.com/tigergraph/gsql-graph-algorithms):")
        self._print_dict(d=self.algo_dict,category=category)

    def _is_query_installed(self, query_name: str) -> bool:
        '''
        If the query id already installed return true
        Args:
            query_name (str): 
                The name of the query
        '''
        resp = "GET /query/{}/{}".format(self.conn.graphname, query_name)
        queries = self.conn.getInstalledQueries()
        return resp in queries

    def _get_query(self,query_name:str):
        '''
        Get the query name, and download it from the github.
        Args:
            query_name (str): 
                The name of the query
        '''
        algo_list = list(self._get_values(self.algo_dict))
        query = ""
        for query_url in algo_list:
            if query_name == query_url.split('/')[-1][:-5]:
                query = requests.get(query_url).text
        if query == "":
            self.listAlgorithms()
            raise ValueError("The query name is not included in the list of queries.")   
        return query

    def _get_query_url(self,query_name:str):
        '''
        Get the query name, and return its url from GitHub.

        Args:
            query_name (str): 
                The name of the query
        '''
        algo_list = list(self._get_values(self.algo_dict))
        flag = False
        for query_url in algo_list:
            if query_name == query_url.split('/')[-1][:-5]:
                flag = True
                return "https://github.com/tigergraph/gsql-graph-algorithms/blob/master"+query_url.split('master')[1]
        if not flag: 
            self.listAlgorithms()
            raise ValueError("The query name is not included in the list of queries.")

    def _install_query_file(self, query_name: str, replace: dict = None,  query_path: str = None, global_change:bool = False):
        '''
        Reads the first line of the query file to get the query name, e.g, CREATE QUERY query_name ...

        Args:
            query_name (str): 
                The name of the query
            replace (dict): 
                If the suffix name needs to be replaced 
            global_change (bool):
                False by default. Set to true if you want to run `GLOBAL SCHEMA_CHANGE JOB`. For Algorithms that are not schema free we need to specify this argument.
                See https://docs.tigergraph.com/gsql-ref/current/ddl-and-loading/modifying-a-graph-schema#_global_vs_local_schema_changes.
        '''
        if query_path:
            with open(query_path, 'r') as f:
                query = f.read()
        # If a suffix is to be added to query name
        if replace and ("{QUERYSUFFIX}" in replace):
            query_name = query_name.replace("{QUERYSUFFIX}",replace["{QUERYSUFFIX}"])
        # If query is already installed, skip.
        if self._is_query_installed(query_name.strip()):
            return query_name
        # Otherwise, install query from its Github address
        if not(query_path):
            query = self._get_query(query_name)
        # Replace placeholders with actual content if given
        if replace:
            for placeholder in replace:
                query = query.replace(placeholder, replace[placeholder])
        self.query = query
        if query_name == "tg_fastRP":
            # Drop all jobs on the graph
            self.conn.gsql("USE GRAPH {}\n".format(self.conn.graphname) + "drop job *")
            res = add_attribute(self.conn, schema_type="VERTEX",attr_type=" LIST<DOUBLE>",attr_name="fastrp_embedding",global_change=global_change)
        # TODO: Check if Distributed query is needed.
        query = ("USE GRAPH {}\n".format(self.conn.graphname) + query + "\nINSTALL QUERY {}\n".format(query_name))
        print("Installing and optimizing the queries, it might take a minute")
        resp = self.conn.gsql(query)
        status = resp.splitlines()[-1]
        if "Failed" in status:
            raise ConnectionError(status)
        return query_name 

    def installAlgorithm(self,query_name:str, query_path: str = None, global_change:bool = False) -> str:
        '''
        Checks if the query is already installed. 
        If the query is not installed, it installs the query and changes the schema if an attribute needs to be added.
        
        Args:
            query_name (str): 
                The name of query to be installed.
            query_path (str):
                If using a custom query, the path to the `.gsql` file that contains the query.
                Note: you must have the `query_name` parameter match the name of the query in the file.
            global_change (bool):
                False by default. Set to true if you want to run `GLOBAL SCHEMA_CHANGE JOB`. For algorithms that are not schema free we need to specify this argument.
                See https://docs.tigergraph.com/gsql-ref/current/ddl-and-loading/modifying-a-graph-schema#_global_vs_local_schema_changes.
        Returns:
            String of query name installed.    
        '''
        resp = self._install_query_file(query_name=query_name, query_path=query_path,global_change=global_change)
        return resp.strip() 

    
    def _get_Params(self,query_name:str):
        '''
        Returns default query parameters by parsing the query header.
        Args:
            query_name (str):
                The name of the query to be executed.
        '''
        _dict = {}
        query = self._get_query(query_name)
        if query == "":
            self.listAlgorithms()
            raise ValueError("The query name is not included in the list of defined queries ")

        try:
            input_params = query[query.find('(')+1:query.find(')')]
            list_params =input_params.split(',')
            for i in range(len(list_params)):
                if "=" in list_params[i]:
                    params_type = list_params[i].split('=')[0].split()[0]
                    if params_type.lower() == 'float' or params_type.lower() == 'double':
                        _dict[list_params[i].split('=')[0].split()[1]] = float(list_params[i].split('=')[1])
                    if params_type.lower() == 'bool':
                        _dict[list_params[i].split('=')[0].split()[1]] = bool(list_params[i].split('=')[1])
                    if params_type.lower() == 'int':
                        _dict[list_params[i].split('=')[0].split()[1]] = int(list_params[i].split('=')[1])
                    if params_type.lower() == 'string':
                        _dict[list_params[i].split('=')[0].split()[1]] = list_params[i].split('=')[1].split()[0][1:-2]
                else:
                    _dict[list_params[i].split()[1]] =  None
        except:
            print("The algorithm does not have any input parameter.")
        self.params_dict[query_name] = _dict
        return _dict  
               
    def runAlgorithm(self, query_name:str, params:dict = None, feat_name:str = None, feat_type:str = None, custom_query:bool = False, schema_name:list = None, global_schema:bool = False, timeout:int = 2147480, sizeLimit:int = None) -> Any:
        '''
        Runs an installed query.
        The query must be already created and installed in the graph.
        If the query accepts input parameters and the parameters have not been provided, calling this function runs the query with the default values for the parameters.
        If the there isn't a default value in the query definition and no parameters are provided, the function raises a `ValueError`.

        Args:
            query_name (str):
                The name of the query to be executed.
            params (dict):
                Query parameters. A dictionary.
            feat_name (str): 
                An attribute name that needs to be added to the vertex/edge
            feat_type (str):
                Type of attribute that needs to be added to the vertex/edge. Only needed if `custom_query` is set to `True`.
            custom_query (bool):
                If the query is a custom query. Defaults to False. 
            schema_name:
                List of Vertices/Edges that the attr_name need to added to them.  
            global_schema (bool):
                False by default. Set to true if you want to run `GLOBAL SCHEMA_CHANGE JOB`.
                See https://docs.tigergraph.com/gsql-ref/current/ddl-and-loading/modifying-a-graph-schema#_global_vs_local_schema_changes.
            timeout (int):
                Maximum duration for successful query execution (in milliseconds).
            sizeLimit (int):
                Maximum size of response (in bytes).

        Returns:
            The output of the query, a list of output elements (vertex sets, edge sets, variables,
            accumulators, etc.)
        '''
        schema_type ="VERTEX"
        if params == None:
            if not(custom_query):
                params = self._get_Params(query_name)
            print("Default parameters are:",params)
            if params:
                if None in params.values():
                    query_ulr= self._get_query_url(query_name)
                    raise ValueError("Query parameters which are not initialized by default need to be initialized, visit "+query_ulr+".")
            else:
                result = self.conn.runInstalledQuery(query_name,timeout=timeout,sizeLimit = sizeLimit,usePost=True)
                if result != None:
                    return result
        else:
            if not(custom_query):
                default_params = self._get_Params(query_name)
            else:
                if feat_name == None or feat_type == None:
                    default_params = {}
                else:
                    default_params = {"result_attr": feat_name}
            if feat_name:
                if "result_attr" in default_params.keys():
                    params["result_attr"] = feat_name
                    if query_name != "tg_fastRP":
                        if not(feat_type):
                            feat_type = self.queryResult_type_dict[query_name]
                        _ = self._add_attribute(schema_type, feat_type, feat_name, schema_name, global_change=global_schema)
                else:
                    query_ulr= self._get_query_url(query_name)
                    raise ValueError("The algorithm does not provide any feature, see the algorithm details:"+query_ulr+".")
            result = self.conn.runInstalledQuery(query_name, params,timeout=timeout,sizeLimit = sizeLimit,usePost=True)
            if result != None:
                return result   
            


    