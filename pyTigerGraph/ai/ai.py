"""AI Submodule
The AI submodule is used to interact with the TigerGraph GraphRAG service.
It allows users to register custom queries, run natural language queries, and interact with the GraphRAG service.

To use the AI submodule, you must first create a TigerGraphConnection object, and verify that you have a TigerGraph GraphRAG service running.

For example, to use the AI submodule, you can run the following code:

[source,python]
----
from pyTigerGraph import TigerGraphConnection

conn = TigerGraphConnection(
    host="https://YOUR_DB_ADDRESS", 
    graphname="DigitalInfra", 
    username="YOUR_DB_USERNAME", 
    password="YOUR_DB_PASSWORD"
)

conn.getToken()

conn.ai.configureGraphRAGHost(hostname="http://GRAPHRAG_ADDRESS")

conn.ai.query("How many servers are there?")
----

For a more detailed and varied examples, see the demo notebooks in the (TigerGraph GraphRAG GitHub repository)[https://github.com/tigergraph/graphrag/tree/main/graphrag/docs/notebooks].
"""

import warnings

from pyTigerGraph import TigerGraphConnection


class AI:
    def __init__(self, conn: TigerGraphConnection) -> None:
        """NO DOC: Initiate an AI object. Currently in beta testing.
            Args:
                conn (TigerGraphConnection):
                    Accept a TigerGraphConnection to run queries with

            Returns:
                None
        """
        self.conn = conn
        self.nlqs_host = None
        self.server_mode = "graphrag"
        if conn.tgCloud:
            # split scheme and host
            scheme, host = conn.host.split("://")
            self.nlqs_host = scheme + "://graphrag-" + host

    def configureGraphRAGHost(self, hostname: str):
        """ Configure the hostname of the GraphRAG service.
            Not necessary if using TigerGraph GraphRAG on TigerGraph Cloud.
            Args:
                hostname (str):
                    The hostname (and port number) of the GraphRAG serivce.
        """
        self.nlqs_host = hostname
        self.server_mode = "graphrag"

    def configureServerHost(self, hostname: str, server: str):
        """ Configure the hostname of the AI service.
            Not necessary if using TigerGraph AI on TigerGraph Cloud.
            Args:
                hostname (str):
                    The hostname (and port number) of the GraphRAG serivce.
                server (str):
                    The service mode of the GraphRAG serivce.
        """
        self.nlqs_host = hostname
        self.server_mode = server

    def registerCustomQuery(self, query_name: str, description: str = None, docstring: str = None, param_types: dict = None):
        """ Register a custom query with the InquiryAI service.
            Args:
                query_name (str):
                    The name of the query being registered. Required.
                description (str):
                    The high-level description of the query being registered. Only used when using TigerGraph 3.x.
                docstring (str):
                    The docstring of the query being registered. Includes information about each parameter.
                    Only used when using TigerGraph 3.x.
                param_types (Dict[str, str]):
                    The types of the parameters. In the format {"param_name": "param_type"}
                    Only used when using TigerGraph 3.x.
            Returns:
                Hash of query that was registered.
        """
        if4 = self.conn.getVer().split(".")[0] >= "4"
        if if4:
            if description or docstring or param_types:
                warnings.warn(
                    """When using TigerGraph 4.x, query descriptions, docstrings, and parameter types are not required parameters.
                    They will be ignored, and the GSQL descriptions of the queries will be used instead.""",
                    UserWarning)
            data = {
                "queries": [query_name]
            }
            url = self.nlqs_host+"/"+self.conn.graphname+"/upsert_from_gsql"
            return self.conn._req("POST", url, authMode="pwd", data=data, jsonData=True, resKey=None)
        else:
            if description is None:
                raise ValueError(
                    "When using TigerGraph 3.x, query descriptions are required parameters.")
            if docstring is None:
                raise ValueError(
                    "When using TigerGraph 3.x, query docstrings are required parameters.")
            if param_types is None:
                raise ValueError(
                    "When using TigerGraph 3.x, query parameter types are required parameters.")
            data = {
                "function_header": query_name,
                "description": description,
                "docstring": docstring,
                "param_types": param_types,
                "graphname": self.conn.graphname
            }
            url = self.nlqs_host+"/"+self.conn.graphname+"/register_docs"
            return self.conn._req("POST", url, authMode="pwd", data=data, jsonData=True, resKey=None)

    def updateCustomQuery(self, query_name: str, description: str = None, docstring: str = None, param_types: dict = None):
        """ Update a custom query with the InquiryAI service.
            Args:
                query_name (str):
                    The name of the query being updated. Required.
                description (str):
                    The high-level description of the query being updated.
                    Only used when using TigerGraph 3.x.
                docstring (str):
                    The docstring of the query being updated. Includes information about each parameter.
                    Only used when using TigerGraph 3.x.
                param_types (Dict[str, str]):
                    The types of the parameters. In the format {"param_name": "param_type"}
                    Only used when using TigerGraph 3.x.
            Returns:
                Hash of query that was updated.
        """
        if4 = self.conn.getVer().split(".")[0] >= "4"
        if if4:
            if description or docstring or param_types:
                warnings.warn(
                    """When using TigerGraph 4.x, query descriptions, docstrings, and parameter types are not required parameters.
                    They will be ignored, and the GSQL descriptions of the queries will be used instead.""",
                    UserWarning)
            data = {
                "queries": [query_name]
            }
            url = self.nlqs_host+"/"+self.conn.graphname+"/upsert_from_gsql"
            return self.conn._req("POST", url, authMode="pwd", data=data, jsonData=True, resKey=None)
        else:
            if description is None:
                raise ValueError(
                    "When using TigerGraph 3.x, query descriptions are required parameters.")
            if docstring is None:
                raise ValueError(
                    "When using TigerGraph 3.x, query docstrings are required parameters.")
            if param_types is None:
                raise ValueError(
                    "When using TigerGraph 3.x, query parameter types are required parameters.")
            data = {
                "function_header": query_name,
                "description": description,
                "docstring": docstring,
                "param_types": param_types,
                "graphname": self.conn.graphname
            }

            json_payload = {"id": "", "query_info": data}
            url = self.nlqs_host+"/"+self.conn.graphname+"/upsert_docs"
            return self.conn._req("POST", url, authMode="pwd", data=json_payload, jsonData=True, resKey=None)

    def deleteCustomQuery(self, query_name: str):
        """ Delete a custom query with the InquiryAI service.
            Args:
                query_name (str):
                    The name of the query being deleted.
            Returns:
                Hash of query that was deleted.
        """
        data = {"ids": [], "expr": "function_header == '"+query_name+"'"}
        url = self.nlqs_host+"/"+self.conn.graphname+"/delete_docs"
        return self.conn._req("POST", url, authMode="pwd", data=data, jsonData=True, resKey=None)

    def retrieveDocs(self, query: str, top_k: int = 3):
        """ Retrieve docs from the vector store.
            Args:
                query (str):
                    The natural language query to retrieve docs with.
                top_k (int):
                    The number of docs to retrieve.
            Returns:
                List of docs retrieved.
        """
        data = {
            "query": query
        }

        url = self.nlqs_host+"/"+self.conn.graphname + \
            "/retrieve_docs?top_k="+str(top_k)
        return self.conn._req("POST", url, authMode="pwd", data=data, jsonData=True, resKey=None, skipCheck=True)

    def query(self, query):
        """ Query the database with natural language.
            Args:
                query (str):
                    Natural language query to ask about the database.
            Returns:
                JSON including the natural language response, a answered_question flag, and answer sources.
        """
        data = {
            "query": query
        }

        url = self.nlqs_host+"/"+self.conn.graphname+"/query"
        return self.conn._req("POST", url, authMode="pwd", data=data, jsonData=True, resKey=None)

    def healthCheck(self):
        """ Check the health of the GraphRAG service.
            Returns:
                JSON response from the GraphRAG service.
        """
        url = self.nlqs_host+"/health"
        return self.conn._req("GET", url, authMode="pwd", resKey=None)

    def initializeSupportAI(self):
        """ Initialize the SupportAI service.
            Returns:
                JSON response from the SupportAI service.
        """
        return self.initializeServer("supportai")

    def initializeGraphRAG(self):
        """ Initialize the GraphAI service.
            Returns:
                JSON response from the GraphAI service.
        """
        return self.initializeServer("graphrag")

    def initializeServer(self, server="graphrag"):
        """ Initialize the given service.
            Returns:
                JSON response from the given service.
        """
        self.server_mode = server
        url = f"{self.nlqs_host}/{self.conn.graphname}/{self.server_mode}/initialize"
        return self.conn._req("POST", url, authMode="pwd", resKey=None)

    def createDocumentIngest(self, data_source="", data_source_config={}, loader_config={}, file_format=""):
        """ Create a document ingest.
            Args:
                data_source (str):
                    The data source of the document ingest.
                data_source_config (dict):
                    The configuration of the data source.
                loader_config (dict):
                    The configuration of the loader.
                file_format (str):
                    The file format of the document ingest.
            Returns:
                JSON response that contains the load_job_id and data_source_id of the document ingest.
        """
        data = {
            "data_source": data_source,
            "data_source_config": data_source_config,
            "loader_config": loader_config,
            "file_format": file_format
        }

        url = f"{self.nlqs_host}/{self.conn.graphname}/{self.server_mode}/create_ingest"
        return self.conn._req("POST", url, authMode="pwd", data=data, jsonData=True, resKey=None)

    def runDocumentIngest(self, load_job_id="", data_source_id="", data_path="", data_source="", load_job_info: dict = None):
        """ Run a document ingest.
            Args:
                load_job_id (str):
                    The load job ID of the document ingest.
                data_source_id (str):
                    The data source ID of the document ingest.
                data_path (str):
                    The data path of the document ingest.
                data_source (str):
                    The data source of the document ingest.
                load_job_info (dict):
                    The information of the load job.
            Returns:
                JSON response from the document ingest.
        """
        if load_job_info:
            if not load_job_id and "load_job_id" in load_job_info:
                load_job_id = load_job_info["load_job_id"]
            if not data_source_id and "data_source_id" in load_job_info:
                data_source_id = load_job_info["data_source_id"]
            if not data_path and "data_path" in load_job_info:
                data_path = load_job_info["data_path"]
            if not data_source and "data_source" in load_job_info:
                data_source = load_job_info["data_source"]

        if not load_job_id or not data_path or not data_source_id and not load_job_info:
            raise ValueError("load_job_id and data_path are required, one of data_source_id or load_job_info must be provided.")

        if data_source.lower() == "local" and data_path.startswith(("/", ".", "~")) :
            return self.conn.runLoadingJobWithFile(data_path, data_source_id, load_job_id)
        else:
            data = {
                "load_job_id": load_job_id,
                "file_path": data_path
            }
            if load_job_info:
                data["load_job_info"] = load_job_info
            if data_source_id:
                data["data_source_id"] = data_source_id

            url = f"{self.nlqs_host}/{self.conn.graphname}/{self.server_mode}/ingest"
            return self.conn._req("POST", url, authMode="pwd", data=data, jsonData=True, resKey=None)

    def searchDocuments(self, query, method="hybrid", method_parameters: dict = {"indices": ["Document", "DocumentChunk", "Entity", "Relationship"], "top_k": 2, "num_hops": 2, "num_seen_min": 2}):
        """ Search documents.
            Args:
                query (str):
                    The query to search documents with.
                method (str):
                    The method to search documents with.
                method_parameters (dict):
                    The parameters of the method.
            Returns:
                JSON response from the document search.
        """
        data = {
            "question": query,
            "method": method,
            "method_params": method_parameters
        }
        url = self.nlqs_host+"/"+self.conn.graphname+"/"+self.server_mode+"/search"
        return self.conn._req("POST", url, authMode="pwd", data=data, jsonData=True, resKey=None)

    def answerQuestion(self, query, method="hybrid", method_parameters: dict = {"indices": ["Document", "DocumentChunk", "Entity", "Relationship"], "top_k": 2, "num_hops": 2, "num_seen_min": 2}):
        """ Answer a question.
            Args:
                query (str):
                    The query to answer the question with.
                method (str):
                    The method to answer the question with.
                method_parameters (dict):
                    The parameters of the method.
            Returns:
                JSON response from the question answer.
        """
        data = {
            "question": query,
            "method": method,
            "method_params": method_parameters
        }
        url = self.nlqs_host+"/"+self.conn.graphname+"/"+self.server_mode+"/answerquestion"
        return self.conn._req("POST", url, authMode="pwd", data=data, jsonData=True, resKey=None)

    def forceConsistencyUpdate(self, method=""):
        """ Force a consistency update for SupportAI embeddings.
            Args:
                method (str):
                    The doc initialization method to run
            Returns:
                JSON response from the consistency update.
        """
        server = method if method else self.server_mode
        url = f"{self.nlqs_host}/{self.conn.graphname}/{server}/forceupdate"
        return self.conn._req("GET", url, authMode="pwd", resKey=None)

    def checkConsistencyProgress(self, method=""):
        """ Check the progress of the consistency update.
            Args:
                method (str):
                    The doc initialization method to check or run.
            Returns:
                JSON response from the consistency update progress.
        """
        server = method if method else self.server_mode
        url = f"{self.nlqs_host}/{self.conn.graphname}/{server}/consistency_status"
        return self.conn._req("GET", url, authMode="pwd", resKey=None)
