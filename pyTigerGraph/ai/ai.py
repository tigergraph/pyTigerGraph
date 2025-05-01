"""AI Submodule
The AI submodule is used to interact with the TigerGraph CoPilot service.
It allows users to register custom queries, run natural language queries, and interact with the CoPilot service.

To use the AI submodule, you must first create a TigerGraphConnection object, and verify that you have a TigerGraph CoPilot service running.

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

conn.ai.configureCoPilotHost(hostname="http://COPILOT_ADDRESS")

conn.ai.query("How many servers are there?")
----

For a more detailed and varied examples, see the demo notebooks in the (TigerGraph CoPilot GitHub repository)[https://github.com/tigergraph/CoPilot/tree/main/copilot/docs/notebooks].
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
        self.aiserver = "supportai"
        if conn.tgCloud:
            # split scheme and host
            scheme, host = conn.host.split("://")
            self.nlqs_host = scheme + "://copilot-" + host

    def configureInquiryAIHost(self, hostname: str):
        """ DEPRECATED: Configure the hostname of the InquiryAI service.
            Not recommended to use. Use configureCoPilotHost() instead.
            Args:
                hostname (str):
                    The hostname (and port number) of the InquiryAI serivce.
        """
        warnings.warn(
            "The `configureInquiryAIHost()` function is deprecated; use `configureCoPilotHost()` function instead.",
            DeprecationWarning)
        self.nlqs_host = hostname

    def configureCoPilotHost(self, hostname: str):
        """ Configure the hostname of the CoPilot service.
            Not necessary if using TigerGraph CoPilot on TigerGraph Cloud.
            Args:
                hostname (str):
                    The hostname (and port number) of the CoPilot serivce.
        """
        self.nlqs_host = hostname

    def configureServerHost(self, hostname: str, aiserver: str):
        """ Configure the hostname of the AI service.
            Not necessary if using TigerGraph AI on TigerGraph Cloud.
            Args:
                hostname (str):
                    The hostname (and port number) of the CoPilot serivce.
        """
        self.nlqs_host = hostname
        self.aiserver = aiserver

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

    def coPilotHealth(self):
        """ Check the health of the CoPilot service.
            Returns:
                JSON response from the CoPilot service.
        """
        url = self.nlqs_host+"/health"
        return self.conn._req("GET", url, authMode="pwd", resKey=None)

    def initializeSupportAI(self):
        """ Initialize the SupportAI service.
            Returns:
                JSON response from the SupportAI service.
        """
        return self.initializeAIServer("supportai")

    def initializeGraphAI(self):
        """ Initialize the GraphAI service.
            Returns:
                JSON response from the GraphAI service.
        """
        return self.initializeAIServer("graphai")

    def initializeAIServer(self, server="supportai"):
        """ Initialize the given service.
            Returns:
                JSON response from the given service.
        """
        self.aiserver = server
        url = f"{self.nlqs_host}/{self.conn.graphname}/{self.aiserver}/initialize"
        return self.conn._req("POST", url, authMode="pwd", resKey=None)

    def createDocumentIngest(self, data_source, data_source_config, loader_config, file_format):
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

        url = f"{self.nlqs_host}/{self.conn.graphname}/{self.aiserver}/create_ingest"
        return self.conn._req("POST", url, authMode="pwd", data=data, jsonData=True, resKey=None)

    def runDocumentIngest(self, load_job_id, data_source_id, data_path, data_source="remote"):
        """ Run a document ingest.
            Args:
                load_job_id (str):
                    The load job ID of the document ingest.
                data_source_id (str):
                    The data source ID of the document ingest.
                data_path (str):
                    The data path of the document ingest.
            Returns:
                JSON response from the document ingest.
        """
        if data_source.lower() == "local" or data_path.startswith(("/", ".", "~")) :
            return self.conn.runLoadingJobWithFile(data_path, data_source_id, load_job_id)
        else:
            data = {
                "load_job_id": load_job_id,
                "data_source_id": data_source_id,
                "file_path": data_path
            }
            url = f"{self.nlqs_host}/{self.conn.graphname}/{self.aiserver}/ingest"
            return self.conn._req("POST", url, authMode="pwd", data=data, jsonData=True, resKey=None)

    def searchDocuments(self, query, method="hnswoverlap", method_parameters: dict = {"indices": ["Document", "DocumentChunk", "Entity", "Relationship"], "top_k": 2, "num_hops": 2, "num_seen_min": 2}):
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
        url = self.nlqs_host+"/"+self.conn.graphname+"/supportai/search"
        return self.conn._req("POST", url, authMode="pwd", data=data, jsonData=True, resKey=None)

    def answerQuestion(self, query, method="hnswoverlap", method_parameters: dict = {"indices": ["Document", "DocumentChunk", "Entity", "Relationship"], "top_k": 2, "num_hops": 2, "num_seen_min": 2}):
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
        url = self.nlqs_host+"/"+self.conn.graphname+"/supportai/answerquestion"
        return self.conn._req("POST", url, authMode="pwd", data=data, jsonData=True, resKey=None)

    def forceConsistencyUpdate(self, method="supportai"):
        """ Force a consistency update for SupportAI embeddings.
            Args:
                method (str):
                    The doc initialization method to run
                    Currentlty only "supportai" is supported in CoPilot v0.9.
            Returns:
                JSON response from the consistency update.
        """
        url = f"{self.nlqs_host}/{self.conn.graphname}/{method}/forceupdate/"
        return self.conn._req("GET", url, authMode="pwd", resKey=None)

    def checkConsistencyProgress(self, method="supportai"):
        """ Check the progress of the consistency update.
            Args:
                method (str):
                    The doc initialization method to check or run.
                    Currentlty only "supportai" is supported in CoPilot v0.9.
            Returns:
                JSON response from the consistency update progress.
        """
        url = f"{self.nlqs_host}/{self.conn.graphname}/supportai/consistency_status/{method}"
        return self.conn._req("GET", url, authMode="pwd", resKey=None)
