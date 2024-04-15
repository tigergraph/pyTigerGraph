import json
import warnings

class AI:
    def __init__(self, conn: "TigerGraphConnection") -> None: 
        """NO DOC: Initiate an AI object. Currently in beta testing.
            Args:
                conn (TigerGraphConnection):
                    Accept a TigerGraphConnection to run queries with
                    
            Returns:
                None
        """
        self.conn = conn
        self.nlqs_host = None

    def configureInquiryAIHost(self, hostname: str):
        """ DEPRECATED: Configure the hostname of the InquiryAI service.
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
            Args:
                hostname (str):
                    The hostname (and port number) of the CoPilot serivce.
        """
        self.nlqs_host = hostname

    def registerCustomQuery(self, function_header: str, description: str, docstring: str, param_types: dict = {}):
        """ Register a custom query with the InquiryAI service.
            Args:
                function_header (str):
                    The name of the query being registered.
                description (str):
                    The high-level description of the query being registered.
                docstring (str):
                    The docstring of the query being registered. Includes information about each parameter.
                param_types (Dict[str, str]):
                    The types of the parameters. In the format {"param_name": "param_type"}
            Returns:
                Hash of query that was registered.
        """
        data = {
            "function_header": function_header,
            "description": description,
            "docstring": docstring,
            "param_types": param_types
        }
        url = self.nlqs_host+"/"+self.conn.graphname+"/register_docs"
        return self.conn._req("POST", url, authMode="pwd", data = data, jsonData=True, resKey=None)
    
    def updateCustomQuery(self, function_header: str, description: str, docstring: str, param_types: dict = {}):
        """ Update a custom query with the InquiryAI service.
            Args:
                function_header (str):
                    The name of the query being updated.
                description (str):
                    The high-level description of the query being updated.
                docstring (str):
                    The docstring of the query being updated. Includes information about each parameter.
                param_types (Dict[str, str]):
                    The types of the parameters. In the format {"param_name": "param_type"}
            Returns:
                Hash of query that was updated.
        """
        data = {
            "function_header": function_header,
            "description": description,
            "docstring": docstring,
            "param_types": param_types
        }

        json_payload = {"query_info": data}
        url = self.nlqs_host+"/"+self.conn.graphname+"/upsert_docs"
        return self.conn._req("POST", url, authMode="pwd", data = json_payload, jsonData=True, resKey=None)
    
    def deleteCustomQuery(self, function_header: str):
        """ Delete a custom query with the InquiryAI service.
            Args:
                function_header (str):
                    The name of the query being deleted.
            Returns:
                Hash of query that was deleted.
        """
        data = {"ids": [], "expr": "function_header == '"+function_header+"'"}
        url = self.nlqs_host+"/"+self.conn.graphname+"/delete_docs"
        return self.conn._req("POST", url, authMode="pwd", data = data, jsonData=True, resKey=None)

    def retrieveDocs(self, query:str, top_k:int = 3):
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

        url = self.nlqs_host+"/"+self.conn.graphname+"/retrieve_docs?top_k="+str(top_k)
        return self.conn._req("POST", url, authMode="pwd", data = data, jsonData=True, resKey=None, skipCheck=True) 

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
        return self.conn._req("POST", url, authMode="pwd", data = data, jsonData=True, resKey=None)
    
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
        url = self.nlqs_host+"/"+self.conn.graphname+"/supportai/initialize"
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

        url = self.nlqs_host+"/"+self.conn.graphname+"/supportai/create_ingest"
        return self.conn._req("POST", url, authMode="pwd", data = data, jsonData=True, resKey=None)
    
    def runDocumentIngest(self, load_job_id, data_source_id, data_path):
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
        data = {
            "load_job_id": load_job_id,
            "data_source_id": data_source_id,
            "file_path": data_path
        }
        url = self.nlqs_host+"/"+self.conn.graphname+"/supportai/ingest"
        return self.conn._req("POST", url, authMode="pwd", data = data, jsonData=True, resKey=None)
    
    def searchDocuments(self, query, method = "hnswoverlap", method_parameters: dict = {"indices": ["Document", "DocumentChunk", "Entity", "Relationship"], "top_k": 2, "num_hops": 2, "num_seen_min": 2}):
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
        return self.conn._req("POST", url, authMode="pwd", data = data, jsonData=True, resKey=None)
    
    def answerQuestion(self, query, method = "hnswoverlap", method_parameters: dict = {"indices": ["Document", "DocumentChunk", "Entity", "Relationship"], "top_k": 2, "num_hops": 2, "num_seen_min": 2}):
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
        return self.conn._req("POST", url, authMode="pwd", data = data, jsonData=True, resKey=None)
    
    def forceConsistencyUpdate(self):
        """ Force a consistency update for SupportAI embeddings.
            Returns:
                JSON response from the consistency update.
        """
        url = self.nlqs_host+"/"+self.conn.graphname+"/supportai/forceupdate"
        return self.conn._req("GET", url, authMode="pwd", resKey=None)