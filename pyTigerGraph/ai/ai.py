import json

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
        """ Configure the hostname of the InquiryAI service.
            Args:
                hostname (str):
                    The hostname (and port number) of the InquiryAI serivce.
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
        url = self.nlqs_host+"/"+self.conn.graphname+"/registercustomquery"
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

        url = self.nlqs_host+"/"+self.conn.graphname+"/retrievedocs?top_k="+str(top_k)
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