"""Query Functions.

The functions on this page run installed or interpret queries in TigerGraph.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import json
import logging
from datetime import datetime

from typing import TYPE_CHECKING, Union, Optional

if TYPE_CHECKING:
    import pandas as pd

from pyTigerGraph.pyTigerGraphException import TigerGraphException
from pyTigerGraph.pyTigerGraphSchema import pyTigerGraphSchema
from pyTigerGraph.pyTigerGraphUtils import pyTigerGraphUtils
from pyTigerGraph.pyTigerGraphGSQL import pyTigerGraphGSQL
logger = logging.getLogger(__name__)


class pyTigerGraphQuery(pyTigerGraphUtils, pyTigerGraphSchema, pyTigerGraphGSQL):
    # TODO getQueries()  # List _all_ query names
    def showQuery(self, queryName: str) -> str:
        """Returns the string of the given GSQL query.
        
        Args:
            queryName (str):
                Name of the query to get metadata of.
        """
        if logger.level == logging.DEBUG:
            logger.debug("entry: showQuery")
        res = self.gsql("USE GRAPH "+self.graphname+" SHOW QUERY "+queryName)
        if logger.level == logging.DEBUG:
            logger.debug("exit: showQuery")
        return res

    def getQueryMetadata(self, queryName: str) -> dict:
        """Returns metadata details about a query. 
        Specifically, it lists the input parameters in the same order as they exist in the query
        and outputs `PRINT` statement syntax.

        Args:
            queryName (str):
                Name of the query to get metadata of.
        """
        if logger.level == logging.DEBUG:
            logger.debug("entry: getQueryMetadata")
        params = {"graph": self.graphname, "query": queryName}
        if self._versionGreaterThan4_0():
            res = self._get(self.gsUrl+"/gsqlserver/gsql/v1/queries/info", params=params, authMode="pwd", resKey="")
        else:    
            res = self._get(self.gsUrl+"/gsqlserver/gsql/queryinfo", params=params, authMode="pwd", resKey="")
        if not res["error"]: 
            if logger.level == logging.DEBUG:
                logger.debug("exit: getQueryMetadata")
            return res
        else:
            TigerGraphException(res["message"], res["code"])
    
    def getInstalledQueries(self, fmt: str = "py") -> Union[dict, str, 'pd.DataFrame']:
        """Returns a list of installed queries.

        Args:
            fmt:
                Format of the results:
                - "py":   Python objects (default)
                - "json": JSON document
                - "df":   pandas DataFrame

        Returns:
            The names of the installed queries.

        TODO This function returns all (installed and non-installed) queries
             Modify to return only installed ones
        TODO Return with query name as key rather than REST endpoint as key?
        """
        logger.info("entry: getInstalledQueries")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = self.getEndpoints(dynamic=True)
        if fmt == "json":
            ret = json.dumps(ret)
        if fmt == "df":
            try:
                import pandas as pd
            except ImportError:
                raise ImportError("Pandas is required to use this function. "
                    "Download pandas using 'pip install pandas'.")
            ret = pd.DataFrame(ret).T

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getInstalledQueries")

        return ret

    # TODO installQueries()
    #   POST /gsql/queries/install
    #   xref:tigergraph-server:API:built-in-endpoints.adoc#_install_a_query[Install a query]

    # TODO checkQueryInstallationStatus()
    #   GET /gsql/queries/install/{request_id}
    #   xref:tigergraph-server:API:built-in-endpoints.adoc#_check_query_installation_status[Check query installation status]

    def _parseQueryParameters(self, params: dict) -> str:
        """Parses a dictionary of query parameters and converts them to query strings.

        While most of the values provided for various query parameter types can be easily converted
        to query strings (key1=value1&key2=value2), `SET` and `BAG` parameter types, and especially
        `VERTEX` and `SET<VERTEX>` (i.e. vertex primary ID types without vertex type specification)
        require special handling.

        See xref:tigergraph-server:API:built-in-endpoints.adoc#_query_parameter_passing[Query parameter passing]

        TODO Accept this format for SET<VERTEX>:
            "key": [([p_id1, p_id2, ...], "vtype"), ...]
            I.e. multiple primary IDs of the same vertex type
        """
        logger.info("entry: _parseQueryParameters")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = ""
        for k, v in params.items():
            if isinstance(v, tuple):
                if len(v) == 2 and isinstance(v[1], str):
                    ret += k + "=" + str(v[0]) + "&" + k + ".type=" + self._safeChar(v[1]) + "&"
                else:
                    raise TigerGraphException(
                        "Invalid parameter value: (vertex_primary_id, vertex_type)"
                        " was expected.")
            elif isinstance(v, list):
                i = 0
                for vv in v:
                    if isinstance(vv, tuple):
                        if len(vv) == 2 and isinstance(vv[1], str):
                            ret += k + "[" + str(i) + "]=" + self._safeChar(vv[0]) + "&" + \
                                   k + "[" + str(i) + "].type=" + vv[1] + "&"
                        else:
                            raise TigerGraphException(
                                "Invalid parameter value: (vertex_primary_id , vertex_type)"
                                " was expected.")
                    else:
                        ret += k + "=" + self._safeChar(vv) + "&"
                    i += 1
            elif isinstance(v, datetime):
                ret += k + "=" + self._safeChar(v.strftime("%Y-%m-%d %H:%M:%S")) + "&"
            else:
                ret += k + "=" + self._safeChar(v) + "&"
        ret = ret[:-1]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: _parseQueryParameters")

        return ret

    def runInstalledQuery(self, queryName: str, params: Union[str, dict] = None,
            timeout: int = None, sizeLimit: int = None, usePost: bool = False, runAsync: bool = False,
            replica: int = None, threadLimit: int = None, memoryLimit: int = None) -> list:
        """Runs an installed query.

        The query must be already created and installed in the graph.
        Use `getEndpoints(dynamic=True)` or GraphStudio to find out the generated endpoint URL of
        the query. Only the query name needs to be specified here.

        Args:
            queryName:
                The name of the query to be executed.
            params:
                Query parameters. A string of param1=value1&param2=value2 format or a dictionary.
                See below for special rules for dictionaries.
            timeout:
                Maximum duration for successful query execution (in milliseconds).
                See xref:tigergraph-server:API:index.adoc#_gsql_query_timeout[GSQL query timeout]
            sizeLimit:
                Maximum size of response (in bytes).
                See xref:tigergraph-server:API:index.adoc#_response_size[Response size]
            usePost:
                Defaults to False. The RESTPP accepts a maximum URL length of 8192 characters. Use POST if additional parameters cause
                you to exceed this limit, or if you choose to pass an empty set into a query for database versions >= 3.8
            runAsync:
                Run the query in asynchronous mode. 
                See xref:gsql-ref:querying:query-operations#_detached_mode_async_option[Async operation]
            replica:
                If your TigerGraph instance is an HA cluster, specify which replica to run the query on. Must be a 
                value between [1, (cluster replication factor)].
                See xref:tigergraph-server:API:built-in-endpoints#_specify_replica[Specify replica]
            threadLimit:
                Specify a limit of the number of threads the query is allowed to use on each node of the TigerGraph cluster.
                See xref:tigergraph-server:API:built-in-endpoints#_specify_thread_limit[Thread limit]
            memoryLimit:
                Specify a limit to the amount of memory consumed by the query (in MB). If the limit is exceeded, the query will abort automatically.
                Supported in database versions >= 3.8.
                See xref:tigergraph-server:system-management:memory-management#_by_http_header[Memory limit]

        Returns:
            The output of the query, a list of output elements (vertex sets, edge sets, variables,
            accumulators, etc.

        Notes:
            When specifying parameter values in a dictionary:

            - For primitive parameter types use
                `"key": value`
            - For `SET` and `BAG` parameter types with primitive values, use
                `"key": [value1, value2, ...]`
            - For `VERTEX<type>` use
                `"key": primary_id`
            - For `VERTEX` (no vertex type specified) use
                `"key": (primary_id, "vertex_type")`
            - For `SET<VERTEX<type>>` use
                `"key": [primary_id1, primary_id2, ...]`
            - For `SET<VERTEX>` (no vertex type specified) use
                `"key": [(primary_id1, "vertex_type1"), (primary_id2, "vertex_type2"), ...]`

        Endpoints:
            - `GET /query/{graph_name}/{query_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_run_an_installed_query_get[Run an installed query (GET)]
            - `POST /query/{graph_name}/{query_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_run_an_installed_query_post[Run an installed query (POST)]
        """
        logger.info("entry: runInstalledQuery")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        headers = {}
        res_key = "results"
        if timeout and timeout > 0:
            headers["GSQL-TIMEOUT"] = str(timeout)
        if sizeLimit and sizeLimit > 0:
            headers["RESPONSE-LIMIT"] = str(sizeLimit)
        if runAsync:
            headers["GSQL-ASYNC"] = "true"
            res_key = "request_id"
        if replica:
            headers["GSQL-REPLICA"] = str(replica)
        if threadLimit:
            headers["GSQL-THREAD-LIMIT"] = str(threadLimit) 
        if memoryLimit:
            headers["GSQL-QueryLocalMemLimitMB"] = str(memoryLimit)

        if usePost:
            ret = self._post(self.restppUrl + "/query/" + self.graphname + "/" + queryName,
                data=params, headers=headers, resKey=res_key, jsonData=True)

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(ret))
            logger.info("exit: runInstalledQuery (POST)")

            return ret
        else:
            if isinstance(params, dict):
                params = self._parseQueryParameters(params)
            ret = self._get(self.restppUrl + "/query/" + self.graphname + "/" + queryName,
                params=params, headers=headers, resKey=res_key)

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(ret))
            logger.info("exit: runInstalledQuery (GET)")

            return ret

    def checkQueryStatus(self, requestId: str = ""):
        """Checks the status of the queries running on the graph specified in the connection.

        Args:
            requestId (str, optional):
                String ID of the request. If empty, returns all running requests.
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_check_query_status_detached_mode[Check query status (detached mode)]

        Endpoint:
            - `GET /query_status/{graph_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_check_query_status_detached_mode[Check query status (detached mode)]
        """
        if requestId != "":
            return self._get(self.restppUrl + "/query_status?graph_name="+self.graphname+"&requestid="+requestId)
        else:
            return self._get(self.restppUrl + "/query_status?graph_name="+self.graphname+"&requestid=all")

    def getQueryResult(self, requestId: str = ""):
        """Gets the result of a detached query.

        Args:
            requestId (str):
                String ID of the request.
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_check_query_results_detached_mode[Check query results (detached mode)]
        """
        return self._get(self.restppUrl + "/query_result?graph_name="+self.graphname+"&requestid="+requestId)

    def runInterpretedQuery(self, queryText: str, params: Union[str, dict] = None) -> list:
        """Runs an interpreted query.

        Use ``$graphname`` or ``@graphname@`` in the ``FOR GRAPH`` clause to avoid hardcoding the
        name of the graph in your app. It will be replaced by the actual graph name.

        Args:
            queryText:
                The text of the GSQL query that must be provided in this format:

                [source.wrap, gsql]
                ----
                INTERPRET QUERY (<params>) FOR GRAPH <graph_name> {
                    <statements>
                }
                ----

            params:
                A string of `param1=value1&param2=value2...` format or a dictionary.
                See below for special rules for dictionaries.

        Returns:
            The output of the query, a list of output elements such as vertex sets, edge sets, variables and
            accumulators.

        Notes:
            When specifying parameter values in a dictionary:

            - For primitive parameter types use
                `"key": value`
            - For `SET` and `BAG` parameter types with primitive values, use
                `"key": [value1, value2, ...]`
            - For `VERTEX<type>` use
                `"key": primary_id`
            - For `VERTEX` (no vertex type specified) use
                `"key": (primary_id, "vertex_type")`
            - For `SET<VERTEX<type>>` use
                `"key": [primary_id1, primary_id2, ...]`
            - For `SET<VERTEX>` (no vertex type specified) use
                `"key": [(primary_id1, "vertex_type1"), (primary_id2, "vertex_type2"), ...]`


        Endpoint:
            - `POST /gsqlserver/interpreted_query`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_run_an_interpreted_query[Run an interpreted query]

        TODO Add "GSQL-TIMEOUT: <timeout value in ms>" and "RESPONSE-LIMIT: <size limit in byte>"
            plus parameters if applicable to interpreted queries (see runInstalledQuery() above)
        """
        logger.info("entry: runInterpretedQuery")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        queryText = queryText.replace("$graphname", self.graphname)
        queryText = queryText.replace("@graphname@", self.graphname)
        if isinstance(params, dict):
            params = self._parseQueryParameters(params)

        if self._versionGreaterThan4_0():
            ret = self._post(self.gsUrl + "/gsqlserver/gsql/v1/queries/interpret", data=queryText,
                params=params, authMode="pwd")
        else:
            ret = self._post(self.gsUrl + "/gsqlserver/interpreted_query", data=queryText,
                params=params, authMode="pwd")

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: runInterpretedQuery")

        return ret

    def getRunningQueries(self) -> dict:
        """Reports the statistics of currently running queries on the graph.
        """
        if logger.level == logging.DEBUG:
            logger.debug("entry: getRunningQueries")
        res = self._get(self.restppUrl+"/showprocesslist/"+self.graphname, resKey="")
        if not res["error"]:
            if logger.level == logging.DEBUG:
                logger.debug("exit: getRunningQueries")
            return res
        else:
            raise TigerGraphException(res["message"], res["code"])

    def abortQuery(self, request_id: Union[str, list] = None, url: str = None):
        """This function safely abortsa a selected query by ID or all queries of an endpoint by endpoint URL of a graph.
        If neither `request_id` or `url` are specified, all queries currently running on the graph are aborted.
        
        Args:
            request_id (str, list, optional):
                The ID(s) of the query(s) to abort. If set to "all", it will abort all running queries.
            url
        """
        if logger.level == logging.DEBUG:
            logger.debug("entry: abortQuery")
        params = {}
        if request_id:
            params["requestid"] = request_id
        if url:
            params["url"] = url
        res = self._get(self.restppUrl+"/abortquery/"+self.graphname, params=params, resKey="")
        if not res["error"]:
            if logger.level == logging.DEBUG: 
                logger.debug("exit: abortQuery")
            return res
        else:
            raise TigerGraphException(res["message"], res["code"])

    def parseQueryOutput(self, output: list, graphOnly: bool = True) -> dict:
        """Parses query output and separates vertex and edge data (and optionally other output) for
            easier use.

        Args:
            output:
                The data structure returned by `runInstalledQuery()` or `runInterpretedQuery()`.
            graphOnly:
                If `True` (the default setting), restricts captured output to vertices and edges.
                If `False`, captures values of variables and accumulators and any other plain text printed.

        Returns:
            A dictionary with two (or three) keys: `"vertices"`, `"edges"` and optionally `"output"`.
            The first two refer to another dictionary containing keys for each vertex and edge types
            found and the instances of those vertex and edge types. `"output"` is a list of
            dictionaries containing the key/value pairs of any other output.

        The JSON output from a query can contain a mixture of results: vertex sets (the output of a
            SELECT statement), edge sets (e.g. collected in a global accumulator), printout of
            global and local variables and accumulators, including complex types (LIST, MAP, etc.).
            The type of the various output entries is not explicit and requires manual inspection to determine the type.

        This function "cleans" this output, separating and collecting vertices and edges in an easy
            to access way. It can also collect other output or ignore it. /
        The output of this function can be used e.g. with the `vertexSetToDataFrame()` and
            `edgeSetToDataFrame()` functions or (after some transformation) to pass a subgraph to a
            visualization component.
        """

        def attCopy(src: dict, trg: dict):
            """Copies the attributes of a vertex or edge into another vertex or edge, respectively.

            args:
                src:
                    Source vertex or edge instance.
                trg:
                    Target vertex or edge instance.
            """
            srca = src["attributes"]
            trga = trg["attributes"]
            for att in srca:
                trga[att] = srca[att]

        def addOccurrences(obj: dict, src: str):
            """Counts and lists the occurrences of a vertex or edge.

            Args:
                obj:
                    The vertex or edge that was found in the output.
                src:
                    The label (variable name or alias) of the source where the vertex or edge
                    was found.

            A given vertex or edge can appear multiple times (in different vertex or edge sets) in
            the output of a query. Each output has a label (either the variable name or an alias
            used in the `PRINT` statement), `x_sources` contains a list of these labels.
            """
            if "x_occurrences" in obj:
                obj["x_occurrences"] += 1
            else:
                obj["x_occurrences"] = 1
            if "x_sources" in obj:
                obj["x_sources"].append(src)
            else:
                obj["x_sources"] = [src]

        logger.info("entry: parseQueryOutput")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        vs = {}
        es = {}
        ou = []

        # Outermost data type is a list
        for o1 in output:
            # Next level data type is dictionary that could be vertex sets, edge sets or generic
            # output (of simple or complex data types)
            for o2 in o1:
                _o2 = o1[o2]
                # Is it an array of dictionaries?
                if isinstance(_o2, list) and len(_o2) > 0 and isinstance(_o2[0], dict):
                    # Iterate through the array
                    for o3 in _o2:
                        if "v_type" in o3:  # It's a vertex!

                            # Handle vertex type first
                            vType = o3["v_type"]
                            vtm = {}
                            # Do we have this type of vertices in our list
                            # (which is actually a dictionary)?
                            if vType in vs:
                                vtm = vs[vType]
                            # No, let's create a dictionary for them and add to the list
                            else:
                                vtm = {}
                                vs[vType] = vtm

                            # Then handle the vertex itself
                            vId = o3["v_id"]
                            # Do we have this specific vertex (identified by the ID) in our list?
                            if vId in vtm:
                                tmp = vtm[vId]
                                attCopy(o3, tmp)
                                addOccurrences(tmp, o2)
                            else:  # No, add it
                                addOccurrences(o3, o2)
                                vtm[vId] = o3

                        elif "e_type" in o3:  # It's an edge!

                            # Handle edge type first
                            eType = o3["e_type"]
                            etm = {}
                            # Do we have this type of edges in our list
                            # (which is actually a dictionary)?
                            if eType in es:
                                etm = es[eType]
                            # No, let's create a dictionary for them and add to the list
                            else:
                                etm = {}
                                es[eType] = etm

                            # Then handle the edge itself
                            eId = o3["from_type"] + "(" + o3["from_id"] + ")->" + o3["to_type"] + \
                                  "(" + o3["to_id"] + ")"
                            o3["e_id"] = eId

                            # Add reverse edge name, if applicable
                            if eType["IsDirected"]:
                                config = eType["Config"]
                                rev = ""
                                if "REVERSE_EDGE" in config:
                                    rev = config["REVERSE_EDGE"]
                                if rev:
                                    o3["reverse_edge"] = rev

                            # Do we have this specific edge (identified by the composite ID) in our
                            # list?
                            if eId in etm:
                                tmp = etm[eId]
                                attCopy(o3, tmp)
                                addOccurrences(tmp, o2)
                            else:  # No, add it
                                addOccurrences(o3, o2)
                                etm[eId] = o3

                        else:  # It's a ... something else
                            ou.append({"label": o2, "value": _o2})
                else:  # It's a ... something else
                    ou.append({"label": o2, "value": _o2})

        ret = {"vertices": vs, "edges": es}
        if not graphOnly:
            ret["output"] = ou

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: parseQueryOutput")

        return ret

    def getStatistics(self, seconds: int = 10, segments: int = 10) -> dict:
        """Retrieves real-time query performance statistics over the given time period.

        Args:
            seconds:
                The duration of statistic collection period (the last _n_ seconds before the function
                call).
            segments:
                The number of segments of the latency distribution (shown in results as
                `LatencyPercentile`). By default, segments is `10`, meaning the percentile range 0-100%
                will be divided into ten equal segments: 0%-10%, 11%-20%, etc.
                This argument must be an integer between 1 and 100.

        Endpoint:
            - `GET /statistics/{graph_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_show_query_performance[Show query performance]
        """
        logger.info("entry: getStatistics")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if not seconds:
            seconds = 10
        else:
            seconds = max(min(seconds, 0), 60)
        if not segments:
            segments = 10
        else:
            segments = max(min(segments, 0), 100)

        ret = self._get(self.restppUrl + "/statistics/" + self.graphname + "?seconds=" +
                         str(seconds) + "&segment=" + str(segments), resKey="")

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getStatistics")

        return ret

    def describeQuery(self, queryName: str, queryDescription: str, parameterDescriptions: dict = {}):
        """Add a query description and parameter descriptions. Only supported on versions of TigerGraph >= 4.0.0.
        
        Args:
            queryName:
                The name of the query to describe.
            queryDescription:
                A description of the query.
            parameterDescriptions (optional):
                A dictionary of parameter descriptions. The keys are the parameter names and the values are the descriptions.
        
        Returns:
            The response from the database.
        """
        logger.info("entry: describeQuery")
        self.ver = self.getVer()
        major_ver, minor_ver, patch_ver = self.ver.split(".")
        if int(major_ver) < 4:
            logger.info("exit: describeQuery")
            raise TigerGraphException("This function is only supported on versions of TigerGraph >= 4.0.0.", 0)
        
        if parameterDescriptions:
            params = {"queries": [
                {"queryName": queryName,
                "description": queryDescription,
                "parameters": [{"paramName": k, "description": v} for k, v in parameterDescriptions.items()]}
            ]}
        else:
            params = {"queries": [
                {"queryName": queryName,
                "description": queryDescription}
            ]}
        if logger.level == logging.DEBUG:
            logger.debug("params: " + params)
        if self._versionGreaterThan4_0():
            res = self._put(self.gsUrl+"/gsqlserver/gsql/v1/description?graph="+self.graphname, data=params, authMode="pwd", jsonData=True)
        else:
            res = self._put(self.gsUrl+"/gsqlserver/gsql/description?graph="+self.graphname, data=params, authMode="pwd", jsonData=True)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.info("exit: describeQuery")

        return res
    
    def getQueryDescription(self, queryName: Optional[Union[str, list]] = "all"):
        """Get the description of a query. Only supported on versions of TigerGraph >= 4.0.0.
        
        Args:
            queryName:
                The name of the query to get the description of. 
                If multiple query descriptions are desired, pass a list of query names.
                If set to "all", returns the description of all queries.
        
        Returns:
            The description of the query(ies).
        """
        logger.info("entry: getQueryDescription")
        self.ver = self.getVer()
        major_ver, minor_ver, patch_ver = self.ver.split(".")
        if int(major_ver) < 4:
            logger.info("exit: getQueryDescription")
            raise TigerGraphException("This function is only supported on versions of TigerGraph >= 4.0.0.", 0)
        
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))
        
        if isinstance(queryName, list):
            queryName = ",".join(queryName)

        if self._versionGreaterThan4_0():
            res = self._get(self.gsUrl+"/gsqlserver/gsql/v1/description?graph="+self.graphname+"&query="+queryName, authMode="pwd", resKey=None)
        else:    
            res = self._get(self.gsUrl+"/gsqlserver/gsql/description?graph="+self.graphname+"&query="+queryName, authMode="pwd", resKey=None)
        if not res["error"]:
            if logger.level == logging.DEBUG:
                logger.debug("exit: getQueryDescription")
            return res["results"]["queries"]
        else:
            raise TigerGraphException(res["message"], res["code"])
        
    def dropQueryDescription(self, queryName: str, dropParamDescriptions: bool = True):
        """Drop the description of a query. Only supported on versions of TigerGraph >= 4.0.0.
        
        Args:
            queryName:
                The name of the query to drop the description of.
                If set to "*", drops the description of all queries.
            dropParamDescriptions:
                Whether to drop the parameter descriptions as well. Defaults to True.
        
        Returns:
            The response from the database.
        """
        logger.info("entry: dropQueryDescription")
        self.ver = self.getVer()
        major_ver, minor_ver, patch_ver = self.ver.split(".")
        if int(major_ver) < 4:
            logger.info("exit: describeQuery")
            raise TigerGraphException("This function is only supported on versions of TigerGraph >= 4.0.0.", 0)
        
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))
        if dropParamDescriptions:
            params = {"queries": [queryName], "queryParameters": [queryName+".*"]}
        else:
            params = {"queries": [queryName]}
        print(params)
        if self._versionGreaterThan4_0():
            res = self._delete(self.gsUrl+"/gsqlserver/gsql/v1/description?graph="+self.graphname, authMode="pwd", data=params, jsonData=True, resKey=None)
        else:
            res = self._delete(self.gsUrl+"/gsqlserver/gsql/description?graph="+self.graphname, authMode="pwd", data=params, jsonData=True, resKey=None)
        
        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.info("exit: dropQueryDescription")
        
        return res