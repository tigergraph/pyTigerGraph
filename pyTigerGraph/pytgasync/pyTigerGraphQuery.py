"""Query Functions.

The functions on this page run installed or interpret queries in TigerGraph.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import logging
import asyncio

from typing import TYPE_CHECKING, Union, Optional

if TYPE_CHECKING:
    import pandas as pd

from pyTigerGraph.common.exception import TigerGraphException
from pyTigerGraph.common.query import (
    _parse_get_installed_queries,
    _parse_query_parameters,
    _prep_run_installed_query,
    _prep_get_statistics
)
from pyTigerGraph.pytgasync.pyTigerGraphGSQL import AsyncPyTigerGraphGSQL

logger = logging.getLogger(__name__)


class AsyncPyTigerGraphQuery(AsyncPyTigerGraphGSQL):
    # TODO getQueries()  # List _all_ query names
    async def showQuery(self, queryName: str) -> str:
        """Returns the string of the given GSQL query.

        Args:
            queryName (str):
                Name of the query to get metadata of.
        """
        if logger.level == logging.DEBUG:
            logger.debug("entry: showQuery")
        res = await self.gsql("USE GRAPH "+self.graphname+" SHOW QUERY "+queryName)
        if logger.level == logging.DEBUG:
            logger.debug("exit: showQuery")
        return res

    async def getQueryMetadata(self, queryName: str) -> dict:
        """Returns metadata details about a query. 
        Specifically, it lists the input parameters in the same order as they exist in the query
        and outputs `PRINT` statement syntax.

        Args:
            queryName (str):
                Name of the query to get metadata of.

        Endpoints:
            - `POST /gsqlserver/gsql/queryinfo` (In TigerGraph versions 3.x)
                See xref:tigergraph-server:API:built-in-endpoints.adoc_get_query_metadata
            - `POST /gsql/v1/queries/signature` (In TigerGraph versions 4.x)
        """
        if logger.level == logging.DEBUG:
            logger.debug("entry: getQueryMetadata")
        if await self._version_greater_than_4_0():
            params = {"graph": self.graphname, "queryName": queryName}
            res = await self._req("POST", self.gsUrl+"/gsql/v1/queries/signature", params=params, authMode="pwd", resKey="")
        else:
            params = {"graph": self.graphname, "query": queryName}
            res = await self._req("GET", self.gsUrl+"/gsqlserver/gsql/queryinfo", params=params, authMode="pwd", resKey="")
        if not res["error"]:
            if logger.level == logging.DEBUG:
                logger.debug("exit: getQueryMetadata")
            return res
        else:
            TigerGraphException(res["message"], res["code"])

    async def getQueryContent(self, queryName: str) -> dict:
        """Returns the content/source code of a query.

        Args:
            queryName (str):
                Name of the query to get content of.

        Returns:
            The response from the database containing the query content.

        Endpoints:
            - `GET /gsql/v1/queries/{queryName}` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: getQueryContent")
        if not await self._version_greater_than_4_0():
            logger.debug("exit: getQueryContent")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        params = {"graph": self.graphname}
        res = await self._req("GET", self.gsUrl+"/gsql/v1/queries/"+queryName,
                             params=params, authMode="pwd", resKey="", headers={'Content-Type': 'application/json'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: getQueryContent")

        return res

    async def createQuery(self, queryText: str) -> dict:
        """Creates a query in the graph.

        Args:
            queryText (str):
                The text of the GSQL query to create. Must be in the format:
                "create query queryName (...) FOR GRAPH graphName { ... }"

        Returns:
            The response from the database containing the creation result.

        Endpoints:
            - `POST /gsql/v1/queries` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: createQuery")
        if not await self._version_greater_than_4_0():
            logger.debug("exit: createQuery")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        # Replace graphname placeholders
        queryText = queryText.replace("$graphname", self.graphname)
        queryText = queryText.replace("@graphname@", self.graphname)

        params = {"graph": self.graphname}
        res = await self._req("POST", self.gsUrl+"/gsql/v1/queries",
                             params=params, data=queryText, authMode="pwd",
                             headers={'Content-Type': 'text/plain'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: createQuery")

        return res

    async def dropQueries(self, queryName: Union[str, list]) -> dict:
        """Drops one or more queries from the graph.

        Args:
            queryName (str or list):
                Name of the query to drop, or list of query names to drop.

        Returns:
            The response from the database containing the drop result.

        Endpoints:
            - `DELETE /gsql/v1/queries/{queryName}` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: dropQueries")
        if not await self._version_greater_than_4_0():
            logger.debug("exit: dropQueries")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        # Handle single query name
        if isinstance(queryName, str):
            params = {"graph": self.graphname}
            res = await self._req("DELETE", self.gsUrl+"/gsql/v1/queries/"+queryName,
                                 params=params, authMode="pwd", resKey="", headers={'Content-Type': 'application/json'})
        # Handle list of query names
        elif isinstance(queryName, list):
            if not queryName:
                raise TigerGraphException("Query name list cannot be empty.", 0)

            params = {"graph": self.graphname, "query": queryName}
            res = await self._req("DELETE", self.gsUrl+"/gsql/v1/queries",
                                 params=params, authMode="pwd", resKey="")
        else:
            raise TigerGraphException("queryName must be a string or list of strings.", 0)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: dropQueries")

        return res

    async def checkQuerySemantic(self, queryCode: str) -> dict:
        """Performs a semantic check of a query.

        Args:
            queryCode (str):
                The GSQL query code to check for semantic errors.

        Returns:
            The response from the database containing the semantic check result.

        Endpoints:
            - `POST /gsql/v1/internal/check/query` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: checkQuerySemantic")
        if not await self._version_greater_than_4_0():
            logger.debug("exit: checkQuerySemantic")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        data = {"code": queryCode}
        res = await self._req("POST", self.gsUrl+"/gsql/v1/internal/check/query",
                             data=data, authMode="pwd", resKey="",
                             headers={'Content-Type': 'application/json'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: checkQuerySemantic")

        return res

    async def getQueryInfo(self, queryName: str = None, status: str = None) -> dict:
        """Gets query information for the graph.

        Args:
            queryName (str, optional):
                The specific query name to get information for. If None, returns info for all queries.
            status (str, optional):
                Filter queries by status (e.g., "VALID", "INVALID", "INSTALLING").

        Returns:
            The response from the database containing query information.

        Endpoints:
            - `GET /gsql/v1/queries/info` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: getQueryInfo")
        if not await self._version_greater_than_4_0():
            logger.debug("exit: getQueryInfo")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        params = {"graph": self.graphname}
        if queryName is not None:
            params["query"] = queryName
        if status is not None:
            params["status"] = status

        res = await self._req("GET", self.gsUrl+"/gsql/v1/queries/info",
                             params=params, authMode="pwd", resKey="", headers={'Content-Type': 'application/json'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: getQueryInfo")

        return res

    async def listQueryNames(self) -> list:
        """Lists all query names of a graph.

        Returns:
            The response from the database containing the list of query names.

        Endpoints:
            - `GET /gsql/v1/queries` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: listQueryNames")
        if not await self._version_greater_than_4_0():
            logger.debug("exit: listQueryNames")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        params = {"graph": self.graphname}
        res = await self._req("GET", self.gsUrl+"/gsql/v1/queries",
                             params=params, authMode="pwd", headers={'Content-Type': 'application/json'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: listQueryNames")

        return res

    async def getInstalledQueries(self, fmt: str = "py") -> Union[dict, str, 'pd.DataFrame']:
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
        logger.debug("entry: getInstalledQueries")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = await self.getEndpoints(dynamic=True)
        ret = _parse_get_installed_queries(fmt, ret)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getInstalledQueries")

        return ret

    async def installQueries(self, queries: Union[str, list], flag: Union[str, list] = None) -> str:
        """Installs one or more queries.

        Args:
            queries:
                A single query string or a list of query strings to install. Use '*' or 'all' to install all queries.
            flag:
                Method to install queries.
                - '-single' Install the query in single gpr mode. 
                - '-legacy' Install the query in UDF mode.
                - '-debug' Present results contains debug info.
                - '-cost' Present results contains performance consumption.
                - '-force' Install the query even if it already installed.

        Returns:
            The response from the server.

        Endpoints:
            GET /gsql/v1/queries/install
            See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_install_a_query
        """
        logger.debug("entry: installQueries")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))
        self.ver = await self.getVer()
        major_ver, minor_ver, patch_ver = self.ver.split(".")
        if int(major_ver) < 4 or int(major_ver) == 4 and int(minor_ver) == 0:
            logger.debug("exit: installQueries")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.1.0.", 0)

        params = {}
        params["graph"] = self.graphname
        if isinstance(queries, list):
            queries = ",".join(queries)
        params["queries"] = queries

        if flag:
            if isinstance(flag, list):
                flag = ",".join(flag)
            params["flag"] = flag

        request_id = await self._req("GET", self.gsUrl + "/gsql/v1/queries/install", params=params, authMode="pwd", resKey="requestId")

        ret = None
        while not ret:
            ret = await self._req("GET", self.gsUrl + "/gsql/v1/queries/install/" + str(request_id), authMode="pwd", resKey="")
            if "SUCCESS" in ret["message"] or "FAILED" in ret["message"]:
                break
            else:
                ret = None
            await asyncio.sleep(1)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: installQueries")

        return ret

    async def getQueryInstallationStatus(self, requestId: str) -> dict:
        """Get the status of query installation.

        Args:
            requestId:
                The request ID returned from installQueries.

        Returns:
            A dictionary containing the installation status.

        Endpoints:
            GET /gsql/queries/install/{request_id}
            See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_check_query_installation_status
        """
        logger.debug("entry: getQueryInstallationStatus")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))
        self.ver = await self.getVer()
        major_ver, minor_ver, patch_ver = self.ver.split(".")
        if int(major_ver) < 4 or int(major_ver) == 4 and int(minor_ver) == 0:
            logger.debug("exit: getQueryInstallationStatus")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.1.0.", 0)

        ret = await self._req("GET", self.gsUrl + "/gsql/v1/queries/install/" + requestId, authMode="pwd", resKey="")

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getQueryInstallationStatus")

        return ret

    async def runInstalledQuery(self, queryName: str, params: Union[str, dict] = None,
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
        logger.debug("entry: runInstalledQuery")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        headers, res_key = _prep_run_installed_query(timeout=timeout, sizeLimit=sizeLimit, runAsync=runAsync,
                                                          replica=replica, threadLimit=threadLimit, memoryLimit=memoryLimit)

        if usePost:
            ret = await self._req("POST", self.restppUrl + "/query/" + self.graphname + "/" + queryName,
                                  data=params, headers=headers, resKey=res_key, jsonData=True)

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(ret))
            logger.debug("exit: runInstalledQuery (POST)")

            return ret
        else:
            # for params contains spaces, we need to append to url directly to keep %20 format
            if params:
                if isinstance(params, dict):
                    params = _parse_query_parameters(params)
                ret = await self._req("GET", self.restppUrl + "/query/" + self.graphname + "/" + queryName + "?" + str(params),
                                  headers=headers, resKey=res_key)
            else:
                ret = await self._req("GET", self.restppUrl + "/query/" + self.graphname + "/" + queryName,
                                  headers=headers, resKey=res_key)

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(ret))
            logger.debug("exit: runInstalledQuery (GET)")

            return ret

    async def checkQueryStatus(self, requestId: str = ""):
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
            return await self._req("GET", self.restppUrl + "/query_status?graph_name="+self.graphname+"&requestid="+requestId)
        else:
            return await self._req("GET", self.restppUrl + "/query_status?graph_name="+self.graphname+"&requestid=all")

    async def getQueryResult(self, requestId: str = ""):
        """Gets the result of a detached query.

        Args:
            requestId (str):
                String ID of the request.
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_check_query_results_detached_mode[Check query results (detached mode)]
        """
        return await self._req("GET", self.restppUrl + "/query_result?graph_name="+self.graphname+"&requestid="+requestId)

    async def runInterpretedQuery(self, queryText: str, params: Union[str, dict] = None) -> list:
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


        Endpoints:
            - `POST /gsqlserver/interpreted_query` (In TigerGraph versions 3.x)
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_run_an_interpreted_query[Run an interpreted query]
            - `POST /gsql/v1/queries/interpret` (In TigerGraph versions 4.x)

        TODO Add "GSQL-TIMEOUT: <timeout value in ms>" and "RESPONSE-LIMIT: <size limit in byte>"
            plus parameters if applicable to interpreted queries (see runInstalledQuery() above)
        """
        logger.debug("entry: runInterpretedQuery")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        queryText = queryText.replace("$graphname", self.graphname)
        queryText = queryText.replace("@graphname@", self.graphname)
        if isinstance(params, dict):
            params = _parse_query_parameters(params)

        if await self._version_greater_than_4_0():
            ret = await self._req("POST", self.gsUrl + "/gsql/v1/queries/interpret",
                                  params=params, data=queryText, authMode="pwd",
                                  headers={'Content-Type': 'text/plain'})
        else:
            ret = await self._req("POST", self.gsUrl + "/gsqlserver/interpreted_query", data=queryText,
                                  params=params, authMode="pwd")

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: runInterpretedQuery")

        return ret

    async def getRunningQueries(self) -> dict:
        """Reports the statistics of currently running queries on the graph.
        """
        if logger.level == logging.DEBUG:
            logger.debug("entry: getRunningQueries")
        res = await self._req("GET", self.restppUrl+"/showprocesslist/"+self.graphname, resKey="")
        if not res["error"]:
            if logger.level == logging.DEBUG:
                logger.debug("exit: getRunningQueries")
            return res
        else:
            raise TigerGraphException(res["message"], res["code"])

    async def abortQuery(self, request_id: Union[str, list] = None, url: str = None):
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
        res = await self._get(self.restppUrl+"/abortquery/"+self.graphname, params=params, resKey="")
        if not res["error"]:
            if logger.level == logging.DEBUG:
                logger.debug("exit: abortQuery")
            return res
        else:
            raise TigerGraphException(res["message"], res["code"])

    async def getStatistics(self, seconds: int = 10, segments: int = 10) -> dict:
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
        logger.debug("entry: getStatistics")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        seconds, segments = _prep_get_statistics(self, seconds, segments)
        ret = await self._req("GET", self.restppUrl + "/statistics/" + self.graphname + "?seconds=" +
                              str(seconds) + "&segment=" + str(segments), resKey="")

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getStatistics")

        return ret

    async def describeQuery(self, queryName: str, queryDescription: str, parameterDescriptions: dict = {}):
        """DEPRECATED: Use updateQueryDescription() instead. Add a query description and parameter descriptions. Only supported on versions of TigerGraph >= 4.0.0.

        Args:
            queryName:
                The name of the query to describe.
            queryDescription:
                A description of the query.
            parameterDescriptions (optional):
                A dictionary of parameter descriptions. The keys are the parameter names and the values are the descriptions.

        Returns:
            The response from the database.

        Endpoints:
            - `PUT /gsqlserver/gsql/description?graph={graph_name}` (In TigerGraph version 4.0)
            - `PUT /gsql/v1/description?graph={graph_name}` (In TigerGraph versions >4.0)
        """
        return await self.updateQueryDescription(queryName, queryDescription, parameterDescriptions)

    async def updateQueryDescription(self, queryName: str, queryDescription: str, parameterDescriptions: dict = {}):
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

        Endpoints:
            - `PUT /gsqlserver/gsql/description?graph={graph_name}` (In TigerGraph version 4.0)
            - `PUT /gsql/v1/description?graph={graph_name}` (In TigerGraph versions >4.0)
        """
        logger.debug("entry: updateQueryDescription")
        self.ver = await self.getVer()
        major_ver, minor_ver, patch_ver = self.ver.split(".")
        if int(major_ver) < 4:
            logger.debug("exit: updateQueryDescription")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.0.", 0)

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
        if await self._version_greater_than_4_0():
            res = await self._put(self.gsUrl+"/gsql/v1/description?graph="+self.graphname, data=params, authMode="pwd", jsonData=True)
        else:
            res = await self._put(self.gsUrl+"/gsqlserver/gsql/description?graph="+self.graphname, data=params, authMode="pwd", jsonData=True)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: updateQueryDescription")

        return res

    async def getQueryDescription(self, queryName: Optional[Union[str, list]] = "all"):
        """Get the description of a query. Only supported on versions of TigerGraph >= 4.0.0.

        Args:
            queryName:
                The name of the query to get the description of. 
                If multiple query descriptions are desired, pass a list of query names.
                If set to "all", returns the description of all queries.

        Returns:
            The description of the query(ies).

        Endpoints:
            - `GET /gsqlserver/gsql/description?graph={graph_name}` (In TigerGraph version 4.0)
            - `GET /gsql/v1/description?graph={graph_name}` (In TigerGraph versions >4.0)
        """
        logger.debug("entry: getQueryDescription")
        self.ver = await self.getVer()
        major_ver, minor_ver, patch_ver = self.ver.split(".")
        if int(major_ver) < 4:
            logger.debug("exit: getQueryDescription")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.0.", 0)

        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if isinstance(queryName, list):
            queryName = ",".join(queryName)

        if await self._version_greater_than_4_0():
            res = await self._get(self.gsUrl+"/gsql/v1/description?graph="+self.graphname+"&query="+queryName, authMode="pwd", resKey=None)
        else:
            res = await self._get(self.gsUrl+"/gsqlserver/gsql/description?graph="+self.graphname+"&query="+queryName, authMode="pwd", resKey=None)
        if not res["error"]:
            if logger.level == logging.DEBUG:
                logger.debug("exit: getQueryDescription")
            return res["results"]["queries"]
        else:
            raise TigerGraphException(res["message"], res["code"])

    async def dropQueryDescription(self, queryName: str, dropParamDescriptions: bool = True):
        """Drop the description of a query. Only supported on versions of TigerGraph >= 4.0.0.

        Args:
            queryName:
                The name of the query to drop the description of.
                If set to "*", drops the description of all queries.
            dropParamDescriptions:
                Whether to drop the parameter descriptions as well. Defaults to True.

        Returns:
            The response from the database.

        Endpoints:
            - `DELETE /gsqlserver/gsql/description?graph={graph_name}` (In TigerGraph version 4.0)
            - `DELETE /gsql/v1/description?graph={graph_name}` (In TigerGraph versions >4.0)
        """
        logger.debug("entry: dropQueryDescription")
        self.ver = await self.getVer()
        major_ver, minor_ver, patch_ver = self.ver.split(".")
        if int(major_ver) < 4:
            logger.debug("exit: describeQuery")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.0.", 0)

        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))
        if dropParamDescriptions:
            params = {"queries": [queryName],
                      "queryParameters": [queryName+".*"]}
        else:
            params = {"queries": [queryName]}
        print(params)
        if await self._versionGreaterThan4_0():
            res = await self._delete(self.gsUrl+"/gsql/v1/description?graph="+self.graphname, authMode="pwd", data=params, jsonData=True, resKey=None)
        else:
            res = await self._delete(self.gsUrl+"/gsqlserver/gsql/description?graph="+self.graphname, authMode="pwd", data=params, jsonData=True, resKey=None)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: dropQueryDescription")

        return res
