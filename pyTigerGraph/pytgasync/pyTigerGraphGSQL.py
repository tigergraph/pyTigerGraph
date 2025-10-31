"""GSQL Interface

Use GSQL within pyTigerGraph.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import logging
import re
import httpx

from typing import Union, Tuple, Dict
from urllib.parse import urlparse, quote_plus

from pyTigerGraph.common.exception import TigerGraphException
from pyTigerGraph.common.gsql import (
    _parse_gsql,
    _prep_get_udf,
    _parse_get_udf
)

from pyTigerGraph.pytgasync.pyTigerGraphBase import AsyncPyTigerGraphBase


logger = logging.getLogger(__name__)

ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')


class AsyncPyTigerGraphGSQL(AsyncPyTigerGraphBase):
    async def gsql(self, query: str, graphname: str = None, options=None) -> Union[str, dict]:
        """Runs a GSQL query and processes the output.

        Args:
            query:
                The text of the query to run as one string. The query is one or more GSQL statement.
            graphname:
                The name of the graph to attach to. If not specified, the graph name provided at the
                time of establishing the connection will be used.
            options:
                DEPRECATED

        Returns:
            The output of the statement(s) executed.

        Endpoint:
            - `POST /gsqlserver/gsql/file` (In TigerGraph versions 3.x)
            - `POST /gsql/v1/statements` (In TigerGraph versions 4.x)
        """
        # Can't use self._isVersionGreaterThan4_0 since you need a token to call /version url
        # but you need a secret to get a token and you need this function to get a secret
        try:
            res = await self._req("POST",
                                  self.gsUrl + "/gsql/v1/statements",
                                  # quote_plus would not work with the new endpoint
                                  data=query.encode("utf-8"),
                                  authMode="pwd", resKey=None, skipCheck=True,
                                  jsonResponse=False,
                                  headers={"Content-Type": "text/plain"})
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                res = await self._req("POST",
                                      self.gsUrl + "/gsqlserver/gsql/file",
                                      data=quote_plus(query.encode("utf-8")),
                                      authMode="pwd", resKey=None, skipCheck=True,
                                      jsonResponse=False)
            else:
                raise e
        return _parse_gsql(res, query, graphname=graphname, options=options)

    # TODO IMPLEMENT INSTALL_UDF

    async def getUDF(self, ExprFunctions: bool = True, ExprUtil: bool = True, json_out=False) -> Union[str, Tuple[str, str], Dict[str, str]]:
        """Get user defined functions (UDF) installed in the database.
        See https://docs.tigergraph.com/gsql-ref/current/querying/func/query-user-defined-functions for details on UDFs.

        Args:
            ExprFunctions (bool, optional):
                Whether to get ExprFunctions. Defaults to True.
            ExprUtil (bool, optional):
                Whether to get ExprUtil. Defaults to True.
            json_out (bool, optional):
                Whether to output as JSON. Defaults to False.
                Only supported on version >=4.1

        Returns:
            str: If only one of `ExprFunctions` or `ExprUtil` is True, return of the content of that file.
            Tuple[str, str]: content of ExprFunctions and content of ExprUtil.

        Endpoints:
            - `GET /gsqlserver/gsql/userdefinedfunction?filename={ExprFunctions or ExprUtil}` (In TigerGraph versions 3.x)
            - `GET /gsql/v1/udt/files/{ExprFunctions or ExprUtil}` (In TigerGraph versions 4.x)
        """
        logger.debug("entry: getUDF")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        urls, alt_urls = _prep_get_udf(
            ExprFunctions=ExprFunctions, ExprUtil=ExprUtil)
        if not await self._version_greater_than_4_0():
            if json_out:
                raise TigerGraphException(
                    "The 'json_out' parameter is only supported in TigerGraph Versions >=4.1.")
            urls = alt_urls
        responses = {}

        for file_name in urls:
            resp = await self._req("GET", f"{self.gsUrl}{urls[file_name]}", resKey="")
            responses[file_name] = resp

        return _parse_get_udf(responses, json_out=json_out)

    async def getAsyncRequestStatus(self, requestId: str) -> dict:
        """Check status of asynchronous request with requestId.

        Args:
            requestId (str):
                The request ID of the asynchronous statement to check.

        Returns:
            dict: The response from the database containing the statement status.

        Endpoints:
            - `GET /gsql/v1/statements/{requestId}` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: getAsyncRequestStatus")
        if not await self._version_greater_than_4_0():
            logger.debug("exit: getAsyncRequestStatus")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        res = await self._req("GET", self.gsUrl+"/gsql/v1/statements/"+requestId,
                             authMode="pwd", resKey="", headers={'Content-Type': 'application/json'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: getAsyncRequestStatus")

        return res

    async def cancelAsyncRequest(self, requestId: str) -> dict:
        """Cancel an asynchronous request with requestId.

        Args:
            requestId (str):
                The request ID of the asynchronous statement to cancel.

        Returns:
            dict: The response from the database containing the cancellation result.

        Endpoints:
            - `PUT /gsql/v1/statements/{requestId}/cancel` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: cancelAsyncRequest")
        if not await self._version_greater_than_4_0():
            logger.debug("exit: cancelAsyncRequest")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        res = await self._req("PUT", self.gsUrl+"/gsql/v1/statements/"+requestId+"/cancel",
                             authMode="pwd", resKey="", headers={'Content-Type': 'application/json'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: cancelAsyncRequest")

        return res

    async def recoverCatalog(self) -> dict:
        """Recover gdict catalog.

        Args:
            None

        Returns:
            dict: The response from the database containing the recovery result.

        Endpoints:
            - `POST /gsql/v1/schema/recover` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: recoverCatalog")
        if not await self._version_greater_than_4_0():
            logger.debug("exit: recoverCatalog")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        res = await self._req("POST", self.gsUrl+"/gsql/v1/schema/recover",
                             authMode="pwd", resKey="", headers={'Content-Type': 'text/plain'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: recoverCatalog")

        return res

    async def clearGraphStore(self) -> dict:
        """Clear graph store.

        This endpoint permanently deletes all the data out of the graph store (database),
        for all graphs. It does not delete the database schema, nor does it delete queries
        or loading jobs. It is equivalent to the GSQL command CLEAR GRAPH STORE.

        WARNING: This operation is not reversible. The deleted data cannot be recovered.

        Args:
            None

        Returns:
            dict: The response from the database containing the clear operation result.

        Endpoints:
            - `GET /gsql/v1/clear-store` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: clearGraphStore")
        if not await self._version_greater_than_4_0():
            logger.debug("exit: clearGraphStore")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        res = await self._req("GET", self.gsUrl+"/gsql/v1/clear-store",
                             authMode="pwd", resKey="", headers={'Content-Type': 'application/json'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: clearGraphStore")

        return res

    async def dropAll(self) -> dict:
        """Drop all.

        This endpoint drops all graphs, vertices, edges, queries, and loading jobs
        from the database. This operation is equivalent to dropping everything
        and starting fresh.

        WARNING: This operation is not reversible. The deleted data cannot be recovered.

        Args:
            None

        Returns:
            dict: The response from the database containing the drop operation result.

        Endpoints:
            - `GET /gsql/v1/drop-all` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: dropAll")
        if not await self._version_greater_than_4_0():
            logger.debug("exit: dropAll")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        res = await self._req("GET", self.gsUrl+"/gsql/v1/drop-all",
                             authMode="pwd", resKey="", headers={'Content-Type': 'application/json'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: dropAll")

        return res

    async def exportDatabase(self, path: str, graphNames: list = None, schema: bool = False,
                           template: bool = False, data: bool = False, users: bool = False,
                           password: str = None, separator: str = "\u001d", eol: str = "\u001c") -> dict:
        """Export database.

        Args:
            path (str):
                The path where the database export will be saved.
            graphNames (list, optional):
                List of graph names to export. Defaults to ["*"] for all graphs.
            schema (bool, optional):
                Whether to export schema. Defaults to False.
            template (bool, optional):
                Whether to export templates. Defaults to False.
            data (bool, optional):
                Whether to export data. Defaults to False.
            users (bool, optional):
                Whether to export users. Defaults to False.
            password (str, optional):
                Password for the export operation.
            separator (str, optional):
                Field separator character. Defaults to "\u001d".
            eol (str, optional):
                End of line character. Defaults to "\u001c".

        Returns:
            dict: The response from the database containing the export operation result.

        Endpoints:
            - `POST /gsql/v1/db-export` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: exportDatabase")
        if not await self._version_greater_than_4_0():
            logger.debug("exit: exportDatabase")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if graphNames is None:
            graphNames = ["*"]

        data = {
            "path": path,
            "graphNames": graphNames,
            "schema": schema,
            "template": template,
            "data": data,
            "users": users,
            "separator": separator,
            "eol": eol
        }

        if password is None:
            password = self.password

        if password is not None:
            data["password"] = password

        res = await self._req("POST", self.gsUrl+"/gsql/v1/db-export",
                             data=data, authMode="pwd", resKey="",
                             headers={'Content-Type': 'application/json'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: exportDatabase")

        return res

    async def importDatabase(self, path: str, graphNames: list = None, keepUsers: bool = False,
                           password: str = None) -> dict:
        """Import database.

        Args:
            path (str):
                The path where the database import will be loaded from.
            graphNames (list, optional):
                List of graph names to import. Defaults to ["*"] for all graphs.
            keepUsers (bool, optional):
                Whether to keep existing users. Defaults to False.
            password (str, optional):
                Password for the import operation.

        Returns:
            dict: The response from the database containing the import operation result.

        Endpoints:
            - `POST /gsql/v1/db-import` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: importDatabase")
        if not await self._version_greater_than_4_0():
            logger.debug("exit: importDatabase")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if graphNames is None:
            graphNames = ["*"]

        data = {
            "path": path,
            "graphNames": graphNames,
            "keepUsers": keepUsers
        }

        if password is None:
            password = self.password

        if password is not None:
            data["password"] = password

        res = await self._req("POST", self.gsUrl+"/gsql/v1/db-import",
                             data=data, authMode="pwd", resKey="",
                             headers={'Content-Type': 'application/json'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: importDatabase")

        return res

    async def getGSQLVersion(self, verbose: bool = False) -> dict:
        """Get GSQL version information.

        Args:
            verbose (bool, optional):
                Whether to return detailed version information. Defaults to False.

        Returns:
            dict: The response from the database containing the GSQL version information.

        Endpoints:
            - `GET /gsql/v1/version` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: getGSQLVersion")
        if not await self._version_greater_than_4_0():
            logger.debug("exit: getGSQLVersion")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        params = {}
        if verbose:
            params["verbose"] = verbose

        res = await self._req("GET", self.gsUrl+"/gsql/v1/version",
                             params=params, authMode="pwd", resKey="",
                             headers={'Content-Type': 'text/plain'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: getGSQLVersion")

        return res
