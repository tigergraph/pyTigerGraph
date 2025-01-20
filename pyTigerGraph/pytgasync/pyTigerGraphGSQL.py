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
        logger.info("entry: getUDF")
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
