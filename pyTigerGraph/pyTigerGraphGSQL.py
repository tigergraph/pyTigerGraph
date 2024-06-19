"""GSQL Interface

Use GSQL within pyTigerGraph.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import logging
import os
import sys
from typing import Union, Tuple
from urllib.parse import urlparse, quote_plus
import re


import requests

from .pyTigerGraphBase import pyTigerGraphBase
from .pyTigerGraphException import TigerGraphException

logger = logging.getLogger(__name__)

ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')


class pyTigerGraphGSQL(pyTigerGraphBase):
    def gsql(self, query: str, graphname: str = None, options=None) -> Union[str, dict]:
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
        """
        logger.info("entry: gsql")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        def check_error(query: str, resp: str) -> None:
            if "CREATE VERTEX" in query.upper():
                if "Failed to create vertex types" in resp:
                    raise TigerGraphException(resp)
            if ("CREATE DIRECTED EDGE" in query.upper()) or ("CREATE UNDIRECTED EDGE" in query.upper()):
                if "Failed to create edge types" in resp:
                    raise TigerGraphException(resp)
            if "CREATE GRAPH" in query.upper():
                if ("The graph" in resp) and ("could not be created!" in resp):
                    raise TigerGraphException(resp)
            if "CREATE DATA_SOURCE" in query.upper():
                if ("Successfully created local data sources" not in resp) and ("Successfully created data sources" not in resp):
                    raise TigerGraphException(resp)
            if "CREATE LOADING JOB" in query.upper():
                if "Successfully created loading jobs" not in resp:
                    raise TigerGraphException(resp)
            if "RUN LOADING JOB" in query.upper():
                if "LOAD SUCCESSFUL" not in resp:
                    raise TigerGraphException(resp)
                
        def clean_res(resp: list) -> str:
            ret = []
            for line in resp:
                if not line.startswith("__GSQL__"):
                    ret.append(line)
            return "\n".join(ret)

        if graphname is None:
            graphname = self.graphname
        if str(graphname).upper() == "GLOBAL" or str(graphname).upper() == "":
            graphname = ""

        if self._versionGreaterThan4_0():
            res = self._req("POST",
                        self.gsUrl + "/gsqlserver/gsql/v1/statements",
                        data=quote_plus(query.encode("utf-8")),
                        authMode="pwd", resKey=None, skipCheck=True,
                        jsonResponse=False)
        else:            
            res = self._req("POST",
                            self.gsUrl + "/gsqlserver/gsql/file",
                            data=quote_plus(query.encode("utf-8")),
                            authMode="pwd", resKey=None, skipCheck=True,
                            jsonResponse=False)


        if isinstance(res, list):
            ret = clean_res(res)
        else:
            ret = clean_res(res.splitlines())

        check_error(query, ret)

        string_without_ansi = ANSI_ESCAPE.sub('', ret)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: gsql (success)")

        return string_without_ansi

    def installUDF(self, ExprFunctions: str = "", ExprUtil: str = "") -> None:
        """Install user defined functions (UDF) to the database.
        See https://docs.tigergraph.com/gsql-ref/current/querying/func/query-user-defined-functions for details on UDFs.

        Args:
            ExprFunctions (str, optional):
                Path or URL to the file for ExprFunctions. Defaults to '' (a blank path/URL).
            ExprUtil (str, optional):
                Path or URL to the file for ExprUtil. Defaults to '' (a blank path/URL).

        Returns:
            Status of the installation.
        """
        logger.info("entry: installUDF")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if ExprFunctions:
            if ExprFunctions.startswith("http"):
                # A URL: fetch from online.
                data = requests.get(ExprFunctions).text
            else:
                # A local file: read from disk.
                with open(ExprFunctions) as infile:
                    data = infile.read()
            
            if self._versionGreaterThan4_0():
                res = self._req("PUT",
                    url="{}/gsqlserver/gsql/v1/udt/files/ExprFunctions".format(
                        self.gsUrl), authMode="pwd", data=data, resKey="")
            else:    
                res = self._req("PUT",
                    url="{}/gsqlserver/gsql/userdefinedfunction?filename=ExprFunctions".format(
                        self.gsUrl), authMode="pwd", data=data, resKey="")
            if not res["error"]:
                logger.info("ExprFunctions installed successfully")
            else:
                logger.error("Failed to install ExprFunctions")
                raise TigerGraphException(res["message"])

        if ExprUtil:
            if ExprUtil.startswith("http"):
                # A URL: fetch from online.
                data = requests.get(ExprUtil).text
            else:
                # A local file: read from disk.
                with open(ExprUtil) as infile:
                    data = infile.read()
            if self._versionGreaterThan4_0():
                res = self._req("PUT",
                    url="{}/gsqlserver/gsql/v1/udt/files/ExprUtil".format(self.gsUrl),
                        authMode="pwd", data=data, resKey="")
            else:
                res = self._req("PUT",
                    url="{}/gsqlserver/gsql/userdefinedfunction?filename=ExprUtil".format(self.gsUrl),
                        authMode="pwd", data=data, resKey="")
            if not res["error"]:
                logger.info("ExprUtil installed successfully")
            else:
                logger.error("Failed to install ExprUtil")
                raise TigerGraphException(res["message"])

        if logger.level == logging.DEBUG:
            logger.debug("return: 0")
        logger.info("exit: installUDF")

        return 0

    def getUDF(self, ExprFunctions: bool = True, ExprUtil: bool = True) -> Union[str, Tuple[str, str]]:       
        """Get user defined functions (UDF) installed in the database.
        See https://docs.tigergraph.com/gsql-ref/current/querying/func/query-user-defined-functions for details on UDFs.

        Args:
            ExprFunctions (bool, optional):
                Whether to get ExprFunctions. Defaults to True.
            ExprUtil (bool, optional):
                Whether to get ExprUtil. Defaults to True.

        Returns:
            str: If only one of `ExprFunctions` or `ExprUtil` is True, return of the content of that file.
            Tuple[str, str]: content of ExprFunctions and content of ExprUtil.
        """
        logger.info("entry: getUDF")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        functions_ret = None
        if ExprFunctions:
            if self._versionGreaterThan4_0():
                resp = self._get(
                    "{}/gsqlserver/gsql/v1/udt/files/ExprFunctions".format(self.gsUrl),
                    resKey="")
            else:
                resp = self._get(
                    "{}/gsqlserver/gsql/userdefinedfunction".format(self.gsUrl),
                    params={"filename": "ExprFunctions"}, resKey="")
            if not resp["error"]:
                logger.info("ExprFunctions get successfully")
                functions_ret = resp["results"]
            else:
                logger.error("Failed to get ExprFunctions")
                raise TigerGraphException(resp["message"])
        
        util_ret = None
        if ExprUtil:
            if self._versionGreaterThan4_0():
                resp = self._get(
                    "{}/gsqlserver/gsql/v1/udt/files/ExprUtil".format(self.gsUrl),
                    resKey="")
            else:
                resp = self._get(
                    "{}/gsqlserver/gsql/userdefinedfunction".format(self.gsUrl),
                    params={"filename": "ExprUtil"}, resKey="")
            if not resp["error"]:
                logger.info("ExprUtil get successfully")
                util_ret = resp["results"]
            else:
                logger.error("Failed to get ExprUtil")
                raise TigerGraphException(resp["message"])

        if (functions_ret is not None) and (util_ret is not None):
            return (functions_ret, util_ret)
        elif functions_ret is not None:
            return functions_ret
        elif util_ret is not None:
            return util_ret
        else:
            return ""

