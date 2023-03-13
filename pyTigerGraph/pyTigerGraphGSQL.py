"""GSQL Interface

Use GSQL within pyTigerGraph.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import logging
import os
import sys
from typing import Union
from urllib.parse import urlparse
import re


import requests
from pyTigerDriver import GSQL_Client

from pyTigerGraph.pyTigerGraphBase import pyTigerGraphBase
from pyTigerGraph.pyTigerGraphException import TigerGraphException

logger = logging.getLogger(__name__)

ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')


class pyTigerGraphGSQL(pyTigerGraphBase):
    def _initGsql(self, certLocation: str = "~/.gsql/my-cert.txt") -> bool:
        """Initialises the GSQL support.

        Args:
            certLocation:
                The path and file of the CA certificate.

        Returns:
            `True` if initialization was successful.

        Raises:
            Exception if initialization was unsuccessful.
        """
        logger.info("entry: _initGsql")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if not certLocation:
            if not os.path.isdir(os.path.expanduser("~/.gsql")):
                os.mkdir(os.path.expanduser("~/.gsql"))
            certLocation = "~/.gsql/my-cert.txt"

        self.certLocation = os.path.expanduser(certLocation)
        self.url = urlparse(self.gsUrl).netloc  # Getting URL with gsql port w/o https://
        sslhost = str(self.url.split(":")[0])

        if self.downloadCert:  # HTTP/HTTPS
            import ssl
            try:
                Res = ssl.get_server_certificate((sslhost, int(self.sslPort)))
            except:  # TODO PEP 8: E722 do not use bare 'except'
                Res = ssl.get_server_certificate((sslhost, 14240))

            try:
                certcontent = open(self.certLocation, "w")
                certcontent.write(Res)
                certcontent.close()
            except Exception:  # TODO Too broad exception clause
                self.certLocation = "/tmp/my-cert.txt"

                certcontent = open(self.certLocation, "w")
                certcontent.write(Res)
                certcontent.close()
            if os.stat(self.certLocation).st_size == 0:
                raise TigerGraphException(
                    "Certificate download failed. Please check that the server is online.",
                    None,
                )

        try:
            if self.downloadCert or self.certPath:
                if not self.certPath:
                    self.certPath = self.certLocation
                self.Client = GSQL_Client(
                    urlparse(self.host).netloc,
                    version=self.version,
                    username=self.username,
                    password=self.password,
                    cacert=self.certPath,
                    gsPort=self.gsPort,
                    restpp=self.restppPort,
                    debug=(logger.level == logging.DEBUG)
                )
            else:
                self.Client = GSQL_Client(
                    urlparse(self.host).netloc,
                    version=self.version,
                    username=self.username,
                    password=self.password,
                    gsPort=self.gsPort,
                    restpp=self.restppPort,
                    debug=(logger.level == logging.DEBUG)
                )
            self.Client.login()
            self.gsqlInitiated = True

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(True))
            logger.info("exit: _initGsql (success)")

            return True
        except Exception as e:
            self.gsqlInitiated = False

            logger.error("Connection failed; check your username and password\n {}".format(e))
            logger.info("exit: _initGsql (failure)")

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

        if graphname is None:
            graphname = self.graphname
        if str(graphname).upper() == "GLOBAL" or str(graphname).upper() == "":
            graphname = ""
        if not self.gsqlInitiated:
            if self.certPath:
                self.gsqlInitiated = self._initGsql(self.certPath)
            else:
                self.gsqlInitiated = self._initGsql()
        if self.gsqlInitiated:
            if "\n" not in query:
                res = self.Client.query(query, graph=graphname)
                if isinstance(res, list):
                    ret = "\n".join(res)
                else:
                    ret = res
            else:
                res = self.Client.run_multiple(query.split("\n"))
                if isinstance(res, list):
                    ret = "\n".join(res)
                else:
                    ret = res

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(ret))
            logger.info("exit: gsql (success)")

            string_without_ansi = ANSI_ESCAPE.sub('', ret)

            return string_without_ansi

        else:
            logger.error("Couldn't initialize the client. See previous error.")
            logger.info("exit: gsql (failure)")
            sys.exit(1)

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

        ret = True
        if ExprFunctions:
            if ExprFunctions.startswith("http"):
                # A URL: fetch from online.
                data = requests.get(ExprFunctions).text
            else:
                # A local file: read from disk.
                with open(ExprFunctions) as infile:
                    data = infile.read()
            res = self._req("PUT",
                url="{}/gsqlserver/gsql/userdefinedfunction?filename=ExprFunctions".format(
                    self.gsUrl), authMode="pwd", data=data, resKey="")
            if not res["error"]:
                logger.info("ExprFunctions installed successfully")
            else:
                logger.error("Failed to install ExprFunctions")
                ret = False

        if ExprUtil:
            if ExprUtil.startswith("http"):
                # A URL: fetch from online.
                data = requests.get(ExprUtil).text
            else:
                # A local file: read from disk.
                with open(ExprUtil) as infile:
                    data = infile.read()
            res = self._req("PUT",
                url="{}/gsqlserver/gsql/userdefinedfunction?filename=ExprUtil".format(self.gsUrl),
                    authMode="pwd", data=data, resKey="")
            if not res["error"]:
                logger.info("ExprUtil installed successfully")
                ret = ret & True
            else:
                logger.error("Failed to install ExprUtil")
                ret = False

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: installUDF")
