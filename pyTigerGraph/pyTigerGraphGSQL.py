"""GSQL Interface

Use GSQL within pyTigerGraph.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object]. 
"""
import os
import sys
from typing import Union
from urllib.parse import urlparse

import requests
from pyTigerDriver import GSQL_Client

from pyTigerGraph.pyTigerGraphBase import pyTigerGraphBase
from pyTigerGraph.pyTigerGraphException import TigerGraphException


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
        if not certLocation:
            if not os.path.isdir(os.path.expanduser("~/.gsql")):
                os.mkdir(os.path.expanduser("~/.gsql"))
            certLocation = "~/.gsql/my-cert.txt"

        self.certLocation = os.path.expanduser(certLocation)
        self.url = urlparse(self.gsUrl).netloc  # Getting URL with gsql port w/o https://
        sslhost = self.url.split(":")[0]

        if self.downloadCert:  # HTTP/HTTPS
            import ssl
            try:
                Res = ssl.get_server_certificate((sslhost, self.sslPort))
                # TODO Expected type 'Tuple[str, int]', got 'Tuple[bytes, Any]' instead
            except:  # TODO PEP 8: E722 do not use bare 'except'
                Res = ssl.get_server_certificate((sslhost, 14240))
                # TODO Expected type 'Tuple[str, int]', got 'Tuple[bytes, int]' instead

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

            if self.downloadCert:
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
                    debug=self.debug,
                )
            else:
                self.Client = GSQL_Client(
                    urlparse(self.host).netloc,
                    version=self.version,
                    username=self.username,
                    password=self.password,
                    gsPort=self.gsPort,
                    restpp=self.restppPort,
                    debug=self.debug,
                )
            self.Client.login()
            self.gsqlInitiated = True
            return True
        except Exception as e:
            print("Connection failed. Check your username or password {}".format(e))
            self.gsqlInitiated = False

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
        if graphname is None:
            graphname = self.graphname
        if str(graphname).upper() == "GLOBAL" or str(graphname).upper() == "":
            graphname = ""
        if not self.gsqlInitiated:
            self.gsqlInitiated = self._initGsql()
        if self.gsqlInitiated:
            if "\n" not in query:
                res = self.Client.query(query, graph=graphname)
                if isinstance(res, list):
                    return "\n".join(res)
                else:
                    return res
            else:
                res = self.Client.run_multiple(query.split("\n"))
                if isinstance(res, list):
                    return "\n".join(res)
                else:
                    return res
        else:
            print("Couldn't initialize the client. See above error.")
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
        if ExprFunctions:
            if ExprFunctions.startswith("http"):
                # A url. Fetch from online.
                data = requests.get(ExprFunctions).text
            else:
                # A local file. Read from disk.
                with open(ExprFunctions) as infile:
                    data = infile.read()
            res = self._req(
                "PUT",
                url="{}/gsqlserver/gsql/userdefinedfunction?filename=ExprFunctions".format(self.gsUrl),
                authMode="pwd",
                data=data,
                resKey=None
            )
            if res["error"] == False:
                print("ExprFunctions installed successfully")
            else:
                print("Failed to install ExprFunctions")

        if ExprUtil:
            if ExprUtil.startswith("http"):
                # A url. Fetch from online.
                data = requests.get(ExprUtil).text
            else:
                # A local file. Read from disk.
                with open(ExprUtil) as infile:
                    data = infile.read()
            res = self._req(
                "PUT",
                url="{}/gsqlserver/gsql/userdefinedfunction?filename=ExprUtil".format(self.gsUrl),
                authMode="pwd",
                data=data,
                resKey=None
            )
            if res["error"] == False:
                print("ExprUtil installed successfully")
            else:
                print("Failed to install ExprUtil")
