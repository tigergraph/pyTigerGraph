"""`TigerGraphConnection`

A TigerGraphConnection object provides the HTTP(S) communication used by all other modules.


"""
import base64
import json
import logging
import sys
import warnings
from typing import Union
from urllib.parse import urlparse

import requests

from pyTigerGraph.pyTigerGraphException import TigerGraphException


def excepthook(type, value, traceback):
    """NO DOC
    
    This function prints out a given traceback and exception to sys.stderr.

    See: https://docs.python.org/3/library/sys.html#sys.excepthook
    """
    print(value)
    # TODO Proper logging


logger = logging.getLogger(__name__)

class pyTigerGraphBase(object):
    def __init__(self, host: str = "http://127.0.0.1", graphname: str = "MyGraph",
            gsqlSecret: str = "", username: str = "tigergraph", password: str = "tigergraph",
            tgCloud: bool = False, restppPort: Union[int, str] = "9000",
            gsPort: Union[int, str] = "14240", gsqlVersion: str = "", version: str = "",
            apiToken: str = "", useCert: bool = None, certPath: str = None, debug: bool = None,
            sslPort: Union[int, str] = "443", gcp: bool = False):
        """Initiate a connection object.

        Args:
            host:
                The host name or IP address of the TigerGraph server. Make sure to include the
                protocol (http:// or https://). If `certPath` is `None` and the protocol is https,
                a self-signed certificate will be used.
            graphname:
                The default graph for running queries.
            gsqlSecret:
                The secret key for GSQL. See https://docs.tigergraph.com/tigergraph-server/current/user-access/managing-credentials#_secrets.
                Required for GSQL authentication on TigerGraph Cloud instances created after
                July 5, 2022.
            username:
                The username on the TigerGraph server.
            password:
                The password for that user.
            tgCloud:
                Set to `True` if using TigerGraph Cloud. If your hostname contains `tgcloud`, then
                this is automatically set to `True`, and you do not need to set this argument.
            restppPort:
                The port for REST++ queries.
            gsPort:
                The port of all other queries.
            gsqlVersion:
                The version of the GSQL client to be used. Effectively the version of the database
                being connected to.
            version:
                DEPRECATED; use `gsqlVersion`.
            apiToken:
                DEPRECATED; use `getToken()` with a secret to get a session token.
            useCert:
                DEPRECATED; the need for a CA certificate is now determined by URL scheme.
            certPath:
                The filesystem path to the CA certificate. Required in case of https connections.
            debug:
                DEPRECATED; configure standard logging in your app.
            sslPort:
                Port for fetching SSL certificate in case of firewall.
            gcp:
                DEPRECATED. Previously used for connecting to databases provisioned on GCP in TigerGraph Cloud.

        Raises:
            TigerGraphException: In case on invalid URL scheme.

        """
        logger.info("entry: __init__")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        inputHost = urlparse(host)
        if inputHost.scheme not in ["http", "https"]:
            raise TigerGraphException("Invalid URL scheme. Supported schemes are http and https.",
                "E-0003")
        self.netloc = inputHost.netloc
        self.host = "{0}://{1}".format(inputHost.scheme, self.netloc)
        if gsqlSecret != "":
            self.username = "__GSQL__secret"
            self.password = gsqlSecret
        else:
            self.username = username
            self.password = password
        self.graphname = graphname
        self.responseConfigHeader = {}
        # TODO Remove apiToken parameter
        if apiToken:
            warnings.warn(
                "The `apiToken` parameter is deprecated; use `getToken()` function instead.",
                DeprecationWarning)
        self.apiToken = apiToken

        # TODO Eliminate version and use gsqlVersion only, meaning TigerGraph server version
        if gsqlVersion:
            self.version = gsqlVersion
        elif version:
            warnings.warn(
                "The `version` parameter is deprecated; use the `gsqlVersion` parameter instead.",
                DeprecationWarning)
            self.version = version
        else:
            self.version = ""
        self.base64_credential = base64.b64encode(
            "{0}:{1}".format(self.username, self.password).encode("utf-8")).decode("utf-8")
        if self.apiToken:
            self.authHeader = {"Authorization": "Bearer " + self.apiToken}
        else:
            self.authHeader = {"Authorization": "Basic {0}".format(self.base64_credential)}

        if debug is not None:
            warnings.warn(
                "The `debug` parameter is deprecated; configure standard logging in your app.",
                DeprecationWarning)
        if not debug:
            sys.excepthook = excepthook  # TODO Why was this necessary? Can it be removed?
            sys.tracebacklimit = None

        self.schema = None

        # TODO Remove useCert parameter
        if useCert is not None:
            warnings.warn(
                "The `useCert` parameter is deprecated; the need for a CA certificate is now determined by URL scheme.",
                DeprecationWarning)
        if inputHost.scheme == "http":
            self.downloadCert = False
            self.useCert = False
            self.certPath = ""
        elif inputHost.scheme == "https":
            if not certPath:
                self.downloadCert = True
            else:
                self.downloadCert = False
            self.useCert = True
            self.certPath = certPath
        self.sslPort = str(sslPort)

        self.gsqlInitiated = False

        self.Client = None

        # TODO Remove gcp parameter
        if gcp:
            warnings.warn("The `gcp` parameter is deprecated.", DeprecationWarning)
        self.tgCloud = tgCloud or gcp
        if "tgcloud" in self.netloc.lower():
            try:  # If get request succeeds, using TG Cloud instance provisioned after 6/20/2022
                self._get(self.host + "/api/ping", resKey="message")
                self.tgCloud = True
            except requests.exceptions.RequestException:  # If get request fails, using TG Cloud instance provisioned before 6/20/2022, before new firewall config
                self.tgCloud = False
            except TigerGraphException:
                raise (TigerGraphException("Incorrect graphname."))

        restppPort = str(restppPort)
        sslPort = str(sslPort)
        if self.tgCloud and (restppPort == "9000" or restppPort == "443"):
            self.restppPort = sslPort
            self.restppUrl = self.host + ":"+sslPort + "/restpp"
        else:
            self.restppPort = restppPort
            self.restppUrl = self.host + ":" + self.restppPort
        self.gsPort = ""
        gsPort = str(gsPort)
        if self.tgCloud and (gsPort == "14240" or gsPort == "443"):
            self.gsPort = sslPort
            self.gsUrl = self.host + ":" + sslPort
        else:
            self.gsPort = gsPort
            self.gsUrl = self.host + ":" + self.gsPort
        self.url = ""

        logger.info("exit: __init__")

    def _locals(self, _locals: dict) -> str:
        del _locals["self"]
        return str(_locals)

    def _errorCheck(self, res: dict):
        """Checks if the JSON document returned by an endpoint has contains `error: true`. If so,
            it raises an exception.

        Args:
            res:
                The output from a request.

        Raises:
            TigerGraphException: if request returned with error, indicated in the returned JSON.
        """
        if "error" in res and res["error"] and res["error"] != "false":
            # Endpoint might return string "false" rather than Boolean false
            raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def _req(self, method: str, url: str, authMode: str = "token", headers: dict = None,
            data: Union[dict, list, str] = None, resKey: str = "results", skipCheck: bool = False,
            params: Union[dict, list, str] = None, strictJson: bool = True, jsonData: bool = False) -> Union[dict, list]:
        """Generic REST++ API request.

        Args:
            method:
                HTTP method, currently one of `GET`, `POST` or `DELETE`.
            url:
                Complete REST++ API URL including path and parameters.
            authMode:
                Authentication mode, either `"token"` (default) or `"pwd"`.
            headers:
                Standard HTTP request headers.
            data:
                Request payload, typically a JSON document.
            resKey:
                The JSON subdocument to be returned, default is `"result"`.
            skipCheck:
                Some endpoints return an error to indicate that the requested
                action is not applicable. This argument skips error checking.
            params:
                Request URL parameters.
            strictJson:
                If JSON should load the response in strict mode or not.
            jsonData:
                If data in data var is a JSON document.

        Returns:
            The (relevant part of the) response from the request (as a dictionary).
        """
        logger.info("entry: _req")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if authMode == "token" and str(self.apiToken) != "":
            if isinstance(self.apiToken, tuple):
                self.apiToken = self.apiToken[0]
            self.authHeader = {'Authorization': "Bearer " + self.apiToken}
            _headers = self.authHeader
        else:
            self.authHeader = {'Authorization': 'Basic {0}'.format(self.base64_credential)}
            _headers = self.authHeader
            authMode = 'pwd'

        if authMode == "pwd":
            _auth = (self.username, self.password)
        else:
            _auth = None
        if headers:
            _headers.update(headers)
        if self.responseConfigHeader:
            _headers.update(self.responseConfigHeader)
        if method == "POST" or method == "PUT":
            _data = data
        else:
            _data = None

        if self.useCert is True or self.certPath is not None:
            verify = False
        else:
            verify = True

        if jsonData:
            res = requests.request(method, url, headers=_headers, json=_data, params=params, verify=verify)
        else:
            res = requests.request(method, url, headers=_headers, data=_data, params=params, verify=verify)

        if res.status_code != 200:
            try:
                res.raise_for_status()
            except:
                self._errorCheck(res)
        try:
            res = json.loads(res.text, strict=strictJson)
        except:
            raise TigerGraphException(res.text)
        if not skipCheck:
            self._errorCheck(res)
        if not resKey:
            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(res))
            logger.info("exit: _req (no resKey)")

            return res

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res[resKey]))
        logger.info("exit: _req (resKey)")

        return res[resKey]

    def _get(self, url: str, authMode: str = "token", headers: dict = None, resKey: str = "results",
            skipCheck: bool = False, params: Union[dict, list, str] = None, strictJson: bool = True) -> Union[dict, list]:
        """Generic GET method.

        Args:
            url:
                Complete REST++ API URL including path and parameters.
            authMode:
                Authentication mode, either `"token"` (default) or `"pwd"`.
            headers:
                Standard HTTP request headers.
            resKey:
                The JSON subdocument to be returned, default is `"result"`.
            skipCheck:
                Some endpoints return an error to indicate that the requested
                action is not applicable. This argument skips error checking.
            params:
                Request URL parameters.

        Returns:
            The (relevant part of the) response from the request (as a dictionary).
       """
        logger.info("entry: _get")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        res = self._req("GET", url, authMode, headers, None, resKey, skipCheck, params, strictJson)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.info("exit: _get")

        return res

    def _post(self, url: str, authMode: str = "token", headers: dict = None,
            data: Union[dict, list, str, bytes] = None, resKey: str = "results", skipCheck: bool = False,
            params: Union[dict, list, str] = None, jsonData: bool = False) -> Union[dict, list]:
        """Generic POST method.

        Args:
            url:
                Complete REST++ API URL including path and parameters.
            authMode:
                Authentication mode, either `"token"` (default) or `"pwd"`.
            headers:
                Standard HTTP request headers.
            data:
                Request payload, typically a JSON document.
            resKey:
                The JSON subdocument to be returned, default is `"result"`.
            skipCheck:
                Some endpoints return an error to indicate that the requested
                action is not applicable. This argument skips error checking.
            params:
                Request URL parameters.

        Returns:
            The (relevant part of the) response from the request (as a dictionary).
        """
        logger.info("entry: _post")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        res = self._req("POST", url, authMode, headers, data, resKey, skipCheck, params, jsonData=jsonData)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.info("exit: _post")

        return res

    def _delete(self, url: str, authMode: str = "token") -> Union[dict, list]:
        """Generic DELETE method.

        Args:
            url:
                Complete REST++ API URL including path and parameters.
            authMode:
                Authentication mode, either `"token"` (default) or `"pwd"`.

        Returns:
            The response from the request (as a dictionary).
        """
        logger.info("entry: _delete")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        res = self._req("DELETE", url, authMode)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.info("exit: _delete")

        return res

    def customizeHeader(self, timeout:int = 16_000, responseSize:int = 3.2e+7):
        """Method to configure the request header.

        Args:
            tiemout (int, optional):
                The timeout value desired in milliseconds. Defaults to 16,000 ms (16 sec)
            responseSize:
                The size of the response in bytes. Defaults to 3.2E7 bytes (32 MB).

        Returns:
            Nothing. Sets `responseConfigHeader` class attribute.
        """
        self.responseConfigHeader = {"GSQL-TIMEOUT": timeout, "RESPONSE-LIMIT": responseSize}