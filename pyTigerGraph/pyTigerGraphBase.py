"""`TigerGraphConnection`

A TigerGraphConnection object provides the HTTP(S) communication used by all other modules.
This object is the **synchronous** version of the connection object.
If you want to use pyTigerGraph in an asynchronous environment, use the `AsyncTigerGraphConnection` object.

The `TigerGraphConnection` object is the main object that you will interact with when using pyTigerGraph.

To test your connection, you can use the `echo()` method. This method sends a simple request to the server and returns the response.

```python
from pyTigerGraph import TigerGraphConnection

conn = TigerGraphConnection(
    host="http://localhost",
    graphname="MyGraph",
    username="tigergraph",
    password="tigergraph")

print(conn.echo())
```
"""
import base64
import json
import logging
import sys
import re
import warnings
import requests

from typing import Union
from urllib.parse import urlparse

from pyTigerGraph.common.exception import TigerGraphException
from pyTigerGraph.common.base import PyTigerGraphCore


def excepthook(type, value, traceback):
    """NO DOC

    This function prints out a given traceback and exception to sys.stderr.

    See: https://docs.python.org/3/library/sys.html#sys.excepthook
    """
    print(value)
    # TODO Proper logging


logger = logging.getLogger(__name__)


class pyTigerGraphBase(PyTigerGraphCore, object):
    def __init__(self, host: str = "http://127.0.0.1", graphname: str = "MyGraph",
                 gsqlSecret: str = "", username: str = "tigergraph", password: str = "tigergraph",
                 tgCloud: bool = False, restppPort: Union[int, str] = "9000",
                 gsPort: Union[int, str] = "14240", gsqlVersion: str = "", version: str = "",
                 apiToken: str = "", useCert: bool = None, certPath: str = None, debug: bool = None,
                 sslPort: Union[int, str] = "443", gcp: bool = False, jwtToken: str = ""):
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
                The port for gsql server.
            gsqlVersion:
                The version of the GSQL client to be used. Effectively the version of the database
                being connected to.
            apiToken (Optional):
                Paremeter for specifying a RESTPP service token. Use `getToken()` to get a token.
            version:
                DEPRECATED; use `gsqlVersion`.
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
            jwtToken:
                The JWT token generated from customer side for authentication

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
        self.awsIamHeaders = {}

        self.jwtToken = jwtToken
        self.apiToken = apiToken
        self.base64_credential = base64.b64encode(
            "{0}:{1}".format(self.username, self.password).encode("utf-8")).decode("utf-8")

        self.authHeader = self._set_auth_header()

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

        # TODO Remove gcp parameter
        if gcp:
            warnings.warn("The `gcp` parameter is deprecated.",
                          DeprecationWarning)
        self.tgCloud = tgCloud or gcp
        if "tgcloud" in self.netloc.lower():
            try:  # If get request succeeds, using TG Cloud instance provisioned after 6/20/2022
                self._get(self.host + "/api/ping", resKey="message")
                self.tgCloud = True
            # If get request fails, using TG Cloud instance provisioned before 6/20/2022, before new firewall config
            except requests.exceptions.RequestException:
                self.tgCloud = False
            except TigerGraphException:
                raise (TigerGraphException("Incorrect graphname."))

        restppPort = str(restppPort)
        gsPort = str(gsPort)
        sslPort = str(sslPort)
        if restppPort == gsPort:
            self.restppPort = restppPort
            self.restppUrl = self.host + ":" + restppPort + "/restpp"
        elif (self.tgCloud and (restppPort == "9000" or restppPort == "443")):
            if restppPort == gsPort:
                sslPort = gsPort
            self.restppPort = sslPort
            self.restppUrl = self.host + ":" + sslPort + "/restpp"
        else:
            self.restppPort = restppPort
            self.restppUrl = self.host + ":" + self.restppPort
        
        self.gsPort = gsPort
        if self.tgCloud and (gsPort == "14240" or gsPort == "443"):
            self.gsPort = sslPort
            self.gsUrl = self.host + ":" + sslPort
        else:
            self.gsPort = gsPort
            self.gsUrl = self.host + ":" + self.gsPort
        self.url = ""

        if self.username.startswith("arn:aws:iam::"):
            import boto3
            from botocore.awsrequest import AWSRequest
            from botocore.auth import SigV4Auth
            # Prepare a GetCallerIdentity request.
            request = AWSRequest(
                method="POST",
                url="https://sts.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15",
                headers={
                    'Host': 'sts.amazonaws.com'
                })
            # Get headers
            SigV4Auth(boto3.Session().get_credentials(),
                      "sts", "us-east-1").add_auth(request)
            self.awsIamHeaders["X-Amz-Date"] = request.headers["X-Amz-Date"]
            self.awsIamHeaders["X-Amz-Security-Token"] = request.headers["X-Amz-Security-Token"]
            self.awsIamHeaders["Authorization"] = request.headers["Authorization"]

        if self.jwtToken:
            self._verify_jwt_token_support()

        self.asynchronous = False

        logger.info("exit: __init__")

    def _set_auth_header(self):
        """Set the authentication header based on available tokens or credentials."""
        if self.jwtToken:
            return {"Authorization": "Bearer " + self.jwtToken}
        elif self.apiToken:
            return {"Authorization": "Bearer " + self.apiToken}
        else:
            return {"Authorization": "Basic {0}".format(self.base64_credential)}

    def _verify_jwt_token_support(self):
        try:
            # Check JWT support for RestPP server
            logger.debug("Attempting to verify JWT token support with getVer() on RestPP server.")
            logger.debug(f"Using auth header: {self.authHeader}") 
            version = self.getVer()
            logger.info(f"Database version: {version}")
            '''
            # Check JWT support for GSQL server
            if self._versionGreaterThan4_0():
                logger.debug(f"Attempting to get auth info with URL: {self.gsUrl + '/gsql/v1/tokens/check'}")
                res = self._post(f"{self.gsUrl}/gsql/v1/tokens/check", authMode="token", resKey=None, data={"token": self.jwtToken}, jsonData=True)
                if "error" in res and res["error"]:
                    raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))  
            else:
                logger.debug(f"Attempting to get auth info with URL: {self.gsUrl + '/gsqlserver/gsql/simpleauth'}")
                self._get(f"{self.gsUrl}/gsqlserver/gsql/simpleauth", authMode="token", resKey=None)
            '''
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error: {e}.")
            raise TigerGraphException("Connection error: "+str(e))
        except Exception as e:
            message = "The JWT token might be invalid or expired or DB version doesn't support JWT token. Please generate new JWT token or switch to API token or username/password. Error: "+str(e)
            logger.error(f"Error occurred: {e}. {message}")
            raise TigerGraphException(message) 

    def _locals(self, _locals: dict) -> str:
        del _locals["self"]
        return str(_locals)

        logger.info("exit: __init__")

    def _req(self, method: str, url: str, authMode: str = "token", headers: dict = None,
             data: Union[dict, list, str] = None, resKey: str = "results", skipCheck: bool = False,
             params: Union[dict, list, str] = None, strictJson: bool = True, jsonData: bool = False,
             jsonResponse: bool = True) -> Union[dict, list]:
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
        _headers, _data, verify = self._prep_req(
            authMode, headers, url, method, data)

        if "GSQL-TIMEOUT" in _headers:
            http_timeout = (10, int(int(_headers["GSQL-TIMEOUT"])/1000) + 10)
        else:
            http_timeout = None

        if jsonData:
            res = requests.request(
                method, url, headers=_headers, json=_data, params=params, verify=verify, timeout=http_timeout)
        else:
            res = requests.request(
                method, url, headers=_headers, data=_data, params=params, verify=verify, timeout=http_timeout)

        try:
            if not skipCheck and not (200 <= res.status_code < 300):
                try:
                    self._error_check(json.loads(res.text))
                except json.decoder.JSONDecodeError:
                    # could not parse the res text (probably returned an html response)
                    pass
            res.raise_for_status()
        except Exception as e:

            # In TG 4.x the port for restpp has changed from 9000 to 14240.
            # This block should only be called once. When using 4.x, using port 9000 should fail so self.restppurl will change to host:14240/restpp
            # ----
            # Changes port to gsql port, adds /restpp to end to url, tries again, saves changes if successful
            if self.restppPort in url and "/gsql" not in url and ("/restpp" not in url or self.tgCloud):
                newRestppUrl = self.host + ":"+self.gsPort+"/restpp"
                # In tgcloud /restpp can already be in the restpp url. We want to extract everything after the port or /restpp
                if self.tgCloud:
                    url = newRestppUrl + '/' + '/'.join(url.split(':')[2].split('/')[2:])
                else:
                    url = newRestppUrl + '/' + \
                        '/'.join(url.split(':')[2].split('/')[1:])
                if jsonData:
                    res = requests.request(
                        method, url, headers=_headers, json=_data, params=params, verify=verify)
                else:
                    res = requests.request(
                        method, url, headers=_headers, data=_data, params=params, verify=verify)

                # Run error check if there might be an error before raising for status
                # raising for status gives less descriptive error message
                if not skipCheck and not (200 <= res.status_code < 300) and res.status_code != 404:
                    try:
                        self._error_check(json.loads(res.text))
                    except json.decoder.JSONDecodeError:
                        # could not parse the res text (probably returned an html response)
                        pass
                res.raise_for_status()
                self.restppUrl = newRestppUrl
                self.restppPort = self.gsPort
            else:
                raise e

        return self._parse_req(res, jsonResponse, strictJson, skipCheck, resKey)

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

        res = self._req("GET", url, authMode, headers, None,
                        resKey, skipCheck, params, strictJson)

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

        res = self._req("POST", url, authMode, headers, data,
                        resKey, skipCheck, params, jsonData=jsonData)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.info("exit: _post")

        return res

    def _put(self, url: str, authMode: str = "token", data=None, resKey=None, jsonData=False) -> Union[dict, list]:
        """Generic PUT method.

        Args:
            url:
                Complete REST++ API URL including path and parameters.
            authMode:
                Authentication mode, either `"token"` (default) or `"pwd"`.

        Returns:
            The response from the request (as a dictionary).
        """
        logger.info("entry: _put")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        res = self._req("PUT", url, authMode, data=data,
                        resKey=resKey, jsonData=jsonData)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.info("exit: _put")

        return res

    def _delete(self, url: str, authMode: str = "token", data: dict = None, resKey="results", jsonData=False) -> Union[dict, list]:
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

        res = self._req("DELETE", url, authMode, data=data,
                        resKey=resKey, jsonData=jsonData)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.info("exit: _delete")

        return res
    
    def getVersion(self, raw: bool = False) -> Union[str, list]:
        """Retrieves the git versions of all components of the system.

        Args:
            raw:
                Return unprocessed version info string, or extract version info for each component
                into a list.

        Returns:
            Either an unprocessed string containing the version info details, or a list with version
            info for each component.

        Endpoint:
            - `GET /version`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_show_component_versions[Show component versions]
        """
        logger.info("entry: getVersion")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))
        response = self._get(self.restppUrl+"/version",
                             strictJson=False, resKey="message")
        components = self._parse_get_version(response, raw)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(components))
        logger.info("exit: getVersion")
        return components

    def getVer(self, component: str = "product", full: bool = False) -> str:
        """Gets the version information of a specific component.

        Get the full list of components using `getVersion()`.

        Args:
            component:
                One of TigerGraph's components (e.g. product, gpe, gse).
            full:
                Return the full version string (with timestamp, etc.) or just X.Y.Z.

        Returns:
            Version info for specified component.

        Raises:
            `TigerGraphException` if invalid/non-existent component is specified.
        """
        logger.info("entry: getVer")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))
        version = self.getVersion()
        ret = self._parse_get_ver(version, component, full)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getVer")

        return ret
    
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
        self.responseConfigHeader = {"GSQL-TIMEOUT": str(timeout), "RESPONSE-LIMIT": str(responseSize)}

    def _version_greater_than_4_0(self) -> bool:
        """Gets if the TigerGraph database version is greater than 4.0 using gerVer().

        Returns:
            Boolean of whether databse version is greater than 4.0.
        """
        version = self.getVer().split('.')
        if version[0] >= "4" and version[1] > "0":
            return True
        return False
