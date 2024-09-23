"""`TigerGraphConnection`

A TigerGraphConnection object provides the HTTP(S) communication used by all other modules.


"""
import base64
import json
import logging
import sys
import re
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
        self.awsIamHeaders={}

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
            self.restppUrl = self.host + ":"+ sslPort + "/restpp"
        else:
            self.restppPort = restppPort
            self.restppUrl = self.host + ":" + self.restppPort
        gsPort = str(gsPort)
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
            SigV4Auth(boto3.Session().get_credentials(), "sts", "us-east-1").add_auth(request)
            self.awsIamHeaders["X-Amz-Date"] = request.headers["X-Amz-Date"]
            self.awsIamHeaders["X-Amz-Security-Token"] = request.headers["X-Amz-Security-Token"]
            self.awsIamHeaders["Authorization"] = request.headers["Authorization"]

        if self.jwtToken:
            self._verify_jwt_token_support()

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

            # Check JWT support for GSQL server
            if self._versionGreaterThan4_0():
                logger.debug(f"Attempting to get auth info with URL: {self.gsUrl + '/gsql/v1/auth/simple'}")
                self._get(f"{self.gsUrl}/gsql/v1/auth/simple", authMode="token", resKey=None)    
            else:
                logger.debug(f"Attempting to get auth info with URL: {self.gsUrl + '/gsqlserver/gsql/simpleauth'}")
                self._get(f"{self.gsUrl}/gsqlserver/gsql/simpleauth", authMode="token", resKey=None)
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error: {e}.")
            raise RuntimeError(f"Connection error: {e}.") from e
        except Exception as e:
            message = "The JWT token might be invalid or expired or DB version doesn't support JWT token. Please generate new JWT token or switch to API token or username/password."
            logger.error(f"Error occurred: {e}. {message}")
            raise RuntimeError(message) from e

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
        logger.info("entry: _req")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        # If JWT token is provided, always use jwtToken as token
        if authMode == "token":
            if isinstance(self.jwtToken, str) and self.jwtToken.strip() != "":
                token = self.jwtToken
            elif isinstance(self.apiToken, tuple):
                token = self.apiToken[0]
            elif isinstance(self.apiToken, str) and self.apiToken.strip() != "":
                token = self.apiToken
            else:
                token = None

            if token:
                self.authHeader = {'Authorization': "Bearer " + token}
                _headers = self.authHeader
            else:
                self.authHeader = {'Authorization': 'Basic {0}'.format(self.base64_credential)}
                _headers = self.authHeader
                authMode = 'pwd'

        if authMode == "pwd":
            if self.jwtToken:
                _headers = {'Authorization': "Bearer " + self.jwtToken}
            else:
                _headers = {'Authorization': 'Basic {0}'.format(self.base64_credential)}

        if headers:
            _headers.update(headers)
        if self.awsIamHeaders:
            if url.startswith(self.gsUrl + "/gsqlserver/") or (self._versionGreaterThan4_0() and url.startswith(self.gsUrl)): # version >=4.1 has removed /gsqlserver/
                _headers.update(self.awsIamHeaders)
        if self.responseConfigHeader:
            _headers.update(self.responseConfigHeader)
        if method == "POST" or method == "PUT" or method == "DELETE":
            _data = data
        else:
            _data = None

        if self.useCert is True or self.certPath is not None:
            verify = False
        else:
            verify = True

        _headers.update({"X-User-Agent": "pyTigerGraph"})

        if jsonData:
            res = requests.request(method, url, headers=_headers, json=_data, params=params, verify=verify)
        else:
            res = requests.request(method, url, headers=_headers, data=_data, params=params, verify=verify)

        try:
            res.raise_for_status()
        except Exception as e:
            # In TG 4.x the port for restpp has changed from 9000 to 14240. 
            # This block should only be called once. When using 4.x, using port 9000 should fail so self.restppurl will change to host:14240/restpp
            # ----
            # Changes port to gsql port, adds /restpp to end to url, tries again, saves changes if successful
            if "/restpp" not in url or self.tgCloud:
                newRestppUrl = self.host + ":"+self.gsPort+"/restpp"
                # In tgcloud /restpp can already be in the restpp url. We want to extract everything after the port or /restpp
                if self.tgCloud:
                    url = newRestppUrl + '/' + '/'.join(url.split(':')[2].split('/')[2:])
                else:
                    url = newRestppUrl + '/' + '/'.join(url.split(':')[2].split('/')[1:])
                if jsonData:
                    res = requests.request(method, url, headers=_headers, json=_data, params=params, verify=verify)
                else:
                    res = requests.request(method, url, headers=_headers, data=_data, params=params, verify=verify)
                res.raise_for_status()
                self.restppUrl = newRestppUrl
                self.restppPort = self.gsPort
            else:
                raise e

        if jsonResponse:
            try:
                res = json.loads(res.text, strict=strictJson)
            except:
                raise TigerGraphException("Cannot parse json: " + res.text)
        else:
            res = res.text

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
    
    def _put(self, url: str, authMode: str = "token", data = None, resKey=None, jsonData=False) -> Union[dict, list]:
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

        res = self._req("PUT", url, authMode, data=data, resKey=resKey, jsonData=jsonData)

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

        res = self._req("DELETE", url, authMode, data=data, resKey=resKey, jsonData=jsonData)

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
        self.responseConfigHeader = {"GSQL-TIMEOUT": str(timeout), "RESPONSE-LIMIT": str(responseSize)}

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
        response = self._get(self.restppUrl+"/version", strictJson=False, resKey="message")
        if raw:
            return response
        res = response.split("\n")
        components = []
        for i in range(len(res)):
            if 2 < i < len(res) - 1:
                m = res[i].split()
                component = {"name": m[0], "version": m[1], "hash": m[2],
                    "datetime": m[3] + " " + m[4] + " " + m[5]}
                components.append(component)

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

        ret = ""
        for v in self.getVersion():
            if v["name"] == component.lower():
                ret = v["version"]
        if ret != "":
            if full:
                return ret
            ret = re.search("_.+_", ret)
            ret = ret.group().strip("_")

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(ret))
            logger.info("exit: getVer")

            return ret
        else:
            raise TigerGraphException("\"" + component + "\" is not a valid component.", None)
        
    def _versionGreaterThan4_0(self) -> bool:
        """Gets if the TigerGraph database version is greater than 4.0 using gerVer().

        Returns:
            Boolean of whether databse version is greater than 4.0.
        """
        version = self.getVer().split('.')
        if version[0]>="4" and version[1]>"0":
            return True
        return False
