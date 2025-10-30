"""`AsyncTigerGraphConnection`

A TigerGraphConnection object provides the HTTP(S) communication used by all other modules.
This object is the **asynchronous** version of the connection object. If you want to use pyTigerGraph in an synchronous
environment, use the `TigerGraphConnection` object.

The `AsyncTigerGraphConnection` object is the main object that you will interact with when using pyTigerGraph.
It provides the same core functionality as the synchronous `TigerGraphConnection` object, but with asynchronous methods.

**Note:** `AsyncTigerGraphConnection` does not currently support the GDS or TigerGraph GraphRAG APIs found in the synchronous version.

To test your connection, you can use the `echo()` method. This method sends a simple request to the server and returns the response.

```python
from pyTigerGraph import TigerGraphConnection

conn = AsyncTigerGraphConnection(
    host="http://localhost",
    graphname="",
    username="tigergraph",
    password="tigergraph")

resp = await conn.echo()

print(resp)
```
"""

import json
import logging
import httpx

from typing import Union
from urllib.parse import urlparse

from pyTigerGraph.common.base import PyTigerGraphCore

logger = logging.getLogger(__name__)


class AsyncPyTigerGraphBase(PyTigerGraphCore):
    def __init__(self, host: str = "http://127.0.0.1", graphname: str = "",
                 gsqlSecret: str = "", username: str = "tigergraph", password: str = "tigergraph",
                 tgCloud: bool = False, restppPort: Union[int, str] = "9000",
                 gsPort: Union[int, str] = "14240", gsqlVersion: str = "", version: str = "",
                 apiToken: str = "", useCert: bool = None, certPath: str = None, debug: bool = None,
                 sslPort: Union[int, str] = "443", gcp: bool = False, jwtToken: str = ""):
        """Initiate a connection object (doc string copied from synchronous __init__).

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
            jwtToken:
                The JWT token generated from customer side for authentication

        Raises:
            TigerGraphException: In case on invalid URL scheme.

        """

        super().__init__(host=host, graphname=graphname, gsqlSecret=gsqlSecret,
                         username=username, password=password, tgCloud=tgCloud,
                         restppPort=restppPort, gsPort=gsPort, gsqlVersion=gsqlVersion,
                         version=version, apiToken=apiToken, useCert=useCert, certPath=certPath,
                         debug=debug, sslPort=sslPort, gcp=gcp, jwtToken=jwtToken)

    async def _req(self, method: str, url: str, authMode: str = "token", headers: dict = None,
                   data: Union[dict, list, str] = None, resKey: str = "results", skipCheck: bool = False,
                   params: Union[dict, list, str] = None, strictJson: bool = True, jsonData: bool = False,
                   jsonResponse: bool = True, func=None) -> Union[dict, list]:
        """Generic REST++ API request. Copied from synchronous version, changing requests to httpx with async functionality.

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
        _headers, _data, verify = self._prep_req(authMode, headers, url, method, data)

        if "GSQL-TIMEOUT" in _headers:
            http_timeout = (30, int(int(_headers["GSQL-TIMEOUT"])/1000) + 30)
        else:
            http_timeout = (30, None)

        async with httpx.AsyncClient(timeout=None) as client:
            if jsonData:
                res = await client.request(method, url, headers=_headers, json=_data, params=params, timeout=http_timeout)
            else:
                res = await client.request(method, url, headers=_headers, data=_data, params=params, timeout=http_timeout)

        try:
            if not skipCheck and not (200 <= res.status_code < 300) and res.status_code != 404:
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
            # Changes port to 14240, adds /restpp to end to url, tries again, saves changes if successful
            if self.restppPort == "9000" and "9000" in url:
                newRestppUrl = self.host + ":14240/restpp"
                # In tgcloud /restpp can already be in the restpp url. We want to extract everything after the port or /restpp
                if '/restpp' in url:
                    url = newRestppUrl + '/' + \
                        '/'.join(url.split(':')[2].split('/')[2:])
                else:
                    url = newRestppUrl + '/' + \
                        '/'.join(url.split(':')[2].split('/')[1:])
                async with httpx.AsyncClient(timeout=None) as client:
                    if jsonData:
                        res = await client.request(method, url, headers=_headers, json=_data, params=params)
                    else:
                        res = await client.request(method, url, headers=_headers, data=_data, params=params)
                if not skipCheck and not (200 <= res.status_code < 300) and res.status_code != 404:
                    try:
                        self._error_check(json.loads(res.text))
                    except json.decoder.JSONDecodeError:
                        # could not parse the res text (probably returned an html response)
                        pass
                res.raise_for_status()
                self.restppUrl = newRestppUrl
                self.restppPort = "14240"
            else:
                raise e

        return self._parse_req(res, jsonResponse, strictJson, skipCheck, resKey)

    async def _get(self, url: str, authMode: str = "token", headers: dict = None, resKey: str = "results",
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
        logger.debug("entry: _get")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        res = await self._req("GET", url, authMode, headers, None, resKey, skipCheck, params, strictJson)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: _get")

        return res

    async def _post(self, url: str, authMode: str = "token", headers: dict = None,
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
        logger.debug("entry: _post")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        res = await self._req("POST", url, authMode, headers, data, resKey, skipCheck, params, jsonData=jsonData)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: _post")

        return res

    async def _put(self, url: str, authMode: str = "token", data=None, resKey=None, jsonData=False) -> Union[dict, list]:
        """Generic PUT method.

        Args:
            url:
                Complete REST++ API URL including path and parameters.
            authMode:
                Authentication mode, either `"token"` (default) or `"pwd"`.

        Returns:
            The response from the request (as a dictionary).
        """
        logger.debug("entry: _put")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        res = await self._req("PUT", url, authMode, data=data, resKey=resKey, jsonData=jsonData)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: _put")

        return res

    async def _delete(self, url: str, authMode: str = "token", data: dict = None, resKey="results", jsonData=False) -> Union[dict, list]:
        """Generic DELETE method.

        Args:
            url:
                Complete REST++ API URL including path and parameters.
            authMode:
                Authentication mode, either `"token"` (default) or `"pwd"`.

        Returns:
            The response from the request (as a dictionary).
        """
        logger.debug("entry: _delete")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        res = await self._req("DELETE", url, authMode, data=data, resKey=resKey, jsonData=jsonData)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: _delete")

        return res

    async def getVersion(self, raw: bool = False) -> Union[str, list]:
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
        logger.debug("entry: getVersion")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))
        response = await self._get(self.restppUrl+"/version", strictJson=False, resKey="message")
        components = self._parse_get_version(response, raw)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(components))
        logger.debug("exit: getVersion")
        return components

    async def getVer(self, component: str = "product", full: bool = False) -> str:
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
        logger.debug("entry: getVer")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))
        version = await self.getVersion()
        ret = self._parse_get_ver(version, component, full)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getVer")

        return ret
    
    async def customizeHeader(self, timeout:int = 16_000, responseSize:int = 3.2e+7):
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

    async def _version_greater_than_4_0(self) -> bool:
        """Gets if the TigerGraph database version is greater than 4.0 using gerVer().

        Returns:
            Boolean of whether databse version is greater than 4.0.

        Note:
            The result is cached to avoid repeated server calls. The cache is cleared
            when the connection object is recreated.
        """
        # Use cached value if available
        if hasattr(self, '_cached_version_greater_than_4_0'):
            return self._cached_version_greater_than_4_0

        # Cache not set, fetch version and cache the result
        try:
            version = self.getVer().split('.')
        except TigerGraphException as e:
            if e.code == "REST-10016":
                self.getToken()
                version = self.getVer().split('.')
            else:
                raise e
        version = version.split('.')
        result = version[0] >= "4" and version[1] > "0"
        self._cached_version_greater_than_4_0 = result
        return result
