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

import asyncio
import json
import logging
import aiohttp

from typing import Optional, Union
from urllib.parse import urlparse

from pyTigerGraph.common.base import PyTigerGraphCore
from pyTigerGraph.common.exception import TigerGraphException

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

        # Lazily initialized on first request (inside an async context) to avoid
        # creating aiohttp.ClientSession outside an event loop in __init__.
        self._async_client: Optional[aiohttp.ClientSession] = None

        # asyncio.Lock for the one-time port failover (TG 3.x port 9000 → 4.x port 14240).
        # Without a lock all concurrent tasks simultaneously fail and all enter the failover
        # block, doubling requests and racing to overwrite self.restppUrl/self.restppPort.
        self._restpp_failover_lock = asyncio.Lock()
        self._token_refresh_lock = asyncio.Lock()

    async def _req(self, method: str, url: str, authMode: str = "token", headers: dict = None,
                   data: Union[dict, list, str] = None, resKey: str = "results", skipCheck: bool = False,
                   params: Union[dict, list, str] = None, strictJson: bool = True, jsonData: bool = False,
                   jsonResponse: bool = True, func=None) -> Union[dict, list]:
        """Generic REST++ API request. Copied from synchronous version, using aiohttp for direct asyncio integration.

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
        # Lazy init: session must be created inside an async context (event loop running).
        if self._async_client is None or self._async_client.closed:
            self._async_client = self._make_async_client()

        _headers, _data, _ = self._prep_req(authMode, headers, url, method, data)

        if "GSQL-TIMEOUT" in _headers:
            http_timeout = aiohttp.ClientTimeout(
                sock_connect=30,
                total=int(int(_headers["GSQL-TIMEOUT"]) / 1000) + 30,
            )
        else:
            http_timeout = aiohttp.ClientTimeout(sock_connect=30, total=None)

        conn_err = None
        try:
            status, body, resp = await self._do_request(
                method, url, _headers, _data, jsonData, params, http_timeout)
        except (aiohttp.ClientConnectorError, aiohttp.ClientConnectionError, OSError) as e:
            status, body, resp = None, None, None
            conn_err = e

        if resp is not None:
            # Auto-refresh token on 401 if the token was generated by getToken()
            if status == 401 and getattr(self, "_token_source", None) == "generated":
                async with self._token_refresh_lock:
                    if not getattr(self, "_refreshing_token", False):
                        try:
                            self._refreshing_token = True
                            await self.getToken()
                            self._refresh_auth_headers()
                        finally:
                            self._refreshing_token = False
                    _headers, _data, _ = self._prep_req(authMode, headers, url, method, data)
                    status, body, resp = await self._do_request(
                        method, url, _headers, _data, jsonData, params, http_timeout)

            if not skipCheck and not (200 <= status < 300) and status != 404:
                try:
                    self._error_check(json.loads(body))
                except json.decoder.JSONDecodeError:
                    pass
            try:
                resp.raise_for_status()
            except (aiohttp.ClientConnectorError, aiohttp.ClientConnectionError, OSError) as e:
                resp = None
                conn_err = e
            # HTTP errors (4xx/5xx) propagate immediately — no failover

        if resp is None:
            # In TG 4.x the port for restpp has changed from 9000 to 14240.
            # This block should only be called once. When using 4.x, using port 9000 should fail so self.restppurl will change to host:14240/restpp
            # ----
            # Changes port to gsql port, adds /restpp to end to url, tries again, saves changes if successful
            if self.restppPort in url and "/gsql" not in url and ("/restpp" not in url or self.tgCloud):
                async with self._restpp_failover_lock:
                    if self.restppPort in url:
                        newRestppUrl = self.host + ":" + self.gsPort + "/restpp"
                        if "/restpp" in url:
                            url = newRestppUrl + "/" + "/".join(url.split(":")[2].split("/")[2:])
                        else:
                            url = newRestppUrl + "/" + "/".join(url.split(":")[2].split("/")[1:])
                        status, body, resp = await self._do_request(
                            method, url, _headers, _data, jsonData, params, None)
                        if not skipCheck and not (200 <= status < 300) and status != 404:
                            try:
                                self._error_check(json.loads(body))
                            except json.decoder.JSONDecodeError:
                                pass
                        resp.raise_for_status()
                        self.restppUrl = newRestppUrl
                        self.restppPort = self.gsPort
                    else:
                        url = url.replace(
                            self.host + ":" + self.gsPort,
                            self.restppUrl, 1)
                        status, body, resp = await self._do_request(
                            method, url, _headers, _data, jsonData, params, None)
                        if not skipCheck and not (200 <= status < 300) and status != 404:
                            try:
                                self._error_check(json.loads(body))
                            except json.decoder.JSONDecodeError:
                                pass
                        resp.raise_for_status()
            else:
                if conn_err is not None:
                    raise conn_err
                raise aiohttp.ClientConnectionError(
                    f"Failed to connect to {url}")

        return self._parse_req(body, jsonResponse, strictJson, skipCheck, resKey)

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

    async def _put(self, url: str, authMode: str = "token", data=None, resKey=None,
                   jsonData=False, params: Union[dict, list, str] = None) -> Union[dict, list]:
        """Generic PUT method.

        Args:
            url:
                Complete REST++ API URL including path and parameters.
            authMode:
                Authentication mode, either `"token"` (default) or `"pwd"`.
            data:
                Request payload, typically a JSON document.
            resKey:
                The JSON subdocument to be returned, default is `None`.
            params:
                Request URL parameters.

        Returns:
            The response from the request (as a dictionary).
        """
        logger.debug("entry: _put")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        res = await self._req("PUT", url, authMode, data=data, resKey=resKey, jsonData=jsonData, params=params)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: _put")

        return res

    async def _delete(self, url: str, authMode: str = "token", headers: dict = None,
                      data: dict = None, resKey="results", skipCheck: bool = False,
                      params: Union[dict, list, str] = None, jsonData=False) -> Union[dict, list]:
        """Generic DELETE method.

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
                The JSON subdocument to be returned, default is `"results"`.
            skipCheck:
                Some endpoints return an error to indicate that the requested
                action is not applicable. This argument skips error checking.
            params:
                Request URL parameters.

        Returns:
            The response from the request (as a dictionary).
        """
        logger.debug("entry: _delete")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        res = await self._req("DELETE", url, authMode, headers, data,
                              resKey, skipCheck, params, jsonData=jsonData)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: _delete")

        return res

    def _make_async_client(self) -> aiohttp.ClientSession:
        """Create a persistent aiohttp.ClientSession.

        aiohttp integrates directly with asyncio (no anyio abstraction layer),
        giving lower per-request overhead than httpx for high-concurrency workloads.
        The connection pool grows to match demand automatically (limit=0).
        SSL verify is taken from self.verify, computed once at __init__ time.

        Must be called from within an async context (event loop running) to avoid
        aiohttp deprecation warnings about session creation outside a coroutine.
        """
        connector = aiohttp.TCPConnector(
            limit=0,                            # unbounded pool, grows with demand
            ssl=None if self.verify else False,  # None = default SSL context (verify on)
        )
        return aiohttp.ClientSession(connector=connector)

    async def _do_request(
        self,
        method: str,
        url: str,
        _headers: dict,
        _data,
        jsonData: bool,
        params,
        timeout: Optional[aiohttp.ClientTimeout],
    ):
        """Execute one HTTP request and return (status_code, response_text, response).

        Wraps aiohttp's per-request context manager so _req can treat the response
        as a plain (status, text, resp) triple. The response object remains usable
        for raise_for_status() after the context manager exits because aiohttp caches
        status, headers, and request_info on the response object at header-receive time.
        """
        kwargs = {"headers": _headers, "params": params}
        if timeout is not None:
            kwargs["timeout"] = timeout
        if jsonData:
            kwargs["json"] = _data
        else:
            kwargs["data"] = _data
        async with self._async_client.request(method, url, **kwargs) as resp:
            # read() returns raw bytes — avoids charset detection overhead and lets
            # orjson/json.loads consume bytes directly without a decode step.
            body = await resp.read()
        return resp.status, body, resp

    async def aclose(self) -> None:
        """Close the underlying HTTP connection pool.

        Call this when done with the connection to release open sockets.
        Alternatively, use the connection as an async context manager:

        ```python
        async with AsyncTigerGraphConnection(...) as conn:
            await conn.runInstalledQuery(...)
        ```
        """
        if self._async_client is not None and not self._async_client.closed:
            await self._async_client.close()
        self._async_client = None

    def __del__(self) -> None:
        """Best-effort cleanup when the object is garbage-collected.

        If the event loop is still running at GC time (e.g. during asyncio.run()
        shutdown), schedules aclose() as a task so sockets are drained gracefully.
        If the loop has already stopped, the OS reclaims the sockets and there is
        nothing more we can do — this is not an error.

        This does NOT replace explicit aclose() / async-with usage: GC timing is
        unpredictable and create_task() is fire-and-forget with no error handling.
        Use `async with AsyncTigerGraphConnection(...) as conn:` for reliable cleanup.
        """
        if self._async_client is not None and not self._async_client.closed:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._async_client.close())
            except RuntimeError:
                pass  # no running loop; OS reclaims sockets on process exit

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()

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

    async def customizeHeader(self, timeout: int = 16_000, responseSize: int = 3.2e+7,
                              threadLimit: int = None, memoryLimit: int = None):
        """Method to configure the request header.

        Args:
            timeout (int, optional):
                The timeout value desired in milliseconds. Defaults to 16,000 ms (16 sec).
            responseSize:
                The size of the response in bytes. Defaults to 3.2E7 bytes (32 MB).
            threadLimit (int, optional):
                Maximum number of threads to use per query. If not set, the server default is used.
                Ignored by TigerGraph versions that do not support this header.
            memoryLimit (int, optional):
                Maximum memory per query in MB. If not set, the server default is used.
                Ignored by TigerGraph versions that do not support this header.

        Returns:
            Nothing. Sets `responseConfigHeader` class attribute.
        """
        self.responseConfigHeader = {"GSQL-TIMEOUT": str(timeout), "RESPONSE-LIMIT": str(responseSize)}
        if threadLimit:
            self.responseConfigHeader["GSQL-THREAD-LIMIT"] = str(threadLimit)
        if memoryLimit:
            self.responseConfigHeader["GSQL-QueryLocalMemLimitMB"] = str(memoryLimit)

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
            version = (await self.getVer()).split('.')
        except TigerGraphException as e:
            if e.code == "REST-10016":
                await self.getToken()
                version = (await self.getVer()).split('.')
            else:
                raise e
        result = version[0] >= "4" and version[1] > "0"
        self._cached_version_greater_than_4_0 = result
        return result
