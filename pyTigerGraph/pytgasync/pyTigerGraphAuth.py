"""Authentication Functions

The functions on this page authenticate connections and manage TigerGraph credentials.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""

import logging
from typing import Union, Dict
import warnings
import httpx

from pyTigerGraph.common.exception import TigerGraphException
from pyTigerGraph.common.auth import (
    _parse_get_secrets,
    _parse_create_secret,
    _prep_token_request,
    _parse_token_response
)
from pyTigerGraph.pytgasync.pyTigerGraphGSQL import AsyncPyTigerGraphGSQL

logger = logging.getLogger(__name__)


class AsyncPyTigerGraphAuth(AsyncPyTigerGraphGSQL):

    async def getSecrets(self, userName: str = "", alias: str = "", createNew: bool = False) -> Dict[str, str]:
        """Lists all secrets for the current user.

        For TigerGraph versions >= 4.0, uses the REST API endpoint.
        For older versions, uses the GSQL command.

        Args:
            userName (str, optional):
                The user name for whom to list secrets. If not provided (TigerGraph 4.x only),
                lists secrets for the default logged-in user. This parameter is only
                supported in TigerGraph >= 4.0.
            alias (str, optional):
                The alias of a specific secret to retrieve. If not provided, all secrets
                are returned. This parameter is only supported in TigerGraph >= 4.0.
            createNew (bool, optional):
                If True and no secrets are found (or the specified alias is not found),
                automatically create a new secret. Defaults to False.
                When creating a new secret:
                - If `alias` is provided, creates a secret with that alias.
                - If `alias` is not provided, creates a secret with auto-generated alias.
                This parameter is only supported in TigerGraph >= 4.0.

        Returns:
            A dictionary of `alias: secret_string` pairs.

        Notes:
            In TigerGraph versions < 4.0, this function returns the masked version of the secret.
            The original value of the secret cannot be retrieved after creation.
            In TigerGraph versions >= 4.0, the REST API returns secrets in plain text.
            If `createNew` is True in TigerGraph < 4.0, this parameter is ignored.

        Endpoints:
            - `GET /gsql/v1/secrets?userName=<user>` (In TigerGraph versions >= 4.0)
            - `GET /gsql/v1/secrets/<alias>` (In TigerGraph versions >= 4.0, when alias is provided)
            - GSQL: `SHOW SECRET` (In TigerGraph versions < 4.0)
        """
        logger.debug("entry: getSecrets")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        # Try to use TigerGraph 4.x REST API first, if failed then use GSQL command
        try:
            if alias:
                # Get specific secret by alias
                try:
                    res = await self._req("GET", self.gsUrl+"/gsql/v1/secrets/"+alias,
                                         authMode="pwd", headers={'Content-Type': 'application/json'})
                except TigerGraphException as e:
                    # Secret not found
                    res = ""

                # Response format: {"alias": "...", "value": "..."}
                ret = {}

                if res and isinstance(res, dict) and res.get("alias"):
                    # Secret found
                    ret[res.get("alias", "")] = res.get("value", "")
                elif createNew:
                    # Secret not found and createNew is True
                    created_secret = await self.createSecret(alias=alias, userName=userName, withAlias=True)
                    if isinstance(created_secret, dict):
                        ret.update(created_secret)
                    else:
                        # This shouldn't happen with withAlias=True, but handle it
                        ret[alias] = created_secret
            else:
                # Get all secrets
                params = {}
                if userName:
                    params["userName"] = userName
                res = await self._req("GET", self.gsUrl+"/gsql/v1/secrets",
                                     params=params, authMode="pwd",
                                     headers={'Content-Type': 'application/json'})

                # Response format: [{"alias": "...", "value": "..."}, ...]
                ret = {}

                # Handle list response
                if isinstance(res, list):
                    for item in res:
                        ret[item.get("alias", "")] = item.get("value", "")

                # If no secrets found and createNew is True, create a new one
                if not ret and createNew:
                    created_secret = await self.createSecret(userName=userName, withAlias=True)
                    if isinstance(created_secret, dict):
                        ret.update(created_secret)
                    else:
                        # This shouldn't happen with withAlias=True, but handle it
                        ret["AUTO_GENERATED"] = created_secret

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(ret))
            logger.debug("exit: getSecrets")

            return ret
        except Exception as e:
            print(e)
            # For older versions, use GSQL command
            res = await self.gsql("""
                USE GRAPH {}
                SHOW SECRET""".format(self.graphname), )
            ret = _parse_get_secrets(res)

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(ret))
            logger.debug("exit: getSecrets")

            return ret

    async def showSecrets(self) -> Dict[str, str]:
        """DEPRECATED

        Use `getSecrets()` instead.
        """
        warnings.warn("The `showSecrets()` function is deprecated; use `getSecrets()` instead.",
                      DeprecationWarning)

        ret = await self.getSecrets()
        return ret

    async def createSecret(self, alias: str = "", withAlias: bool = False, value: str = "", userName: str = "" ) -> Union[str, Dict[str, str]]:
        """Creates a secret for generating authentication tokens.

        For TigerGraph versions >= 4.0, uses the REST API endpoint.
        For older versions, uses the GSQL command.

        Args:
            alias (str, optional):
                The alias of the secret. If not provided, a random alias starting with
                `AUTO_GENERATED_ALIAS_` followed by a random 7-character string will be generated.
            value (str, optional):
                The secret value. If not provided (TigerGraph 4.x only), a random 32-character
                string will be generated. This parameter is only supported in TigerGraph >= 4.0.
            userName (str, optional):
                The user name for whom the secret is created. If not provided (TigerGraph 4.x only),
                the secret is created for the default logged-in user. This parameter is only
                supported in TigerGraph >= 4.0.
            withAlias (bool, optional):
                Return the new secret as an `{"alias": "secret"}` dictionary. This can be useful if
                an alias was not provided, for example if it is auto-generated.

        Returns:
            The secret string, or a dictionary with alias and secret if `withAlias` is True.

        Notes:
            Generally, secrets are generated by the database administrator and
            used to generate a token. If you use this function, please consider reviewing your
            internal processes of granting access to TigerGraph instances. Normally, this function
            should not be necessary and should not be executable by generic users.

        Endpoints:
            - `POST /gsql/v1/secrets` (In TigerGraph versions >= 4.0)
            - GSQL: `CREATE SECRET [<alias>]` (In TigerGraph versions < 4.0)
        """
        logger.debug("entry: createSecret")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        # Try to use TigerGraph 4.x REST API first, if failed then use GSQL command
        try:
            params = {}
            if userName is not None:
                params["userName"] = userName
            if alias:
                params["alias"] = alias
            if value:
                params["value"] = value

            res = await self._req("POST", self.gsUrl+"/gsql/v1/secrets",
                                params=params, authMode="pwd",
                                headers={'Content-Type': 'application/json'})

            # Response format: {"alias": "...", "value": "..."}
            secret = res.get("value", "")
            actual_alias = res.get("alias", alias)

            if withAlias:
                if logger.level == logging.DEBUG:
                    logger.debug("return: " + str({actual_alias: secret}))
                logger.debug("exit: createSecret")
                return {actual_alias: secret}

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(secret))
            logger.debug("exit: createSecret")
            return secret
        except Exception as e:
            # For older versions, use GSQL command
            res = await self.gsql("""
                USE GRAPH {}
                CREATE SECRET {} """.format(self.graphname, alias))
            secret = _parse_create_secret(
                res, alias=alias, withAlias=withAlias)

            # Alias was not provided, let's find out the autogenerated one
            # done in createSecret since need to call self.getSecrets which is a possibly async function
            if withAlias and not alias:
                masked = secret[:3] + "****" + secret[-3:]
                secs = await self.getSecrets()
                for a, s in secs.items():
                    if s == masked:
                        secret = {a: secret}

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(secret))
            logger.debug("exit: createSecret")
            return secret

    async def dropSecret(self, alias: Union[str, list], ignoreErrors: bool = True, userName: str = "") -> Union[str, dict]:
        """Drops a secret.

        For TigerGraph versions >= 4.0, uses the REST API endpoint.
        For older versions, uses the GSQL command.

        See https://docs.tigergraph.com/tigergraph-server/current/user-access/managing-credentials#_drop_a_secret

        Args:
            alias:
                One or more alias(es) or secret value(s) to drop. Can be a string or list of strings.
            ignoreErrors:
                Ignore errors arising from trying to drop non-existent secrets.
            userName (str, optional):
                The user name for whom to drop secrets. If not provided (TigerGraph 4.x only),
                drops secrets for the default logged-in user. This parameter is only
                supported in TigerGraph >= 4.0.

        Returns:
            For TigerGraph versions < 4.0: Returns the GSQL response string.
            For TigerGraph versions >= 4.0: Returns the REST API response dictionary.

        Raises:
            `TigerGraphException` if a non-existent secret is attempted to be dropped (unless
            `ignoreErrors` is `True`). Re-raises other exceptions.

        Endpoints:
            - `DELETE /gsql/v1/secrets/<alias>` (In TigerGraph versions >= 4.0, for single alias without userName)
            - `DELETE /gsql/v1/secrets?userName=<user>` (In TigerGraph versions >= 4.0, with payload)
            - GSQL: `DROP SECRET <secret> or <alias>` (In TigerGraph versions < 4.0)
        """
        logger.debug("entry: dropSecret")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        # For TigerGraph 4.x, use REST API
        if await self._version_greater_than_4_0():
            # Normalize to list
            secrets_list = [alias] if isinstance(alias, str) else alias

            if not secrets_list:
                raise TigerGraphException("No secret or alias provided.", 0)

            # Single alias without userName - use path parameter for cleaner API call
            if len(secrets_list) == 1 and not userName:
                single_secret = secrets_list[0]
                try:
                    res = await self._delete(self.gsUrl+"/gsql/v1/secrets/"+single_secret,
                                         authMode="pwd", resKey="message",
                                         headers={'Content-Type': 'application/json'})

                    if logger.level == logging.DEBUG:
                        logger.debug("return: " + str(res))
                    logger.debug("exit: dropSecret")
                    return res
                except Exception as e:
                    if not ignoreErrors:
                        raise
                    if logger.level == logging.DEBUG:
                        logger.debug("return: error ignored")
                    logger.debug("exit: dropSecret")
                    return {"error": False, "message": "Error ignored as requested"}

            # Multiple secrets/aliases or with userName - use payload
            url = self.gsUrl+"/gsql/v1/secrets"
            params = {}
            if userName:
                params["userName"] = userName

            data = {"secrets": secrets_list}
            try:
                res = await self._delete(url, data=data, params=params, authMode="pwd", resKey="message",
                                     headers={'Content-Type': 'application/json'}, jsonData=True)
                if logger.level == logging.DEBUG:
                    logger.debug("return: " + str(res))
                logger.debug("exit: dropSecret")
                return res
            except Exception as e:
                if not ignoreErrors:
                    raise
                if logger.level == logging.DEBUG:
                    logger.debug("return: error ignored")
                logger.debug("exit: dropSecret")
                return {"error": False, "message": "Error ignored as requested"}
        else:
            # For older versions, use GSQL command
            if isinstance(alias, str):
                alias = [alias]
            cmd = """
                USE GRAPH {}""".format(self.graphname)
            for a in alias:
                cmd += """
                    DROP SECRET {}""".format(a)
            res = await self.gsql(cmd)

            if "Failed to drop secrets" in res and not ignoreErrors:
                raise TigerGraphException(res)

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(res))
            logger.debug("exit: dropSecret")

            return res

    async def _token(self, secret: str = None, lifetime: int = None, token=None, _method=None, is_global: bool = True) -> Union[tuple, str]:
        method, url, alt_url, authMode, data, alt_data = _prep_token_request(self.restppUrl,
                                                                             self.gsUrl,
                                                                             None if is_global else self.graphname,
                                                                             secret=secret,
                                                                             lifetime=lifetime,
                                                                             token=token)
        #  _method Used for delete and refresh token

        # method == GET when using old version since _prep_newToken() gets the method for getting a new token for a version
        if method == "GET":
            if _method:
                method = _method

            # Use TG < 3.5 format (no json data)
            res = await self._req(method, url, authMode=authMode, data=data, resKey=None)
            mainVer = 3
        else:
            if _method:
                method = _method

            # Try using TG 4.1 endpoint first, if url not found then try <4.1 endpoint
            try:
                res = await self._req(method, url, authMode=authMode, data=data, resKey=None, jsonData=True)
                mainVer = 4
            except:
                try:
                    res = await self._req(method, alt_url, authMode=authMode, data=alt_data, resKey=None)
                    mainVer = 3
                except:
                    raise TigerGraphException("Error requesting token. Check if the connection's graphname is correct.", 400)

        # uses mainVer instead of _versionGreaterThan4_0 since you need a token for verson checking
        return res, mainVer

    async def getToken(self, secret: str = None, setToken: bool = True, lifetime: int = None, is_global: bool = True) -> Union[tuple, str]:
        logger.debug("entry: getToken")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        res, mainVer = await self._token(secret, lifetime, is_global=is_global)
        token, auth_header = _parse_token_response(res,
                                                   setToken,
                                                   mainVer,
                                                   self.base64_credential
                                                  )
        
        self.apiToken = token
        self.authHeader = auth_header
        self.authMode = "token"

        logger.debug("exit: getToken")
        return token

    async def refreshToken(self, secret: str = None, setToken: bool = True, lifetime: int = None, token="") -> Union[tuple, str]:
        logger.debug("entry: refreshToken")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if await self._version_greater_than_4_0():
            logger.debug("exit: refreshToken")
            raise TigerGraphException(
                "Refreshing tokens is only supported on versions of TigerGraph <= 4.0.0.", 0)

        if not token:
            token = self.apiToken
        res, mainVer = await self._token(secret=secret, lifetime=lifetime, token=token, _method="PUT")
        newToken = _parse_token_response(res, setToken, mainVer)

        logger.debug("exit: refreshToken")
        return newToken

    async def deleteToken(self, secret: str, token=None, skipNA=True) -> bool:
        if not token:
            token = self.apiToken
        res, _ = await self._token(secret=secret, token=token, _method="DELETE")

        if not res["error"] or (res["code"] == "REST-3300" and skipNA):
            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(True))
            logger.debug("exit: deleteToken")

            return True

        raise TigerGraphException(
            res["message"], (res["code"] if "code" in res else None))

    async def checkJwtToken(self, token: str = None) -> dict:
        """Check JWT token validity.

        Check if a JWT token is valid or not.

        Args:
            token (str, optional):
                The JWT token to check. If not provided, uses the current connection's token.

        Returns:
            dict: The response from the database containing the token validation result.

        Endpoints:
            - `POST /gsql/v1/tokens/check` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: checkJwtToken")
        if not await self._version_greater_than_4_0():
            logger.debug("exit: checkJwtToken")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if token is None:
            token = self.apiToken

        if not token:
            raise TigerGraphException("No token provided and no token is currently set.", 0)

        data = {"token": token}
        res = await self._req("POST", self.gsUrl+"/gsql/v1/tokens/check",
                             data=data, authMode="pwd", resKey="",
                             headers={'Content-Type': 'application/json'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: checkJwtToken")

        return res
