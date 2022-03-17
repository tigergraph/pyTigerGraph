import json
import time
from datetime import datetime

import requests

from pyTigerGraph.pyTigerGraphException import TigerGraphException
from pyTigerGraph.pyTigerGraphGSQL import pyTigerGraphGSQL


class pyTigerGraphAuth(pyTigerGraphGSQL):
    def showSecrets(self) -> dict:
        """Issues a `CREATE SECRET` GSQL statement and returns the secret generated by that
            statement.

        Returns:
            A dictionary of <alias>: <secret_string> pairs.

        This function return the masked version of secrets. The original value of secrets cannot be
            retrieved after creation. So, this function is not very useful.

        Documentation:
            https://docs.tigergraph.com/admin/admin-guide/user-access/managing-credentials#create-a-secret
        """
        if not self.gsqlInitiated:
            self.initGsql()

        response = self.gsql("""
                USE GRAPH {}
                SHOW SECRET """.format(self.graphname))
        return response
        # TODO Process response, return a dictionary of alias/secret pairs

    # TODO showSecret()

    def createSecret(self, alias: str = "") -> str:
        """Issues a `CREATE SECRET` GSQL statement and returns the secret generated by that statement.

        Args:
            alias:
                The alias of the secret.
                Beginning with TigerGraph 3.1.4, the system will generate a random alias for the
                secret if the user does not provide an alias for that secret. Randomly generated
                aliases begin with AUTO_GENERATED_ALIAS_ and include a random 7-character string.

        Returns:
            The secret string.

        Documentation:
            https://docs.tigergraph.com/admin/admin-guide/user-access/managing-credentials#create-a-secret
        """
        if not self.gsqlInitiated:
            self.initGsql()

        response = self.gsql("""
        USE GRAPH {}
        CREATE SECRET {} """.format(self.graphname, alias))
        try:
            # print(response)
            if ("already exists" in response):
                # get the sec
                errorMsg = "The secret "
                if alias != "":
                    errorMsg += "with alias {} ".format(alias)
                errorMsg += "already exists."
                raise TigerGraphException(errorMsg, "E-00001")
            secret = "".join(response).replace('\n', '').split('The secret: ')[1].split(" ")[0]
            return secret.strip()
        except:
            raise

    # def dropSecret(self, alias: str) -> bool:
    """
        Args:
            alias:
                The alias of the secret.

    Documentation:
        https://docs.tigergraph.com/admin/admin-guide/user-access/managing-credentials#drop-a-secret
    """

    # TODO Implementation

    def getToken(self, secret: str, setToken: bool = True, lifetime: int = None) -> tuple:
        """Requests an authorization token.

        This function returns a token only if REST++ authentication is enabled. If not, an exception
        will be raised.
        See: https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication

        Args:
            secret:
                The secret (string) generated in GSQL using `CREATE SECRET`.
                See https://docs.tigergraph.com/tigergraph-server/current/user-access/managing-credentials#_create_a_secret
            setToken:
                Set the connection's API token to the new value (default: True).
            lifetime:
                Duration of token validity (in secs, default 30 days = 2,592,000 secs).

        Returns:
            A tuple of (<token>, <expiration_timestamp_unixtime>, <expiration_timestamp_ISO8601>).
            Return value can be ignored.
            Expiration timestamp's time zone might be different from your computer's local time zone.

        Raises:
            `TigerGraphException` if REST++ authentication is not enabled or authentication error
            occurred.

        Endpoint:
            GET /requesttoken
        Documentation:
            https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_request_a_token
        """
        s, m, i = (0, 0, 0)
        res = {}
        if self.version:
            s, m, i = self.version.split(".")
        success = False
        if int(s) < 3 or (int(s) >= 3 and int(m) < 5):
            try:
                if self.useCert and self.certPath:
                    res = json.loads(requests.request("GET", self.restppUrl +
                                                             "/requesttoken?secret=" + secret +
                                                             ("&lifetime=" + str(
                                                                 lifetime) if lifetime else "")).text)
                else:
                    res = json.loads(requests.request("GET", self.restppUrl +
                                                             "/requesttoken?secret=" + secret +
                                                             ("&lifetime=" + str(
                                                                 lifetime) if lifetime else ""),
                        verify=False).text)
                if not res["error"]:
                    success = True
            except:
                success = False
        if not success:
            try:
                data = {"secret": secret}

                if lifetime:
                    data["lifetime"] = str(lifetime)
                if self.useCert is True and self.certPath is not None:
                    res = json.loads(requests.post(self.restppUrl + "/requesttoken",
                        data=json.dumps(data)).text)
                else:
                    res = json.loads(requests.post(self.restppUrl + "/requesttoken",
                        data=json.dumps(data), verify=False).text)
            except:
                success = False
        if not res["error"]:
            if setToken:
                self.apiToken = res["token"]
                self.authHeader = {'Authorization': "Bearer " + self.apiToken}
            else:
                self.apiToken = None
                self.authHeader = {'Authorization': 'Basic {0}'.format(self.base64_credential)}

            return res["token"], res["expiration"], \
                datetime.utcfromtimestamp(float(res["expiration"])).strftime('%Y-%m-%d %H:%M:%S')
        if "Endpoint is not found from url = /requesttoken" in res["message"]:
            raise TigerGraphException("REST++ authentication is not enabled, can't generate token.",
                None)
        raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def refreshToken(self, secret: str, token: str = "", lifetime: int = None) -> tuple:
        """Extends a token's lifetime.

        This function works only if REST++ authentication is enabled. If not, an exception will be
        raised.
        See: https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication

        Args:
            secret:
                The secret (string) generated in GSQL using `CREATE SECRET`.
                See https://docs.tigergraph.com/tigergraph-server/current/user-access/managing-credentials#_create_a_secret
            token:
                The token requested earlier. If not specified, refreshes current connection's token.
            lifetime:
                Duration of token validity (in secs, default 30 days = 2,592,000 secs) from current
                system timestamp.

        Returns:
            A tuple of (<token>, <expiration_timestamp_unixtime>, <expiration_timestamp_ISO8601>).
            Return value can be ignored.
            Expiration timestamp's time zone might be different from your computer's local time zone.
            New expiration timestamp will be now + lifetime seconds, _not_ current expiration
            timestamp + lifetime seconds.

        Raises:
            `TigerGraphException` if REST++ authentication is not enabled or authentication error
            occurred, e.g. specified token does not exists.

        Note:

        Endpoint:
            PUT /requesttoken
        Documentation:
            https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_refresh_a_token
        TODO Rework lifetime parameter handling the same as in getToken()
        """
        if not token:
            token = self.apiToken
        if self.useCert and self.certPath:
            res = json.loads(requests.request("PUT", self.restppUrl + "/requesttoken?secret=" +
                                                     secret + "&token=" + token +
                                                     ("&lifetime=" + str(
                                                         lifetime) if lifetime else ""),
                verify=False).text)
        else:
            res = json.loads(requests.request("PUT", self.restppUrl + "/requesttoken?secret=" +
                                                     secret + "&token=" + token +
                                                     ("&lifetime=" + str(
                                                         lifetime) if lifetime else "")).text)
        if not res["error"]:
            exp = time.time() + res["expiration"]
            return res["token"], int(exp), datetime.utcfromtimestamp(exp).strftime(
                '%Y-%m-%d %H:%M:%S')
        if "Endpoint is not found from url = /requesttoken" in res["message"]:
            raise TigerGraphException("REST++ authentication is not enabled, can't refresh token.",
                None)
        raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def deleteToken(self, secret, token=None, skipNA=True):
        """Deletes a token.

        This function works only if REST++ authentication is enabled. If not, an exception will be
        raised.
        See: https://docs.tigergraph.com/tigergraph-server/current/user-access/enabling-user-authentication#_enable_restpp_authentication

        Args:
            secret:
                The secret (string) generated in GSQL using `CREATE SECRET`.
                See https://docs.tigergraph.com/tigergraph-server/current/user-access/managing-credentials#_create_a_secret
            token:
                The token requested earlier. If not specified, deletes current connection's token,
                so be careful.
            skipNA:
                Don't raise exception if specified token does not exist.

        Returns:
            `True` if deletion was successful, or token did not exist but `skipNA` was `True`.

        Raises:
            `TigerGraphException` if REST++ authentication is not enabled or authentication error
            occurred, e.g. specified token does not exists.

        Endpoint:
            DELETE /requesttoken
        Documentation:
            https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_delete_a_token
        """
        if not token:
            token = self.apiToken
        if self.useCert is True and self.certPath is not None:
            res = json.loads(
                requests.request("DELETE",
                    self.restppUrl + "/requesttoken?secret=" + secret + "&token=" + token,
                    verify=False).text)
        else:
            res = json.loads(
                requests.request("DELETE",
                    self.restppUrl + "/requesttoken?secret=" + secret + "&token=" + token).text)
        if not res["error"]:
            return True
        if res["code"] == "REST-3300" and skipNA:
            return True
        if "Endpoint is not found from url = /requesttoken" in res["message"]:
            raise TigerGraphException("REST++ authentication is not enabled, can't delete token.",
                None)
        raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))
