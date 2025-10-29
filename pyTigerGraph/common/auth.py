import json
import logging

from datetime import datetime
from typing import Union, Tuple, Dict

from pyTigerGraph.common.exception import TigerGraphException

logger = logging.getLogger(__name__)

def _parse_get_secrets(response: str) -> Dict[str, str]:
    secrets_dict = {}
    lines = response.split("\n")

    i = 0
    while i < len(lines):
        line = lines[i]
        # s = ""
        if "- Secret" in line:
            secret = line.split(": ")[1]
            i += 1
            line = lines[i]
            if "- Alias" in line:
                secrets_dict[line.split(": ")[1]] = secret
        i += 1
    return secrets_dict

def _parse_create_secret(response: str, alias: str = "", withAlias: bool = False) -> Union[str, Dict[str, str]]:
    try:
        if "already exists" in response:
            error_msg = "The secret "
            if alias != "":
                error_msg += "with alias {} ".format(alias)
            error_msg += "already exists."
            raise TigerGraphException(error_msg, "E-00001")

        secret = "".join(response).replace('\n', '').split(
            'The secret: ')[1].split(" ")[0].strip()

        if not withAlias:
            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(secret))
            logger.debug("exit: createSecret (withAlias")

            return secret

        if alias:
            ret = {alias: secret}

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(ret))
            logger.debug("exit: createSecret (alias)")

            return ret

        return secret

    except IndexError as e:
        raise TigerGraphException(
            "Failed to parse secret from response.", "E-00002") from e

def _prep_token_request(restppUrl: str,
                        gsUrl: str,
                        graphname: str = None,
                        version: str = None,
                        secret: str = None,
                        lifetime: int = None,
                        token: str = None,
                        method: str = None):
    major_ver, minor_ver, patch_ver = (0, 0, 0)
    if version:
        major_ver, minor_ver, patch_ver = version.split(".")

    if 0 < int(major_ver) < 3 or (int(major_ver) == 3 and int(minor_ver) < 5):
        method = "GET"
        url = restppUrl + "/requesttoken?secret=" + secret + \
            ("&lifetime=" + str(lifetime) if lifetime else "") + \
            ("&token=" + token if token else "")
        authMode = None
        if not secret:
            raise TigerGraphException(
                "Cannot request a token with username/password for versions < 3.5.")
    else:
        method = "POST"
        url = gsUrl + "/gsql/v1/tokens"  # used for TG 4.x
        data = {"graph": graphname} if graphname else {}

        # alt_url and alt_data used to construct the method and url for functions run in TG version 3.x
        alt_url = restppUrl+"/requesttoken"  # used for TG 3.x
        alt_data = {}

        if lifetime:
            data["lifetime"] = str(lifetime)
            alt_data["lifetime"] = str(lifetime)
        if token:
            data["tokens"] = token
            alt_data["token"] = token
        if graphname:
            alt_data["graph"] = graphname
        if secret:
            authMode = "None"
            data["secret"] = secret
            alt_data["secret"] = secret
        else:
            authMode = "pwd"

        alt_data = json.dumps(alt_data)

    return method, url, alt_url, authMode, data, alt_data

def _parse_token_response(response: dict,
                          setToken: bool,
                          mainVer: int,
                          base64_credential: str) -> Tuple[Union[Tuple[str, str], str], dict]:
    if not response.get("error"):
        # Note that /requesttoken has sightly different response using username-password pair.
        # See https://docs.tigergraph.com/tigergraph-server/3.10/api/built-in-endpoints#_request_a_token
        token = response.get("results", response)["token"]
        if setToken:
            apiToken = token
            authHeader = {'Authorization': "Bearer " + apiToken}
        else:
            apiToken = None
            authHeader = {
                'Authorization': 'Basic {0}'.format(base64_credential)}

        if response.get("expiration"):
            # On >=4.1 the format for the date of expiration changed. Convert back to old format
            # Can't use self._versionGreaterThan4_0 since you need a token for that
            if mainVer >= 4:
                return (token, response.get("expiration")), authHeader
            else:
                return (token, response.get("expiration"), \
                    datetime.utcfromtimestamp(
                        float(response.get("expiration"))).strftime('%Y-%m-%d %H:%M:%S')), authHeader
        else:
            return token, authHeader

    elif "Endpoint is not found from url = /requesttoken" in response["message"]:
        raise TigerGraphException("REST++ authentication is not enabled, can't generate token.",
                                    None)
    else:
        raise TigerGraphException(
            response["message"], (response["code"] if "code" in response else None))
