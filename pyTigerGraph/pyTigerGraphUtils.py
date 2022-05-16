"""Utility Functions.

Utility functions for pyTigerGraph.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object]. 
"""
import json
import re
import urllib
from typing import Any, Union
from urllib.parse import urlparse

import requests
from typing import TYPE_CHECKING, Union

from pyTigerGraph.pyTigerGraphBase import pyTigerGraphBase
from pyTigerGraph.pyTigerGraphException import TigerGraphException


class pyTigerGraphUtils(pyTigerGraphBase):
    def _safeChar(self, inputString: Any) -> str:
        """Replace special characters in string using the %xx escape.

        Args:
            inputString:
                The string to process

        Returns:
            Processed string.

        Documentation:
            https://docs.python.org/3/library/urllib.parse.html#url-quoting
        """
        return urllib.parse.quote(str(inputString), safe='')

    def echo(self, usePost: bool = False) -> str:
        """Pings the database.

        Args:
            usePost:
                Use POST instead of GET

        Returns:
            "Hello GSQL" if everything was OK.

        Endpoint:
            - `GET /echo`
            - `POST /echo`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_echo[Echo]

        TODO Implement POST
        """
        if usePost:
            return str(self._post(self.restppUrl + "/echo/" + self.graphname, resKey="message"))
        return str(self._get(self.restppUrl + "/echo/" + self.graphname, resKey="message"))

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
        if self.useCert and self.certPath:
            response = requests.request("GET", self.restppUrl + "/version/" + self.graphname,
                headers=self.authHeader, verify=False)
        else:
            response = requests.request("GET", self.restppUrl + "/version/" + self.graphname,
                headers=self.authHeader)
        res = json.loads(response.text, strict=False)  # "strict=False" is why _get() was not used
        self._errorCheck(res)

        if raw:
            return response.text
        res = res["message"].split("\n")
        components = []
        for i in range(len(res)):
            if 2 < i < len(res) - 1:
                m = res[i].split()
                component = {"name": m[0], "version": m[1], "hash": m[2],
                    "datetime": m[3] + " " + m[4] + " " + m[5]}
                components.append(component)
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
        ret = ""
        for v in self.getVersion():
            if v["name"] == component.lower():
                ret = v["version"]
        if ret != "":
            if full:
                return ret
            ret = re.search("_.+_", ret)
            return ret.group().strip("_")
        else:
            raise TigerGraphException("\"" + component + "\" is not a valid component.", None)

    def getLicenseInfo(self) -> dict:
        """Returns the expiration date and remaining days of the license.

        Returns:
            Returns license details. For an evaluation/trial deployment, returns an information message and -1 remaining days.

        TODO Check if this endpoint was still available.
        """
        res = self._get(self.restppUrl + "/showlicenseinfo", resKey="", skipCheck=True)
        ret = {}
        if not res["error"]:
            ret["message"] = res["message"]
            ret["expirationDate"] = res["results"][0]["Expiration date"]
            ret["daysRemaining"] = res["results"][0]["Days remaining"]
        elif "code" in res and res["code"] == "REST-5000":
            ret["message"] = \
                "This instance does not have a valid enterprise license. Is this a trial version?"
            ret["daysRemaining"] = -1
        else:
            raise TigerGraphException(res["message"], res["code"])
        return ret
