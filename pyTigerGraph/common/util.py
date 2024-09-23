"""Utility Functions.

Utility functions for pyTigerGraph.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""

import logging
import urllib

from typing import Any, TYPE_CHECKING
from urllib.parse import urlparse

from pyTigerGraph.common.exception import TigerGraphException

logger = logging.getLogger(__name__)

def _safe_char(inputString: Any) -> str:
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

def _parse_get_license_info(res):
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

def _prep_get_system_metrics(from_ts: int = None,
                             to_ts: int = None,
                             latest: int = None,
                             who: str = None,
                             where: str = None):
    params = {}
    _json = {}  # in >=4.1 we need a json request of different parameter names
    if from_ts or to_ts:
        _json["TimeRange"] = {}
    if from_ts:
        params["from"] = from_ts
        _json['TimeRange']['StartTimestampNS'] = str(from_ts)
    if to_ts:
        params["to"] = to_ts
        _json['TimeRange']['EndTimestampNS'] = str(from_ts)
    if latest:
        params["latest"] = latest
        _json["LatestNum"] = str(latest)
    if who:
        params["who"] = who
    if where:
        params["where"] = where
        _json["HostID"] = where

    return params, _json
