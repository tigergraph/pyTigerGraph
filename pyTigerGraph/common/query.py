"""Query Functions.

The functions on this page run installed or interpret queries in TigerGraph.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import json
import logging

from datetime import datetime
from typing import TYPE_CHECKING, Union, Optional

if TYPE_CHECKING:
    import pandas as pd

from pyTigerGraph.common.exception import TigerGraphException
from pyTigerGraph.common.util import (
    _safe_char
)

logger = logging.getLogger(__name__)

# TODO getQueries()  # List _all_ query names
def _parse_get_installed_queries(fmt, ret, graphname: str = ""):
    prefix = f"GET /query/{graphname}/" if graphname else "GET /query/"
    if fmt == "list":
        return [ep[len(prefix):] for ep in ret if ep.startswith(prefix)]
    ret = {ep: v for ep, v in ret.items() if ep.startswith(prefix)}
    if fmt == "json":
        ret = json.dumps(ret)
    if fmt == "df":
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("Pandas is required to use this function. "
                                "Download pandas using 'pip install pandas'.")
        ret = pd.DataFrame(ret).T
    return ret

# TODO installQueries()
#   POST /gsql/queries/install
#   xref:tigergraph-server:API:built-in-endpoints.adoc#_install_a_query[Install a query]

# TODO checkQueryInstallationStatus()
#   GET /gsql/queries/install/{request_id}
#   xref:tigergraph-server:API:built-in-endpoints.adoc#_check_query_installation_status[Check query installation status]

def _parse_query_parameters(params: dict) -> str:
    """Parses a dictionary of query parameters and converts them to query strings.

    While most of the values provided for various query parameter types can be easily converted
    to query strings (key1=value1&key2=value2), `SET` and `BAG` parameter types, and especially
    `VERTEX` and `SET<VERTEX>` (i.e. vertex primary ID types without vertex type specification)
    require special handling.

    See xref:tigergraph-server:API:built-in-endpoints.adoc#_query_parameter_passing[Query parameter passing]
    """
    logger.debug("entry: _parseQueryParameters")
    logger.debug("params: " + str(params))

    parts = []
    for k, v in params.items():
        if isinstance(v, tuple):
            if len(v) == 1:
                # VERTEX<T> (typed): (id,)  →  k=id
                parts.append(k + "=" + _safe_char(v[0]))
            elif len(v) == 2 and isinstance(v[1], str):
                if not v[1]:
                    raise TigerGraphException(
                        f"Invalid vertex parameter '{k}': vertex type string must not be empty. "
                        "Use (id,) for VERTEX<T> or (id, 'type') for untyped VERTEX.")
                # VERTEX (untyped): (id, "type")  →  k=id&k.type=type
                parts.append(k + "=" + str(v[0]))
                parts.append(k + ".type=" + _safe_char(v[1]))
            else:
                raise TigerGraphException(
                    f"Invalid vertex parameter '{k}': expected (id,) for VERTEX<T> "
                    "or (id, 'type') for untyped VERTEX.")
        elif isinstance(v, list):
            for i, vv in enumerate(v):
                if isinstance(vv, tuple):
                    if len(vv) == 1:
                        # SET<VERTEX<T>>: (id,)  →  k=id  (repeated, no index)
                        parts.append(k + "=" + _safe_char(vv[0]))
                    elif len(vv) == 2 and isinstance(vv[1], str):
                        if not vv[1]:
                            raise TigerGraphException(
                                f"Invalid vertex parameter '{k}[{i}]': vertex type string must not be empty. "
                                "Use (id,) for VERTEX<T> or (id, 'type') for untyped VERTEX.")
                        # SET<VERTEX>: (id, "type")  →  k[i]=id&k[i].type=type
                        parts.append(k + "[" + str(i) + "]=" + _safe_char(vv[0]))
                        parts.append(k + "[" + str(i) + "].type=" + vv[1])
                    else:
                        raise TigerGraphException(
                            f"Invalid vertex parameter '{k}[{i}]': expected (id,) for VERTEX<T> "
                            "or (id, 'type') for untyped VERTEX.")
                else:
                    parts.append(k + "=" + _safe_char(vv))
        elif isinstance(v, datetime):
            parts.append(k + "=" + _safe_char(v.strftime("%Y-%m-%d %H:%M:%S")))
        else:
            parts.append(k + "=" + _safe_char(v))
    ret = "&".join(parts)

    if logger.level == logging.DEBUG:
        logger.debug("return: " + str(ret))
    logger.debug("exit: _parseQueryParameters")

    return ret

def _encode_str_for_post(value: str) -> str:
    """Encode ``%`` as ``%25`` in string values destined for TigerGraph's POST
    /query JSON body.

    TigerGraph's RESTPP POST parser URL-decodes string values inside JSON
    payloads.  A bare ``%`` followed by hex-like characters is mis-interpreted
    as a percent-encoded sequence, causing a ``REST-30000`` parse error.
    Encoding ``%`` → ``%25`` prevents this; the server decodes it back to
    ``%`` transparently.
    """
    return value.replace("%", "%25")


def _prep_query_parameters_json(params: dict) -> dict:
    """Converts a parameter dictionary into the JSON format expected by TigerGraph's
    POST /query endpoint.

    Handles the same Python conventions as ``_parse_query_parameters`` (used for
    GET mode), but produces a JSON-serialisable dict instead of a query string.

    Conversion rules (per TigerGraph REST API docs):
        - ``(id,)`` 1-tuple        →  ``{"id": id}``
          Use for VERTEX<T> (typed vertex) parameters.
        - ``(id, "type")`` 2-tuple →  ``{"id": id, "type": "type"}``
          Use for VERTEX (untyped vertex) parameters.
        - ``list`` of 1-tuples     →  ``[{"id": id}, ...]``
          Use for SET<VERTEX<T>> (typed vertex set) parameters.
        - ``list`` of 2-tuples     →  ``[{"id": id, "type": "type"}, ...]``
          Use for SET<VERTEX> (untyped vertex set) parameters.
        - ``datetime``             →  ``"YYYY-MM-DD HH:MM:SS"``
        - ``dict`` with ``"id"``   →  passed through unchanged
          (caller already supplied correct vertex JSON object)
        - ``dict`` without ``"id"``→  ``{"keylist": [...], "valuelist": [...]}``
          (Python dict → TigerGraph MAP wire format)
        - ``list`` of dicts        →  passed through unchanged
        - ``str``                  →  ``%`` encoded as ``%25``
          (TigerGraph URL-decodes strings inside JSON POST bodies)
        - primitive / other        →  left as-is

    Note: plain ``str`` / ``int`` values are correct for STRING/INT/FLOAT/…
    parameters only.  Always use tuples for vertex parameters — the API cannot
    distinguish a STRING value from a VERTEX<T> ID without schema introspection.

    See https://docs.tigergraph.com/tigergraph-server/4.2/api/built-in-endpoints#_run_an_installed_query_post
    """
    if not params or not isinstance(params, dict):
        return params

    converted = {}
    for k, v in params.items():
        if isinstance(v, tuple):
            if len(v) == 1:
                # VERTEX<T> (typed): (id,)  →  {"id": id}
                converted[k] = {"id": v[0]}
            elif len(v) == 2 and isinstance(v[1], str):
                if not v[1]:
                    raise TigerGraphException(
                        f"Invalid vertex parameter '{k}': vertex type string must not be empty. "
                        "Use (id,) for VERTEX<T> or (id, 'type') for untyped VERTEX.")
                # VERTEX (untyped): (id, "type")  →  {"id": id, "type": "type"}
                converted[k] = {"id": v[0], "type": v[1]}
            else:
                raise TigerGraphException(
                    f"Invalid vertex parameter '{k}': expected (id,) for VERTEX<T> "
                    "or (id, 'type') for untyped VERTEX.")
        elif isinstance(v, dict):
            if "id" in v:
                # Pre-formatted vertex object — pass through unchanged.
                converted[k] = v
            else:
                # Python dict → TigerGraph MAP wire format.
                converted[k] = {
                    "keylist": list(v.keys()),
                    "valuelist": list(v.values()),
                }
        elif isinstance(v, list):
            new_list = []
            for vv in v:
                if isinstance(vv, tuple):
                    if len(vv) == 1:
                        # SET<VERTEX<T>>: (id,)  →  {"id": id}
                        new_list.append({"id": vv[0]})
                    elif len(vv) == 2 and isinstance(vv[1], str):
                        if not vv[1]:
                            raise TigerGraphException(
                                f"Invalid vertex parameter '{k}': vertex type string must not be empty. "
                                "Use (id,) for VERTEX<T> or (id, 'type') for untyped VERTEX.")
                        # SET<VERTEX>: (id, "type")  →  {"id": id, "type": "type"}
                        new_list.append({"id": vv[0], "type": vv[1]})
                    else:
                        raise TigerGraphException(
                            f"Invalid vertex parameter '{k}': expected (id,) for VERTEX<T> "
                            "or (id, 'type') for untyped VERTEX.")
                elif isinstance(vv, datetime):
                    new_list.append(vv.strftime("%Y-%m-%d %H:%M:%S"))
                elif isinstance(vv, str):
                    new_list.append(_encode_str_for_post(vv))
                else:
                    new_list.append(vv)
            converted[k] = new_list
        elif isinstance(v, datetime):
            converted[k] = v.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(v, str):
            converted[k] = _encode_str_for_post(v)
        else:
            converted[k] = v
    return converted


def _prep_run_installed_query(timeout, sizeLimit, runAsync, replica, threadLimit, memoryLimit):
    """header builder for runInstalledQuery()"""
    headers = {}
    res_key = "results"
    if timeout and timeout > 0:
        headers["GSQL-TIMEOUT"] = str(timeout)
    if sizeLimit and sizeLimit > 0:
        headers["RESPONSE-LIMIT"] = str(sizeLimit)
    if runAsync:
        headers["GSQL-ASYNC"] = "true"
        res_key = "request_id"
    if replica:
        headers["GSQL-REPLICA"] = str(replica)
    if threadLimit:
        headers["GSQL-THREAD-LIMIT"] = str(threadLimit)
    if memoryLimit:
        headers["GSQL-QueryLocalMemLimitMB"] = str(memoryLimit)
    return headers, res_key

def _prep_get_statistics(seconds, segments):
    '''parameter parsing for getStatistics()'''
    if not seconds:
        seconds = 10
    else:
        seconds = max(min(seconds, 0), 60)
    if not segments:
        segments = 10
    else:
        segments = max(min(segments, 0), 100)
    return seconds, segments
