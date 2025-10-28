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
def _parse_get_installed_queries(fmt, ret):
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

    TODO Accept this format for SET<VERTEX>:
        "key": [([p_id1, p_id2, ...], "vtype"), ...]
        I.e. multiple primary IDs of the same vertex type
    """
    logger.debug("entry: _parseQueryParameters")
    logger.debug("params: " + str(params))

    ret = ""
    for k, v in params.items():
        if isinstance(v, tuple):
            if len(v) == 2 and isinstance(v[1], str):
                ret += k + "=" + str(v[0]) + "&" + k + \
                    ".type=" + _safe_char(v[1]) + "&"
            else:
                raise TigerGraphException(
                    "Invalid parameter value: (vertex_primary_id, vertex_type)"
                    " was expected.")
        elif isinstance(v, list):
            i = 0
            for vv in v:
                if isinstance(vv, tuple):
                    if len(vv) == 2 and isinstance(vv[1], str):
                        ret += k + "[" + str(i) + "]=" + _safe_char(vv[0]) + "&" + \
                            k + "[" + str(i) + "].type=" + vv[1] + "&"
                    else:
                        raise TigerGraphException(
                            "Invalid parameter value: (vertex_primary_id, vertex_type)"
                            " was expected.")
                else:
                    ret += k + "=" + _safe_char(vv) + "&"
                i += 1
        elif isinstance(v, datetime):
            ret += k + "=" + \
                _safe_char(v.strftime("%Y-%m-%d %H:%M:%S")) + "&"
        else:
            ret += k + "=" + _safe_char(v) + "&"
    if ret:
        ret = ret[:-1]

    if logger.level == logging.DEBUG:
        logger.debug("return: " + str(ret))
    logger.debug("exit: _parseQueryParameters")

    return ret

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
