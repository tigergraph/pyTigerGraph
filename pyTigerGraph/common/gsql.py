"""GSQL Interface

Use GSQL within pyTigerGraph.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import logging
import re

from typing import Union, Tuple, Dict
from urllib.parse import urlparse, quote_plus

from pyTigerGraph.common.base import PyTigerGraphCore
from pyTigerGraph.common.exception import TigerGraphException

logger = logging.getLogger(__name__)

ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

# Once again could just put resand query parameter in but this is more braindead and allows for easier pattern
def _parse_gsql(res, query: str, graphname: str = None, options=None):
    def check_error(query: str, resp: str) -> None:
        if "CREATE VERTEX" in query.upper():
            if "Failed to create vertex types" in resp:
                raise TigerGraphException(resp)
        if ("CREATE DIRECTED EDGE" in query.upper()) or ("CREATE UNDIRECTED EDGE" in query.upper()):
            if "Failed to create edge types" in resp:
                raise TigerGraphException(resp)
        if "CREATE GRAPH" in query.upper():
            if ("The graph" in resp) and ("could not be created!" in resp):
                raise TigerGraphException(resp)
        if "CREATE DATA_SOURCE" in query.upper():
            if ("Successfully created local data sources" not in resp) and ("Successfully created data sources" not in resp):
                raise TigerGraphException(resp)
        if "CREATE LOADING JOB" in query.upper():
            if "Successfully created loading jobs" not in resp:
                raise TigerGraphException(resp)
        if "RUN LOADING JOB" in query.upper():
            if "LOAD SUCCESSFUL" not in resp:
                raise TigerGraphException(resp)

    def clean_res(resp: list) -> str:
        ret = []
        for line in resp:
            if not line.startswith("__GSQL__"):
                ret.append(line)
        return "\n".join(ret)

    if isinstance(res, list):
        ret = clean_res(res)
    else:
        ret = clean_res(res.splitlines())

    check_error(query, ret)

    string_without_ansi = ANSI_ESCAPE.sub('', ret)

    if logger.level == logging.DEBUG:
        logger.debug("return: " + str(ret))
    logger.debug("exit: gsql (success)")

    return string_without_ansi

def _prep_get_udf(ExprFunctions: bool = True, ExprUtil: bool = True):
    urls = {}  # urls when using TG 4.x
    alt_urls = {}  # urls when using TG 3.x
    if ExprFunctions:
        alt_urls["ExprFunctions"] = (
            "/gsqlserver/gsql/userdefinedfunction?filename=ExprFunctions")
        urls["ExprFunctions"] = ("/gsql/v1/udt/files/ExprFunctions")
    if ExprUtil:
        alt_urls["ExprUtil"] = (
            "/gsqlserver/gsql/userdefinedfunction?filename=ExprUtil")
        urls["ExprUtil"] = ("/gsql/v1/udt/files/ExprUtil")

    return urls, alt_urls

def _parse_get_udf(responses, json_out):
    rets = []
    for file_name in responses:
        resp = responses[file_name]
        if not resp["error"]:
            logger.info(f"{file_name} get successfully")
            rets.append(resp["results"])
        else:
            logger.error(f"Failed to get {file_name}")
            raise TigerGraphException(resp["message"])

    if json_out:
        # concatente the list of dicts into one dict
        rets = rets[0].update(rets[-1])
        return rets
    if len(rets) == 2:
        return tuple(rets)
    if rets:
        return rets[0]
    return ""
