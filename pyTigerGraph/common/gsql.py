"""GSQL Interface

Use GSQL within pyTigerGraph.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].

This module also defines the canonical set of GSQL reserved keywords,
serving as the single source of truth for pyTigerGraph, tigergraph-mcp,
and any downstream application.
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

_GSQL_ERROR_PATTERNS = [
    "Encountered \"",
    "SEMANTIC ERROR",
    "Syntax Error",
    "Failed to create",
    "does not exist",
    "is not a valid",
    "already exists",
    "Invalid syntax",
]


def _wrap_gsql_result(result, skipCheck: bool = False):
    """Wrap a gsql() string result into a dict matching 4.x REST response format.

    Args:
        result:     The raw string returned by ``gsql()``.
        skipCheck:  If ``False`` (default), raises ``TigerGraphException`` when
                    an error pattern is detected — consistent with ``_error_check``
                    on the 4.x REST path.  If ``True``, returns the dict with
                    ``"error": True`` without raising.
    """
    msg = str(result) if result else ""
    has_error = any(p in msg for p in _GSQL_ERROR_PATTERNS)
    if has_error and not skipCheck:
        raise TigerGraphException(msg)
    return {
        "error": has_error,
        "message": msg,
    }


def _parse_graph_list(gsql_output):
    """Parse ``SHOW GRAPH *`` output into a list of dicts matching 4.x REST format."""
    output = str(gsql_output) if gsql_output else ""
    graphs = []
    for line in output.splitlines():
        stripped = line.strip().lstrip("- ").strip()
        if not stripped.startswith("Graph "):
            continue
        paren_start = stripped.find("(")
        name = stripped[6:paren_start].strip() if paren_start > 6 else stripped[6:].strip()
        if not name or name == "*":
            continue
        vertices = []
        edges = []
        if paren_start != -1:
            paren_end = stripped.rfind(")")
            inner = stripped[paren_start + 1:paren_end] if paren_end > paren_start else ""
            for token in inner.split(","):
                token = token.strip()
                if token.endswith(":v"):
                    vertices.append(token[:-2])
                elif token.endswith(":e"):
                    edges.append(token[:-2])
        graphs.append({
            "GraphName": name,
            "VertexTypes": vertices,
            "EdgeTypes": edges,
        })
    return graphs


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


# ─── GSQL Reserved Keywords ──────────────────────────────────────────────

_RESERVED_KEYWORDS: frozenset = frozenset({
    "ACCUM", "ADD", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "AS", "ASC",
    "AVG", "BAG", "BATCH", "BETWEEN", "BIGINT", "BLOB", "BOOL", "BOOLEAN",
    "BOTH", "BREAK", "BY", "CALL", "CASCADE", "CASE", "CATCH", "CHAR",
    "CHARACTER", "CHECK", "CLOB", "COALESCE", "COMPRESS", "CONST", "CONSTRAINT",
    "CONTINUE", "COST", "COUNT", "CREATE", "CURRENT_DATE", "CURRENT_TIME",
    "CURRENT_TIMESTAMP", "CURSOR", "KAFKA", "S3", "DATETIME", "DATETIME_ADD",
    "DATETIME_SUB", "DAY", "DATETIME_DIFF", "DATETIME_TO_EPOCH",
    "DATETIME_FORMAT", "DECIMAL", "DECLARE", "DELETE", "DESC", "DISTRIBUTED",
    "DO", "DOUBLE", "DROP", "EDGE", "ELSE", "ELSEIF", "EPOCH_TO_DATETIME",
    "END", "ESCAPE", "EXCEPTION", "EXISTS", "FALSE", "FILE", "SYS.FILE_NAME",
    "FILTER", "FIXED_BINARY", "FLOAT", "FOR", "FOREACH", "FROM", "GLOBAL",
    "GRANTS", "GRAPH", "GROUP", "GROUPBYACCUM", "HAVING", "HOUR", "HEADER",
    "HEAPACCUM", "IF", "IGNORE", "SYS.INTERNAL_ID", "IN", "INDEX",
    "INPUT_LINE_FILTER", "INSERT", "INT", "INTERSECT", "INT8", "INT16", "INT32",
    "INT32_T", "INT64_T", "INTEGER", "INTERPRET", "INTO", "IS", "ISEMPTY",
    "JOB", "JOIN", "JSONARRAY", "JSONOBJECT", "KEY", "LEADING", "LIKE", "LIMIT",
    "LIST", "LOAD", "LOADACCUM", "LOG", "LONG", "MAP", "MINUTE", "NOBODY",
    "NOT", "NOW", "NULL", "OFFSET", "ON", "OPENCYPHER", "OR", "ORDER",
    "PINNED", "POLICY", "POST_ACCUM", "POST-ACCUM", "PRIMARY", "PRIMARY_ID",
    "PRINT", "PROXY", "QUERY", "QUIT", "RAISE", "RANGE", "REDUCE", "REPLACE",
    "RESET_COLLECTION_ACCUM", "RETURN", "RETURNS", "ROW", "SAMPLE", "SECOND",
    "SELECT", "SELECTVERTEX", "SET", "STATIC", "STRING", "SUM", "TARGET",
    "TEMP_TABLE", "THEN", "TO", "TO_CSV", "TO_DATETIME", "TRAILING",
    "TRANSLATESQL", "TRIM", "TRUE", "TRY", "TUPLE", "TYPE", "TYPEDEF", "UINT",
    "UINT8", "UINT16", "UINT32", "UINT8_T", "UINT32_T", "UINT64_T", "UNION",
    "UPDATE", "UPSERT", "USING", "VALUES", "VERTEX", "WHEN", "WHERE", "WHILE",
    "WITH", "GSQL_SYS_TAG", "_INTERNAL_ATTR_TAG",
})


def _get_reserved_keywords() -> frozenset:
    """Return the full set of GSQL reserved keywords.

    Returns:
        A frozenset of uppercase keyword strings.
    """
    return _RESERVED_KEYWORDS


def _is_reserved_keyword(name: str) -> bool:
    """Check whether *name* is a GSQL reserved keyword (case-insensitive).

    Args:
        name: The identifier to check.

    Returns:
        True if the name is reserved.
    """
    return name.upper() in _RESERVED_KEYWORDS
