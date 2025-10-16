"""Vertex Functions.

Functions to upsert, retrieve and delete vertices.

All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import json
import logging

from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    import pandas as pd

from pyTigerGraph.common.exception import TigerGraphException
from pyTigerGraph.common.util import _safe_char

logger = logging.getLogger(__name__)


def _parse_get_vertex_count(res, vertexType, where):
    if where:
        if vertexType == "*":
            raise TigerGraphException(
                "VertexType cannot be \"*\" if where condition is specified.", None)
        else:
            raise TigerGraphException(
                "VertexType cannot be a list if where condition is specified.", None)

    ret = {d["v_type"]: d["count"] for d in res}

    if isinstance(vertexType, list):
        ret = {vt: ret[vt] for vt in vertexType}

    return ret

def _prep_upsert_vertex_dataframe(df, v_id, attributes):
    json_up = []

    for index in df.index:
        json_up.append(json.loads(df.loc[index].to_json()))
        json_up[-1] = (
            index if v_id is None else json_up[-1][v_id],
            json_up[-1] if attributes is None
            else {target: json_up[-1][source] for target, source in attributes.items()}
        )
    return json_up

def _prep_get_vertices(restppUrl: str, graphname: str, vertexType: str, select: str = "", where: str = "",
                        limit: Union[int, str] = None, sort: str = "", timeout: int = 0):
    '''url builder for getVertices()'''

    url = restppUrl + "/graph/" + graphname + "/vertices/" + vertexType
    isFirst = True
    if select:
        url += "?select=" + select
        isFirst = False
    if where:
        url += ("?" if isFirst else "&") + "filter=" + where
        isFirst = False
    if limit:
        url += ("?" if isFirst else "&") + "limit=" + str(limit)
        isFirst = False
    if sort:
        url += ("?" if isFirst else "&") + "sort=" + sort
        isFirst = False
    if timeout and timeout > 0:
        url += ("?" if isFirst else "&") + "timeout=" + str(timeout)
    return url

def _prep_get_vertices_by_id(restppUrl: str, graphname: str, vertexIds, vertexType):
    '''parameter parsing and url building for getVerticesById()'''

    if not vertexIds:
        raise TigerGraphException("No vertex ID was specified.", None)
    vids = []
    if isinstance(vertexIds, (int, str)):
        vids.append(vertexIds)
    else:
        vids = vertexIds
    url = restppUrl + "/graph/" + graphname + "/vertices/" + vertexType + "/"
    return vids, url

def _parse_get_vertex_stats(responses, skipNA):
    '''response parsing for getVertexStats()'''
    ret = {}
    for vt, res in responses:
        if res["error"]:
            if "stat_vertex_attr is skip" in res["message"]:
                if not skipNA:
                    ret[vt] = {}
            else:
                raise TigerGraphException(res["message"],
                                            (res["code"] if "code" in res else None))
        else:
            res = res["results"]
            for r in res:
                ret[r["v_type"]] = r["attributes"]

    return ret

def _prep_del_vertices(restppUrl: str, graphname: str, vertexType,
                       where, limit, sort, permanent, timeout):
    '''url builder for delVertices()'''
    url = restppUrl + "/graph/" + graphname + "/vertices/" + vertexType
    isFirst = True
    if where:
        url += "?filter=" + where
        isFirst = False
    if limit and sort:  # These two must be provided together
        url += ("?" if isFirst else "&") + "limit=" + \
            str(limit) + "&sort=" + sort
        isFirst = False
    if permanent:
        url += ("?" if isFirst else "&") + "permanent=true"
        isFirst = False
    if timeout and timeout > 0:
        url += ("?" if isFirst else "&") + "timeout=" + str(timeout)

    return url

def _prep_del_vertices_by_id(restppUrl: str, graphname: str,
                             vertexIds, vertexType, permanent, timeout):
    '''url builder and param parser for delVerticesById()'''
    if not vertexIds:
        raise TigerGraphException("No vertex ID was specified.", None)
    vids = []
    if isinstance(vertexIds, (int, str)):
        vids.append(_safe_char(vertexIds))
    else:
        vids = [_safe_char(f) for f in vertexIds]

    url1 = restppUrl + "/graph/" + \
        graphname + "/vertices/" + vertexType + "/"
    url2 = ""
    if permanent:
        url2 = "?permanent=true"
    if timeout and timeout > 0:
        url2 += ("&" if url2 else "?") + "timeout=" + str(timeout)
    return url1, url2, vids

def _prep_del_vertices_by_type(restppUrl: str,
                               graphname: str,
                               vertexType: str,
                               ack: str,
                               permanent: bool):
    '''url builder for delVerticesByType()'''
    url = restppUrl + "/graph/" + graphname + "/vertices/" + vertexType + "?ack=" + ack.lower()
    if permanent:
        url += "&permanent=true"
    return url

def vertexSetToDataFrame(vertexSet: list, withId: bool = True,
                            withType: bool = False) -> 'pd.DataFrame':
    """Converts a vertex set to Pandas DataFrame.

    Vertex sets are used for both the input and output of `SELECT` statements. They contain
    instances of vertices of the same type.
    For each vertex instance, the vertex ID, the vertex type and the (optional) attributes are
    present under the `v_id`, `v_type` and `attributes` keys, respectively. /
    See an example in `edgeSetToDataFrame()`.

    A vertex set has this structure (when serialised as JSON):
    [source.wrap,json]
    ----
    [
        {
            "v_id": <vertex_id>,
            "v_type": <vertex_type_name>,
            "attributes":
                {
                    "attr1": <value1>,
                    "attr2": <value2>,
                        ⋮
                }
        },
            ⋮
    ]
    ----
    For more information on vertex sets see xref:gsql-ref:querying:declaration-and-assignment-statements.adoc#_vertex_set_variables[Vertex set variables].

    Args:
        vertexSet:
            A JSON array containing a vertex set in the format returned by queries (see below).
        withId:
            Whether to include vertex primary ID as a column.
        withType:
            Whether to include vertex type info as a column.

    Returns:
        A pandas DataFrame containing the vertex attributes (and optionally the vertex primary
        ID and type).
    """
    logger.debug("entry: vertexSetToDataFrame")
    logger.debug("params: " + str(locals()))

    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Pandas is required to use this function. "
                            "Download pandas using 'pip install pandas'.")

    df = pd.DataFrame(vertexSet)
    cols = []
    if withId:
        cols.append(df["v_id"])
    if withType:
        cols.append(df["v_type"])
    cols.append(pd.DataFrame(df["attributes"].tolist()))

    ret = pd.concat(cols, axis=1)

    if logger.level == logging.DEBUG:
        logger.debug("return: " + str(ret))
    logger.debug("exit: vertexSetToDataFrame")

    return ret
