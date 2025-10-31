import json
import logging

from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    import pandas as pd

from pyTigerGraph.common.exception import TigerGraphException
from pyTigerGraph.common.util import (
    _safe_char
)
from pyTigerGraph.common.schema import (
    _upsert_attrs
)

logger = logging.getLogger(__name__)

___trgvtxids = "___trgvtxids"

def _parse_get_edge_source_vertex_type(edgeTypeDetails):
    # Edge type with a single source vertex type
    if edgeTypeDetails["FromVertexTypeName"] != "*":
        ret = edgeTypeDetails["FromVertexTypeName"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getEdgeSourceVertexType (single source)")

        return ret

    # Edge type with multiple source vertex types
    if "EdgePairs" in edgeTypeDetails:
        # v3.0 and later notation
        vts = set()
        for ep in edgeTypeDetails["EdgePairs"]:
            vts.add(ep["From"])

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(vts))
        logger.debug("exit: getEdgeSourceVertexType (multi source)")

        return vts
    else:
        # 2.6.1 and earlier notation
        if logger.level == logging.DEBUG:
            logger.debug("return: *")
        logger.info(
            "exit: getEdgeSourceVertexType (multi source, pre-3.x)")

        return "*"

def _parse_get_edge_target_vertex_type(edgeTypeDetails):
    # Edge type with a single target vertex type
    if edgeTypeDetails["ToVertexTypeName"] != "*":
        ret = edgeTypeDetails["ToVertexTypeName"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getEdgeTargetVertexType (single target)")

        return ret

    # Edge type with multiple target vertex types
    if "EdgePairs" in edgeTypeDetails:
        # v3.0 and later notation
        vts = set()
        for ep in edgeTypeDetails["EdgePairs"]:
            vts.add(ep["To"])

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(vts))
        logger.debug("exit: getEdgeTargetVertexType (multi target)")

        return vts
    else:
        # 2.6.1 and earlier notation
        if logger.level == logging.DEBUG:
            logger.debug("return: *")
        logger.info(
            "exit: getEdgeTargetVertexType (multi target, pre-3.x)")

        return "*"

def _prep_get_edge_count_from(restppUrl: str,
                              graphname: str,
                              sourceVertexType: str = "",
                              sourceVertexId: Union[str, int] = None,
                              edgeType: str = "",
                              targetVertexType: str = "",
                              targetVertexId: Union[str, int] = None,
                              where: str = ""):
    data = None
    # If WHERE condition is not specified, use /builtins else user /vertices
    if where or (sourceVertexType and sourceVertexId):
        if not sourceVertexType or not sourceVertexId:
            raise TigerGraphException(
                "If where condition is specified, then both sourceVertexType and sourceVertexId"
                " must be provided too.", None)
        url = restppUrl + "/graph/" + _safe_char(graphname) + "/edges/" + \
            _safe_char(sourceVertexType) + "/" + \
            _safe_char(sourceVertexId)
        if edgeType:
            url += "/" + _safe_char(edgeType)
            if targetVertexType:
                url += "/" + _safe_char(targetVertexType)
                if targetVertexId:
                    url += "/" + _safe_char(targetVertexId)
        url += "?count_only=true"
        if where:
            url += "&filter=" + _safe_char(where)
    else:
        if not edgeType:  # TODO Is this a valid check?
            raise TigerGraphException(
                "A valid edge type or \"*\" must be specified for edge type.", None)
        data = '{"function":"stat_edge_number","type":"' + edgeType + '"' \
                + (',"from_type":"' + sourceVertexType + '"' if sourceVertexType else '') \
                + (',"to_type":"' + targetVertexType + '"' if targetVertexType else '') \
                + '}'
        url = restppUrl + "/builtins/" + graphname
    return url, data

def _parse_get_edge_count_from(res, edgeType):
    if len(res) == 1 and res[0]["e_type"] == edgeType:
        ret = res[0]["count"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getEdgeCountFrom (single edge type)")

        return ret

    ret = {}
    for r in res:
        ret[r["e_type"]] = r["count"]
    return ret

def _prep_upsert_edge(sourceVertexType: str,
                      sourceVertexId: str,
                      edgeType: str,
                      targetVertexType: str,
                      targetVertexId: str,
                      attributes: dict = None):
    '''defining edge schema structure for upsertEdge()'''
    if not(attributes):
        attributes = {}

    vals = _upsert_attrs(attributes)
    data = json.dumps({
        "edges": {
            sourceVertexType: {
                sourceVertexId: {
                    edgeType: {
                        targetVertexType: {
                            targetVertexId: vals
                        }
                    }
                }
            }
        }
    })
    return data

def _dumps(data) -> str:
    """Generates the JSON format expected by the endpoint (Used in upsertEdges()).

    The important thing this function does is converting the list of target vertex IDs and
    the attributes belonging to the edge instances into a JSON object that can contain
    multiple occurrences of the same key. If the these details were stored in a dictionary
    then in case of MultiEdge only the last instance would be retained (as the key would be
    the target vertex ID).

    Args:
        data:
            The Python data structure containing the edge instance details.

    Returns:
        The JSON to be sent to the endpoint.
    """
    ret = ""
    if isinstance(data, dict):
        c1 = 0
        for k1, v1 in data.items():
            if c1 > 0:
                ret += ","
            if k1 == ___trgvtxids:
                # Dealing with the (possibly multiple instances of) edge details
                # v1 should be a dict of lists
                c2 = 0
                for k2, v2 in v1.items():
                    if c2 > 0:
                        ret += ","
                    c3 = 0
                    for v3 in v2:
                        if c3 > 0:
                            ret += ","
                        ret += json.dumps(k2) + ':' + json.dumps(v3)
                        c3 += 1
                    c2 += 1
            else:
                ret += json.dumps(k1) + ':' + _dumps(data[k1])
            c1 += 1
    return "{" + ret + "}"

def _prep_upsert_edges(sourceVertexType,
                       edgeType,
                       targetVertexType,
                       edges):
    '''converting vertex parameters into edge structure'''
    data = {sourceVertexType: {}}
    l1 = data[sourceVertexType]
    for e in edges:
        if len(e) > 2:
            vals = _upsert_attrs(e[2])
        else:
            vals = {}
        # sourceVertexId
        # Converted to string as the key in the JSON payload must be a string
        sourceVertexId = str(e[0])
        if sourceVertexId not in l1:
            l1[sourceVertexId] = {}
        l2 = l1[sourceVertexId]
        # edgeType
        if edgeType not in l2:
            l2[edgeType] = {}
        l3 = l2[edgeType]
        # targetVertexType
        if targetVertexType not in l3:
            l3[targetVertexType] = {}
        l4 = l3[targetVertexType]
        if ___trgvtxids not in l4:
            l4[___trgvtxids] = {}
        l4 = l4[___trgvtxids]
        # targetVertexId
        # Converted to string as the key in the JSON payload must be a string
        targetVertexId = str(e[1])
        if targetVertexId not in l4:
            l4[targetVertexId] = []
        l4[targetVertexId].append(vals)

    data = _dumps({"edges": data})
    return data

def _prep_upsert_edge_dataframe(df, from_id, to_id, attributes):
    '''converting dataframe into an upsertable object structure'''
    json_up = []

    for index in df.index:
        json_up.append(json.loads(df.loc[index].to_json()))
        json_up[-1] = (
            index if from_id is None else json_up[-1][from_id],
            index if to_id is None else json_up[-1][to_id],
            json_up[-1] if attributes is None
            else {target: json_up[-1][source] for target, source in attributes.items()}
        )
    return json_up

def _prep_get_edges(restppUrl: str,
                    graphname: str,
                    sourceVertexType: str,
                    sourceVertexId: str,
                    edgeType: str = "",
                    targetVertexType: str = "",
                    targetVertexId: str = "",
                    select: str = "",
                    where: str = "",
                    limit: Union[int, str] = None,
                    sort: str = "",
                    timeout: int = 0):
    '''url builder for getEdges()'''
    # TODO Change sourceVertexId to sourceVertexIds and allow passing both str and list<str> as
    #   parameter
    if not sourceVertexType or not sourceVertexId:
        raise TigerGraphException(
            "Both source vertex type and source vertex ID must be provided.", None)
    url = restppUrl + "/graph/" + graphname + "/edges/" + sourceVertexType + "/" + \
        str(sourceVertexId)
    if edgeType:
        url += "/" + edgeType
        if targetVertexType:
            url += "/" + targetVertexType
            if targetVertexId:
                url += "/" + str(targetVertexId)
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

def _prep_get_edges_by_type(graphname,
                            sourceVertexType,
                            edgeType):
    '''build the query to select edges for getEdgesByType()'''
    # TODO Support edges with multiple source vertex types
    if isinstance(sourceVertexType, set) or sourceVertexType == "*":
        raise TigerGraphException(
            "Edges with multiple source vertex types are not currently supported.", None)

    queryText = \
        'INTERPRET QUERY () FOR GRAPH $graph { \
        SetAccum<EDGE> @@edges; \
        start = {ANY}; \
        res = \
            SELECT s \
            FROM   start:s-(:e)->ANY:t \
            WHERE  e.type == "$edgeType" \
                AND s.type == "$sourceEdgeType" \
            ACCUM  @@edges += e; \
        PRINT @@edges AS edges; \
    }'

    queryText = queryText.replace("$graph", graphname) \
        .replace('$sourceEdgeType', sourceVertexType) \
        .replace('$edgeType', edgeType)
    return queryText

def _parse_get_edge_stats(responses, skipNA):
    '''error checking and parsing responses for getEdgeStats()'''
    ret = {}
    for et, res in responses:
        if res["error"]:
            if "stat_edge_attr is skip" in res["message"] or \
                    "No valid edge for the input edge type" in res["message"]:
                if not skipNA:
                    ret[et] = {}
            else:
                raise TigerGraphException(res["message"],
                                            (res["code"] if "code" in res else None))
        else:
            res = res["results"]
            for r in res:
                ret[r["e_type"]] = r["attributes"]
    return ret

def _prep_del_edges(restppUrl: str,
                    graphname: str,
                    sourceVertexType,
                    sourceVertexId,
                    edgeType,
                    targetVertexType,
                    targetVertexId,
                    where,
                    limit,
                    sort,
                    timeout):
    '''url building for delEdges()'''
    if not sourceVertexType or not sourceVertexId:
        raise TigerGraphException("Both sourceVertexType and sourceVertexId must be provided.",
                                    None)

    url = restppUrl + "/graph/" + graphname + "/edges/" + sourceVertexType + "/" + str(
        sourceVertexId)

    if edgeType:
        url += "/" + edgeType
        if targetVertexType:
            url += "/" + targetVertexType
            if targetVertexId:
                url += "/" + str(targetVertexId)

    isFirst = True
    if where:
        url += ("?" if isFirst else "&") + "filter=" + where
        isFirst = False
    if limit and sort:  # These two must be provided together
        url += ("?" if isFirst else "&") + "limit=" + \
            str(limit) + "&sort=" + sort
        isFirst = False
    if timeout and timeout > 0:
        url += ("?" if isFirst else "&") + "timeout=" + str(timeout)
    return url

def edgeSetToDataFrame(edgeSet: list,
                       withId: bool = True,
                       withType: bool = False) -> 'pd.DataFrame':
    """Converts an edge set to Pandas DataFrame

    Edge sets contain instances of the same edge type. Edge sets are not generated "naturally"
    like vertex sets. Instead, you need to collect edges in (global) accumulators, like when you
    want to visualize them in GraphStudio or by other tools.

    For example:
    ```
    SetAccum<EDGE> @@edges;

    start = {country.*};

    result =
        SELECT trg
        FROM   start:src -(city_in_country:e)- city:trg
        ACCUM  @@edges += e;

    PRINT start, result, @@edges;
    ```

    The `@@edges` is an edge set.
    It contains, for each edge instance, the source and target vertex type and ID, the edge type,
    a directedness indicator and the (optional) attributes. /

    [NOTE]
    `start` and `result` are vertex sets.

    An edge set has this structure (when serialised as JSON):

    [source.wrap, json]
    ----
    [
        {
            "e_type": <edge_type_name>,
            "from_type": <source_vertex_type_name>,
            "from_id": <source_vertex_id>,
            "to_type": <target_vertex_type_name>,
            "to_id": <targe_vertex_id>,
            "directed": <true_or_false>,
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

    Args:
        edgeSet:
            A JSON array containing an edge set in the format returned by queries (see below).
        withId:
            Whether to include the type and primary ID of source and target vertices as a column. Default is `True`.
        withType:
            Whether to include edge type info as a column. Default is `False`.

    Returns:
        A pandas DataFrame containing the edge attributes and optionally the type and primary
        ID or source and target vertices, and the edge type.

    """
    logger.debug("entry: edgeSetToDataFrame")
    logger.debug("params: " + str(locals()))

    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Pandas is required to use this function. "
                            "Download pandas using 'pip install pandas'.")

    df = pd.DataFrame(edgeSet)
    cols = []
    if withId:
        cols.extend([df["from_type"], df["from_id"],
                    df["to_type"], df["to_id"]])
    if withType:
        cols.append(df["e_type"])
    cols.append(pd.DataFrame(df["attributes"].tolist()))

    ret = pd.concat(cols, axis=1)

    if logger.level == logging.DEBUG:
        logger.debug("return: " + str(ret))
    logger.debug("exit: edgeSetToDataFrame")

    return ret
