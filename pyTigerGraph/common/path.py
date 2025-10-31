"""Path Finding Functions.

The functions on this page find paths between vertices within the graph.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""

import json
import logging

from typing import Union


logger = logging.getLogger(__name__)


def _prepare_path_params(sourceVertices: Union[dict, tuple, list],
                         targetVertices: Union[dict, tuple, list],
                         maxLength: int = None,
                         vertexFilters: Union[list, dict] = None,
                         edgeFilters: Union[list, dict] = None,
                         allShortestPaths: bool = False) -> str:
    """Prepares the input parameters by transforming them to the format expected by the path
        algorithms.

    See xref:tigergraph-server:API:built-in-endpoints.adoc#[Parameters and output format for path finding]

    A vertex set is a dict that has three top-level keys: `v_type`, `v_id`, `attributes` (also a dictionary).

    Args:
        sourceVertices:
            A vertex set (a list of vertices) or a list of `(vertexType, vertexID)` tuples;
            the source vertices of the shortest paths sought.
        targetVertices:
            A vertex set (a list of vertices) or a list of `(vertexType, vertexID)` tuples;
            the target vertices of the shortest paths sought.
        maxLength:
            The maximum length of a shortest path. Optional, default is `6`.
        vertexFilters:
            An optional list of `(vertexType, condition)` tuples or
            `{"type": <str>, "condition": <str>}` dictionaries.
        edgeFilters:
            An optional list of `(edgeType, condition)` tuples or
            `{"type": <str>, "condition": <str>}` dictionaries.
        allShortestPaths:
            If `True`, the endpoint will return all shortest paths between the source and target.
            Default is `False`, meaning that the endpoint will return only one path.

    Returns:
        A string representation of the dictionary of end-point parameters.
    """

    def parse_vertices(vertices: Union[dict, tuple, list]) -> list:
        """Parses vertex input parameters and converts it to the format required by the path
        finding endpoints.

        Args:
            vertices:
                A vertex set (a list of vertices) or a list of `(vertexType, vertexID)` tuples;
                the source or target vertices of the shortest paths sought.
        Returns:
            A list of vertices in the format required by the path finding endpoints.
        """
        logger.debug("entry: parseVertices")
        logger.debug("params: " + str(locals))

        ret = []
        if not isinstance(vertices, list):
            vertices = [vertices]
        for v in vertices:
            if isinstance(v, tuple):
                tmp = {"type": v[0], "id": v[1]}
                ret.append(tmp)
            elif isinstance(v, dict) and "v_type" in v and "v_id" in v:
                tmp = {"type": v["v_type"], "id": v["v_id"]}
                ret.append(tmp)
            else:
                logger.warning("Invalid vertex type or value: " + str(v))

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: parseVertices")

        return ret

    def parse_filters(filters: Union[dict, tuple, list]) -> list:
        """Parses filter input parameters and converts it to the format required by the path
        finding endpoints.

        Args:
            filters:
                A list of `(vertexType, condition)` tuples or
                `{"type": <str>, "condition": <str>}` dictionaries.

        Returns:
            A list of filters in the format required by the path finding endpoints.
        """
        logger.debug("entry: parseFilters")
        logger.debug("params: " + str(locals()))

        ret = []
        if not isinstance(filters, list):
            filters = [filters]
        for f in filters:
            if isinstance(f, tuple):
                tmp = {"type": f[0], "condition": f[1]}
                ret.append(tmp)
            elif isinstance(f, dict) and "type" in f and "condition" in f:
                tmp = {"type": f["type"], "condition": f["condition"]}
                ret.append(tmp)
            else:
                logger.warning("Invalid filter type or value: " + str(f))

        logger.debug("return: " + str(ret))
        logger.debug("exit: parseFilters")

        return ret

    logger.debug("entry: _preparePathParams")
    logger.debug("params: " + str(locals()))

    # Assembling the input payload
    if not sourceVertices or not targetVertices:
        return ""
        # TODO Should allow returning error instead of handling missing parameters here?
    data = {"sources": parse_vertices(
        sourceVertices), "targets": parse_vertices(targetVertices)}
    if vertexFilters:
        data["vertexFilters"] = parse_filters(vertexFilters)
    if edgeFilters:
        data["edgeFilters"] = parse_filters(edgeFilters)
    if maxLength:
        data["maxLength"] = maxLength
    if allShortestPaths:
        data["allShortestPaths"] = True

    ret = json.dumps(data)

    logger.debug("return: " + str(ret))
    logger.debug("exit: _preparePathParams")

    return ret
