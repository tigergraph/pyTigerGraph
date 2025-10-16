"""Path Finding Functions.

The functions on this page find paths between vertices within the graph.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""

import logging
from typing import Union

from pyTigerGraph.common.path import _prepare_path_params
from pyTigerGraph.pytgasync.pyTigerGraphBase import AsyncPyTigerGraphBase

logger = logging.getLogger(__name__)


class AsyncPyTigerGraphPath(AsyncPyTigerGraphBase):

    async def shortestPath(self, sourceVertices: Union[dict, tuple, list],
                           targetVertices: Union[dict, tuple, list], maxLength: int = None,
                           vertexFilters: Union[list, dict] = None, edgeFilters: Union[list, dict] = None,
                           allShortestPaths: bool = False) -> dict:
        """Finds the shortest path (or all shortest paths) between the source and target vertex sets.

        A vertex set is a set of dictionaries that each has three top-level keys: `v_type`, `v_id`,
            and `attributes` (also a dictionary).

        Args:
            sourceVertices:
                A vertex set (a list of vertices) or a list of `(vertexType, vertexID)` tuples;
                the source vertices of the shortest paths sought.
            targetVertices:
                A vertex set (a list of vertices) or a list of `(vertexType, vertexID)` tuples;
                the target vertices of the shortest paths sought.
            maxLength:
                The maximum length of a shortest path. Optional, default is 6.
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
            The shortest path between the source and the target.
            The returned value is a subgraph: all vertices and edges that are part of the path(s);
            i.e. not a (list of individual) path(s).

        Examples:

            [source.wrap,python]
            ----
            path = conn.shortestPath(("account", 10), ("person", 50), maxLength=3)

            path = conn.shortestPath(("account", 10), ("person", 50), allShortestPaths=True,
                vertexFilters=("transfer", "amount>950"), edgeFilters=("receive", "type=4"))
            ----

        Endpoint:
            - `POST /shortestpath/{graphName}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_find_shortest_path[Find the shortest path].
        """
        logger.debug("entry: shortestPath")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        data = _prepare_path_params(sourceVertices, targetVertices, maxLength, vertexFilters,
                                         edgeFilters, allShortestPaths)
        ret = await self._post(self.restppUrl + "/shortestpath/" + self.graphname, data=data)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: shortestPath")

        return ret

    async def allPaths(self, sourceVertices: Union[dict, tuple, list],
                       targetVertices: Union[dict, tuple, list], maxLength: int,
                       vertexFilters: Union[list, dict] = None, edgeFilters: Union[list, dict] = None) -> dict:
        """Find all possible paths up to a given maximum path length between the source and target
        vertex sets.

        A vertex set is a dict that has three top-level keys: v_type, v_id, attributes (a dict).

        Args:
            sourceVertices:
                A vertex set (a list of vertices) or a list of `(vertexType, vertexID)` tuples;
                the source vertices of the shortest paths sought.
            targetVertices:
                A vertex set (a list of vertices) or a list of `(vertexType, vertexID)` tuples;
                the target vertices of the shortest paths sought.
            maxLength:
                The maximum length of the paths.
            vertexFilters:
                An optional list of `(vertexType, condition)` tuples or
                `{"type": <str>, "condition": <str>}` dictionaries.
            edgeFilters:
                An optional list of `(edgeType, condition)` tuples or
                `{"type": <str>, "condition": <str>}` dictionaries.

        Returns:
            All paths between a source vertex (or vertex set) and target vertex (or vertex set).
            The returned value is a subgraph: all vertices and edges that are part of the path(s);
            i.e. not a (list of individual) path(s).

        Example:
            [source.wrap, python]
            ----
            path = conn.allPaths(("account", 10), ("person", 50), allShortestPaths=True,
                vertexFilters=("transfer", "amount>950"), edgeFilters=("receive", "type=4"))
            ----


        Endpoint:
            - `POST /allpaths/{graphName}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_find_all_paths[Find all paths]
        """
        logger.debug("entry: allPaths")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        data = _prepare_path_params(sourceVertices, targetVertices, maxLength, vertexFilters,
                                         edgeFilters)
        ret = await self._post(self.restppUrl + "/allpaths/" + self.graphname, data=data)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: allPaths")

        return ret
