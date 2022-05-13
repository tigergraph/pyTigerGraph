"""Path Finding Functions.

The functions on this page find paths between vertices within the graph.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object]. 
"""

import json
from typing import Union

from pyTigerGraph.pyTigerGraphBase import pyTigerGraphBase


class pyTigerGraphPath(pyTigerGraphBase):
    def _preparePathParams(self, sourceVertices: Union[dict, tuple, list],
            targetVertices: Union[dict, tuple, list], maxLength: int = None,
            vertexFilters: Union[list, dict] = None, edgeFilters: Union[list, dict] = None,
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

        def parseVertices(vertices: Union[dict, tuple, list]) -> list:
            """Parses vertex input parameters and converts it to the format required by the path
            finding endpoints.

            Args:
                vertices:
                    A vertex set (a list of vertices) or a list of `(vertexType, vertexID)` tuples;
                    the source or target vertices of the shortest paths sought.
            Returns:
                A list of vertices in the format required by the path finding endpoints.
            """
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
                elif self.debug:
                    print("Invalid vertex type or value: " + str(v))
                    # TODO Proper logging
            return ret

        def parseFilters(filters: list) -> list:
            """Parses filter input parameters and converts it to the format required by the path
            finding endpoints.

            Args:
                filters:
                    A list of `(vertexType, condition)` tuples or `{"type": <str>, "condition": <str>}`
                    dictionaries.

            Returns:
                A list of filters in the format required by the path finding endpoints.
            """
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
                elif self.debug:
                    print("Invalid filter type or value: " + str(f))
                    # TODO Proper logging
            return ret

        # Assembling the input payload
        if not sourceVertices or not targetVertices:
            return ""
            # TODO Should allow returning error instead of handling missing parameters here?
        data = {"sources": parseVertices(sourceVertices), "targets": parseVertices(targetVertices)}
        if vertexFilters:
            data["vertexFilters"] = parseFilters(vertexFilters)
        if edgeFilters:
            data["edgeFilters"] = parseFilters(edgeFilters)
        if maxLength:
            data["maxLength"] = maxLength
        if allShortestPaths:
            data["allShortestPaths"] = True

        return json.dumps(data)

    def shortestPath(self, sourceVertices: Union[dict, tuple, list],
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
        data = self._preparePathParams(sourceVertices, targetVertices, maxLength, vertexFilters,
            edgeFilters, allShortestPaths)
        return self._post(self.restppUrl + "/shortestpath/" + self.graphname, data=data)

    def allPaths(self, sourceVertices: Union[dict, tuple, list],
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
        data = self._preparePathParams(sourceVertices, targetVertices, maxLength, vertexFilters,
            edgeFilters)
        return self._post(self.restppUrl + "/allpaths/" + self.graphname, data=data)
