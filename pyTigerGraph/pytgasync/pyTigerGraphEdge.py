"""Edge Functions

Functions to upsert, retrieve and delete edges.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import json
import logging
import warnings

from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    import pandas as pd

from pyTigerGraph.common.edge import (
    _parse_get_edge_source_vertex_type,
    _parse_get_edge_target_vertex_type,
    _prep_get_edge_count_from,
    _parse_get_edge_count_from,
    _prep_upsert_edge,
    _dumps,
    _prep_upsert_edges,
    _prep_upsert_edge_dataframe,
    _prep_get_edges,
    _prep_get_edges_by_type,
    _parse_get_edge_stats,
    _prep_del_edges
)

from pyTigerGraph.common.edge import edgeSetToDataFrame as _eS2DF

from pyTigerGraph.common.schema import (
    _get_attr_type,
    _upsert_attrs
)

from pyTigerGraph.pytgasync.pyTigerGraphQuery import AsyncPyTigerGraphQuery

logger = logging.getLogger(__name__)


class AsyncPyTigerGraphEdge(AsyncPyTigerGraphQuery):

    ___trgvtxids = "___trgvtxids"

    async def getEdgeTypes(self, force: bool = False) -> list:
        """Returns the list of edge type names of the graph.

        Args:
            force:
                If `True`, forces the retrieval the schema metadata again, otherwise returns a
                cached copy of edge type metadata (if they were already fetched previously).

        Returns:
            The list of edge types defined in the current graph.
        """
        logger.debug("entry: getEdgeTypes")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = []
        schema = await self.getSchema(force=force)
        for et in schema["EdgeTypes"]:
            ret.append(et["Name"])

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getEdgeTypes")

        return ret

    async def getEdgeType(self, edgeType: str, force: bool = False) -> dict:
        """Returns the details of the edge type.

        Args:
            edgeType:
                The name of the edge type.
            force:
                If `True`, forces the retrieval the schema details again, otherwise returns a cached
                copy of edge type details (if they were already fetched previously).

        Returns:
            The metadata of the edge type.
        """
        logger.debug("entry: getEdgeType")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        schema = await self.getSchema(force=force)
        for et in schema["EdgeTypes"]:
            if et["Name"] == edgeType:
                if logger.level == logging.DEBUG:
                    logger.debug("return: " + str(et))
                logger.debug("exit: getEdgeType (found)")

                return et

        logger.warning("Edge type `" + edgeType + "` was not found.")
        logger.debug("exit: getEdgeType (not found)")

        return {}

    async def getEdgeAttrs(self, edgeType: str) -> list:
        """Returns the names and types of the attributes of the edge type.

        Args:
            edgeType:
                The name of the edge type.

        Returns:
            A list of (attribute_name, attribute_type) tuples.
            The format of attribute_type is one of
             - "scalar_type"
             - "complex_type(scalar_type)"
             - "map_type(key_type,value_type)"
            and it is a string.
        """
        logger.debug("entry: getAttributes")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        et = await self.getEdgeType(edgeType)
        ret = []

        for at in et["Attributes"]:
            ret.append(
                (at["AttributeName"], _get_attr_type(at["AttributeType"])))

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getAttributes")

        return ret

    async def getEdgeSourceVertexType(self, edgeType: str) -> Union[str, set]:
        """Returns the type(s) of the edge type's source vertex.

        Args:
            edgeType:
                The name of the edge type.

        Returns:
            - A single source vertex type name string if the edge has a single source vertex type.
            - "*" if the edge can originate from any vertex type (notation used in 2.6.1 and earlier
                versions).
                See https://docs.tigergraph.com/v/2.6/dev/gsql-ref/ddl-and-loading/defining-a-graph-schema#creating-an-edge-from-or-to-any-vertex-type
            - A set of vertex type name strings (unique values) if the edge has multiple source
                vertex types (notation used in 3.0 and later versions). /
                Even if the source vertex types were defined as `"*"`, the REST API will list them as
                pairs (i.e. not as `"*"` in 2.6.1 and earlier versions), just like as if there were
                defined one by one (e.g. `FROM v1, TO v2 | FROM v3, TO v4 | …`).

            The returned set contains all source vertex types, but it does not certainly mean that
                the edge is defined between all source and all target vertex types. You need to look
                at the individual source/target pairs to find out which combinations are
                valid/defined.
        """
        logger.debug("entry: getEdgeSourceVertexType")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        edgeTypeDetails = await self.getEdgeType(edgeType)
        res = _parse_get_edge_source_vertex_type(edgeTypeDetails)
        return res

    async def getEdgeTargetVertexType(self, edgeType: str) -> Union[str, set]:
        """Returns the type(s) of the edge type's target vertex.

        Args:
            edgeType:
                The name of the edge type.

        Returns:
            - A single target vertex type name string if the edge has a single target vertex type.
            - "*" if the edge can end in any vertex type (notation used in 2.6.1 and earlier
                versions).
                See https://docs.tigergraph.com/v/2.6/dev/gsql-ref/ddl-and-loading/defining-a-graph-schema#creating-an-edge-from-or-to-any-vertex-type
            - A set of vertex type name strings (unique values) if the edge has multiple target
                vertex types (notation used in 3.0 and later versions). /
                Even if the target vertex types were defined as "*", the REST API will list them as
                pairs (i.e. not as "*" in 2.6.1 and earlier versions), just like as if there were
                defined one by one (e.g. `FROM v1, TO v2 | FROM v3, TO v4 | …`).

            The returned set contains all target vertex types, but does not certainly mean that the
                edge is defined between all source and all target vertex types. You need to look at
                the individual source/target pairs to find out which combinations are valid/defined.
        """
        logger.debug("entry: getEdgeTargetVertexType")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        edgeTypeDetails = await self.getEdgeType(edgeType)
        ret = _parse_get_edge_target_vertex_type(edgeTypeDetails)
        return ret

    async def isDirected(self, edgeType: str) -> bool:
        """Is the specified edge type directed?

        Args:
            edgeType:
                The name of the edge type.

        Returns:
            `True`, if the edge is directed.
        """
        logger.debug("entry: isDirected")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = await self.getEdgeType(edgeType)
        ret = ret["IsDirected"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: isDirected")

        return ret

    async def getReverseEdge(self, edgeType: str) -> str:
        """Returns the name of the reverse edge of the specified edge type, if applicable.

        Args:
            edgeType:
                The name of the edge type.

        Returns:
            The name of the reverse edge, if it was defined.
        """
        logger.debug("entry: getReverseEdge")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if not await self.isDirected(edgeType):
            logger.error(edgeType + " is not a directed edge")
            logger.debug("exit: getReverseEdge (not directed)")

            return ""

        config = await self.getEdgeType(edgeType)
        config = config["Config"]
        if "REVERSE_EDGE" in config:
            ret = config["REVERSE_EDGE"]

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(ret))
            logger.debug("exit: getReverseEdge (reverse edge found)")

            return ret

        logger.debug("exit: getReverseEdge (reverse edge not found)")

        return ""
        # TODO Should return some other value or raise exception?

    async def isMultiEdge(self, edgeType: str) -> bool:
        """Can the edge have multiple instances between the same pair of vertices?

        Args:
            edgeType:
                The name of the edge type.

        Returns:
            `True`, if the edge can have multiple instances between the same pair of vertices.
        """
        logger.debug("entry: isMultiEdge")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        et = await self.getEdgeType(edgeType)
        ret = ("DiscriminatorCount" in et) and et["DiscriminatorCount"] > 0

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: isMultiEdge")

        return ret

    async def getDiscriminators(self, edgeType: str) -> list:
        """Returns the names and types of the discriminators of the edge type.

        Args:
            edgeType:
                The name of the edge type.

        Returns:
            A list of (attribute_name, attribute_type) tuples.
        """
        logger.debug("entry: getDiscriminators")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        et = await self.getEdgeType(edgeType)
        ret = []

        for at in et["Attributes"]:
            if "IsDiscriminator" in at and at["IsDiscriminator"]:
                ret.append(
                    (at["AttributeName"], _get_attr_type(at["AttributeType"])))

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getDiscriminators")

        return ret

    async def getEdgeCountFrom(self, sourceVertexType: str = "", sourceVertexId: Union[str, int] = None,
                               edgeType: str = "", targetVertexType: str = "", targetVertexId: Union[str, int] = None,
                               where: str = "") -> dict:
        """Returns the number of edges from a specific vertex.

        Args:
            sourceVertexType:
                The name of the source vertex type.
            sourceVertexId:
                The primary ID value of the source vertex instance.
            edgeType:
                The name of the edge type.
            targetVertexType:
                The name of the target vertex type.
            targetVertexId:
                The primary ID value of the target vertex instance.
            where:
                A comma separated list of conditions that are all applied on each edge's attributes.
                The conditions are in logical conjunction (i.e. they are "AND'ed" together).

        Returns:
            A dictionary of `edge_type: edge_count` pairs.

        Uses:
            - If `edgeType` = "*": edge count of all edge types (no other arguments can be specified
                in this case).
            - If `edgeType` is specified only: edge count of the given edge type.
            - If `sourceVertexType`, `edgeType`, `targetVertexType` are specified: edge count of the
                given edge type between source and target vertex types.
            - If `sourceVertexType`, `sourceVertexId` are specified: edge count of all edge types
                from the given vertex instance.
            - If `sourceVertexType`, `sourceVertexId`, `edgeType` are specified: edge count of all
                edge types from the given vertex instance.
            - If `sourceVertexType`, `sourceVertexId`, `edgeType`, `where` are specified: the edge
                count of the given edge type after filtered by `where` condition.
            - If `targetVertexId` is specified, then `targetVertexType` must also be specified.
            - If `targetVertexType` is specified, then `edgeType` must also be specified.

        Endpoints:
            - `GET /graph/{graph_name}/edges/{source_vertex_type}/{source_vertex_id}`
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_list_edges_of_a_vertex
            - `POST /builtins/{graph_name}`
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_run_built_in_functions_on_graph
        """
        logger.debug("entry: getEdgeCountFrom")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        url, data = _prep_get_edge_count_from(restppUrl=self.restppUrl,
                                              graphname=self.graphname,
                                              sourceVertexType=sourceVertexType,
                                              sourceVertexId=sourceVertexId,
                                              edgeType=edgeType,
                                              targetVertexType=targetVertexType,
                                              targetVertexId=targetVertexId,
                                              where=where)
        if data:
            res = await self._req("POST", url, data=data)
        else:
            res = await self._req("GET", url)
        ret = _parse_get_edge_count_from(res, edgeType)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getEdgeCountFrom  (multiple edge types)")

        return ret

    async def getEdgeCount(self, edgeType: str = "*", sourceVertexType: str = "",
                           targetVertexType: str = "") -> dict:
        """Returns the number of edges of an edge type.

        This is a simplified version of `getEdgeCountFrom()`, to be used when the total number of
        edges of a given type is needed, regardless which vertex instance they are originated from.
        See documentation of `getEdgeCountFrom` above for more details.

        Args:
            edgeType:
                The name of the edge type.
            sourceVertexType:
                The name of the source vertex type.
            targetVertexType:
                The name of the target vertex type.

        Returns:
            A dictionary of `edge_type: edge_count` pairs.
        """
        logger.debug("entry: getEdgeCount")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = await self.getEdgeCountFrom(edgeType=edgeType, sourceVertexType=sourceVertexType,
                                          targetVertexType=targetVertexType)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getEdgeCount")

        return ret

    async def upsertEdge(self, sourceVertexType: str, sourceVertexId: str, edgeType: str,
                         targetVertexType: str, targetVertexId: str, attributes: dict = None) -> int:
        """Upserts an edge.

        Data is upserted:

        - If edge is not yet present in graph, it will be created (see special case below).
        - If it's already in the graph, it is updated with the values specified in the request.
        - If `vertex_must_exist` is True then edge will only be created if both vertex exists
            in graph. Otherwise missing vertices are created with the new edge; the newly created
            vertices' attributes (if any) will be created with default values.

        Args:
            sourceVertexType:
                The name of the source vertex type.
            sourceVertexId:
                The primary ID value of the source vertex instance.
            edgeType:
                The name of the edge type.
            targetVertexType:
                The name of the target vertex type.
            targetVertexId:
                The primary ID value of the target vertex instance.
            attributes:
                A dictionary in this format:
                ```
                {<attribute_name>, <attribute_value>|(<attribute_name>, <operator>), …}
                ```
                Example:
                ```
                {"visits": (1482, "+"), "max_duration": (371, "max")}
                ```
                For valid values of `<operator>` see https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#operation-codes.

        Returns:
            A single number of accepted (successfully upserted) edges (0 or 1).

        Endpoint:
            - `POST /graph/{graph_name}`
                See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#upsert-data-to-graph

        TODO Add ack, new_vertex_only, vertex_must_exist, update_vertex_only and atomic_level
            parameters and functionality.
        """
        logger.debug("entry: upsertEdge")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        data = _prep_upsert_edge(sourceVertexType,
                                 sourceVertexId,
                                 edgeType,
                                 targetVertexType,
                                 targetVertexId,
                                 attributes)
        ret = await self._req("POST", self.restppUrl + "/graph/" + self.graphname, data=data)
        ret = ret[0]["accepted_edges"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: upsertEdge")

        return ret

    async def upsertEdges(self, sourceVertexType: str, edgeType: str, targetVertexType: str,
                          edges: list, atomic: bool = False) -> int:
        """Upserts multiple edges (of the same type).

        Args:
            sourceVertexType:
                The name of the source vertex type.
            edgeType:
                The name of the edge type.
            targetVertexType:
                The name of the target vertex type.
            edges:
                A list in of tuples in this format:
                ```
                [
                    (<source_vertex_id>, <target_vertex_id>, {<attribute_name>: <attribute_value>, …}),
                    (<source_vertex_id>, <target_vertex_id>, {<attribute_name>: (<attribute_value>, <operator>), …})
                    ⋮
                ]
                ```
                Example:
                ```
                [
                    (17, "home_page", {"visits": (35, "+"), "max_duration": (93, "max")}),
                    (42, "search", {"visits": (17, "+"), "max_duration": (41, "max")})
                ]
                ```
                For valid values of `<operator>` see https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#operation-codes .
            atomic:
                The request is an atomic transaction. An atomic transaction means that updates to
                the database contained in the request are all-or-nothing: either all changes are
                successful, or none are successful. This uses the `gsql-atomic-level` header, and sets
                the value to `atomic` if `True`, and `nonatomic` if `False`. Default is `False`.

        Returns:
            A single number of accepted (successfully upserted) edges (0 or positive integer).

        Endpoint:
            - `POST /graph/{graph_name}`
                See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#upsert-data-to-graph

        TODO Add ack, new_vertex_only, vertex_must_exist, update_vertex_only and atomic_level
            parameters and functionality.
        """

        logger.debug("entry: upsertEdges")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        """
            NOTE: The source and target vertex primary IDs are converted below to string as the keys
            in a JSON document must be string.
            This probably should not be an issue as the primary ID has a predefined data type, so if
            the same primary ID is sent as two different literal (say: 1 as number and "1" as
            string), it will be converted anyhow to the same (numerical or string) data type.
            Converting the primary IDs to string here prevents inconsistencies as Python dict would
            otherwise handle 1 and "1" as two separate keys.
        """

        data = _prep_upsert_edges(sourceVertexType=sourceVertexType,
                                 edgeType=edgeType,
                                 targetVertexType=targetVertexType,
                                 edges=edges)

        headers = {}
        if atomic:
            headers["gsql-atomic-level"] = "atomic"

        ret = await self._req("POST", self.restppUrl + "/graph/" + self.graphname, data=data)
        ret = ret[0]["accepted_edges"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: upsertEdges")

        return ret

    async def upsertEdgeDataFrame(self, df: 'pd.DataFrame', sourceVertexType: str, edgeType: str,
                                  targetVertexType: str, from_id: str = "", to_id: str = "",
                                  attributes: dict = None, atomic: bool = False) -> int:
        """Upserts edges from a Pandas DataFrame.

        Args:
            df:
                The DataFrame to upsert.
            sourceVertexType:
                The type of source vertex for the edge.
            edgeType:
                The type of edge to upsert data to.
            targetVertexType:
                The type of target vertex for the edge.
            from_id:
                The field name where the source vertex primary id is given. If omitted, the
                dataframe index would be used instead.
            to_id:
                The field name where the target vertex primary id is given. If omitted, the
                dataframe index would be used instead.
            attributes:
                A dictionary in the form of `{target: source}` where source is the column name in
                the dataframe and target is the attribute name on the edge. When omitted,
                all columns would be upserted with their current names. In this case column names
                must match the edges's attribute names.
            atomic:
                The request is an atomic transaction. An atomic transaction means that updates to
                the database contained in the request are all-or-nothing: either all changes are
                successful, or none are successful. This uses the `gsql-atomic-level` header, and sets
                the value to `atomic` if `True`, and `nonatomic` if `False`. Default is `False`.

        Returns:
            The number of edges upserted.
        """
        logger.debug("entry: upsertEdgeDataFrame")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        json_up = _prep_upsert_edge_dataframe(df, from_id, to_id, attributes)
        ret = await self.upsertEdges(sourceVertexType, edgeType, targetVertexType, json_up, atomic=atomic)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: upsertEdgeDataFrame")

        return ret

    async def getEdges(self, sourceVertexType: str, sourceVertexId: str, edgeType: str = "",
                       targetVertexType: str = "", targetVertexId: str = "", select: str = "", where: str = "",
                       limit: Union[int, str] = None, sort: str = "", fmt: str = "py", withId: bool = True,
                       withType: bool = False, timeout: int = 0) -> Union[dict, str, 'pd.DataFrame']:
        """Retrieves edges of the given edge type originating from a specific source vertex.

        Only `sourceVertexType` and `sourceVertexId` are required.
        If `targetVertexId` is specified, then `targetVertexType` must also be specified.
        If `targetVertexType` is specified, then `edgeType` must also be specified.

        Args:
            sourceVertexType:
                The name of the source vertex type.
            sourceVertexId:
                The primary ID value of the source vertex instance.
            edgeType:
                The name of the edge type.
            targetVertexType:
                The name of the target vertex type.
            targetVertexId:
                The primary ID value of the target vertex instance.
            select:
                Comma separated list of edge attributes to be retrieved or omitted.
            where:
                Comma separated list of conditions that are all applied on each edge's attributes.
                The conditions are in logical conjunction (i.e. they are "AND'ed" together).
            sort:
                Comma separated list of attributes the results should be sorted by.
            limit:
                Maximum number of edge instances to be returned (after sorting).
            fmt:
                Format of the results returned:
                - "py":   Python objects
                - "json": JSON document
                - "df":   pandas DataFrame
            withId:
                (When the output format is "df") Should the source and target vertex types and IDs
                be included in the dataframe?
            withType:
                (When the output format is "df") Should the edge type be included in the dataframe?
            timeout:
                Time allowed for successful execution (0 = no time limit, default).

        Returns:
            The (selected) details of the (matching) edge instances (sorted, limited) as dictionary,
            JSON or pandas DataFrame.

        Endpoint:
            - `GET /graph/{graph_name}/edges/{source_vertex_type}/{source_vertex_id}`
                See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#list-edges-of-a-vertex
        """
        logger.debug("entry: getEdges")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        url = _prep_get_edges(self.restppUrl,
                              self.graphname,
                              sourceVertexType,
                              sourceVertexId,
                              edgeType,
                              targetVertexType,
                              targetVertexId,
                              select,
                              where,
                              limit,
                              sort,
                              timeout)
        ret = await self._req("GET", url)

        if fmt == "json":
            ret = json.dumps(ret)
        elif fmt == "df":
            ret = _eS2DF(ret, withId, withType)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getEdges")

        return ret

    async def getEdgesDataFrame(self, sourceVertexType: str, sourceVertexId: str, edgeType: str = "",
                                targetVertexType: str = "", targetVertexId: str = "", select: str = "", where: str = "",
                                limit: Union[int, str] = None, sort: str = "", timeout: int = 0) -> 'pd.DataFrame':
        """Retrieves edges of the given edge type originating from a specific source vertex.

        This is a shortcut to ``getEdges(..., fmt="df", withId=True, withType=False)``.
        Only ``sourceVertexType`` and ``sourceVertexId`` are required.
        If ``targetVertexId`` is specified, then ``targetVertexType`` must also be specified.
        If ``targetVertexType`` is specified, then ``edgeType`` must also be specified.

        Args:
            sourceVertexType:
                The name of the source vertex type.
            sourceVertexId:
                The primary ID value of the source vertex instance.
            edgeType:
                The name of the edge type.
            targetVertexType:
                The name of the target vertex type.
            targetVertexId:
                The primary ID value of the target vertex instance.
            select:
                Comma separated list of edge attributes to be retrieved or omitted.
            where:
                Comma separated list of conditions that are all applied on each edge's attributes.
                The conditions are in logical conjunction (i.e. they are "AND'ed" together).
            sort:
                Comma separated list of attributes the results should be sorted by.
            limit:
                Maximum number of edge instances to be returned (after sorting).
            timeout:
                Time allowed for successful execution (0 = no limit, default).

        Returns:
            The (selected) details of the (matching) edge instances (sorted, limited) as dictionary,
            JSON or pandas DataFrame.
        """
        logger.debug("entry: getEdgesDataFrame")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = await self.getEdges(sourceVertexType, sourceVertexId, edgeType, targetVertexType,
                                  targetVertexId, select, where, limit, sort, fmt="df", timeout=timeout)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getEdgesDataFrame")

        return ret

    async def getEdgesDataframe(self, sourceVertexType: str, sourceVertexId: str, edgeType: str = "",
                                targetVertexType: str = "", targetVertexId: str = "", select: str = "", where: str = "",
                                limit: Union[int, str] = None, sort: str = "", timeout: int = 0) -> 'pd.DataFrame':
        """DEPRECATED

        Use `getEdgesDataFrame()` instead.
        """
        warnings.warn(
            "The `getEdgesDataframe()` function is deprecated; use `getEdgesDataFrame()` instead.",
            DeprecationWarning)

        return await self.getEdgesDataFrame(sourceVertexType, sourceVertexId, edgeType, targetVertexType,
                                            targetVertexId, select, where, limit, sort, timeout)

    async def getEdgesByType(self, edgeType: str, fmt: str = "py", withId: bool = True,
                             withType: bool = False, limit: int = None) -> Union[dict, str, 'pd.DataFrame']:
        """Retrieves edges of the given edge type regardless the source vertex.

        Args:
            edgeType:
                The name of the edge type.
            fmt:
                Format of the results returned:
                - "py":   Python objects
                - "json": JSON document
                - "df":   pandas DataFrame
            withId:
                (When the output format is "df") Should the source and target vertex types and IDs
                be included in the dataframe?
            withType:
                (When the output format is "df") should the edge type be included in the dataframe?
            limit:
                Maximum number of edge instances to be returned. Default is `None` (no limit).
                **Note:** The limit is applied after retrieving the edges, so the load on the database is not reduced.

        Returns:
            The details of the edge instances of the given edge type as dictionary, JSON or pandas
            DataFrame.
        """
        logger.debug("entry: getEdgesByType")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if not edgeType:
            logger.warning("Edge type is not specified")
            logger.debug("exit: getEdgesByType")

            return {}

        sourceVertexType = await self.getEdgeSourceVertexType(edgeType)
        queryText = _prep_get_edges_by_type(self.graphname, sourceVertexType, edgeType)
        ret = await self.runInterpretedQuery(queryText)

        if limit:
            ret = ret[0]["edges"][:limit]
        else:
            ret = ret[0]["edges"]

        if fmt == "json":
            ret = json.dumps(ret)
        elif fmt == "df":
            ret = _eS2DF(ret, withId, withType)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: _upsertAttrs")

        return ret

    # TODO getEdgesDataFrameByType

    async def getEdgeStats(self, edgeTypes: Union[str, list], skipNA: bool = False) -> dict:
        """Returns edge attribute statistics.

        Args:
            edgeTypes:
                A single edge type name or a list of edges types names or '*' for all edges types.
            skipNA:
                Skip those edges that do not have attributes or none of their attributes have
                statistics gathered.

        Returns:
            Attribute statistics of edges; a dictionary of dictionaries.

        Endpoint:
            - `POST /builtins/{graph_name}`
                See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#run-built-in-functions-on-graph
        """
        logger.debug("entry: getEdgeStats")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ets = []
        if edgeTypes == "*":
            ets = await self.getEdgeTypes()
        elif isinstance(edgeTypes, str):
            ets = [edgeTypes]
        elif isinstance(edgeTypes, list):
            ets = edgeTypes
        else:
            logger.warning("The `edgeTypes` parameter is invalid.")
            logger.debug("exit: getEdgeStats")

            return {}

        responses = []
        for et in ets:
            data = '{"function":"stat_edge_attr","type":"' + \
                et + '","from_type":"*","to_type":"*"}'
            res = await self._req("POST", self.restppUrl + "/builtins/" + self.graphname, data=data, resKey="",
                                  skipCheck=True)
            responses.append((et, res))
        ret = _parse_get_edge_stats(responses, skipNA)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getEdgeStats")

        return ret

    async def delEdges(self, sourceVertexType: str, sourceVertexId: str, edgeType: str = "",
                       targetVertexType: str = "", targetVertexId: str = "", where: str = "",
                       limit: str = "", sort: str = "", timeout: int = 0) -> dict:
        """Deletes edges from the graph.

        Only `sourceVertexType` and `sourceVertexId` are required.
        If `targetVertexId` is specified, then `targetVertexType` must also be specified.
        If `targetVertexType` is specified, then `edgeType` must also be specified.

        Args:
            sourceVertexType:
                The name of the source vertex type.
            sourceVertexId:
                The primary ID value of the source vertex instance.
            edgeType:
                The name of the edge type.
            targetVertexType:
                The name of the target vertex type.
            targetVertexId:
                The primary ID value of the target vertex instance.
            where:
                Comma separated list of conditions that are all applied on each edge's attributes.
                The conditions are in logical conjunction (they are connected as if with an `AND` statement).
            limit:
                Maximum number of edge instances to be returned after sorting.
            sort:
                Comma-separated list of attributes the results should be sorted by.
            timeout:
                Time allowed for successful execution. The default is `0`, or no limit.

        Returns:
             A dictionary of `edge_type: deleted_edge_count` pairs.

        Endpoint:
            - `DELETE /graph/{graph_name}/edges/{source_vertex_type}/{source_vertex_id}/{edge_type}/{target_vertex_type}/{target_vertex_id}`
                See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#delete-an-edge
        """
        logger.debug("entry: delEdges")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        url = _prep_del_edges(self.restppUrl,
                              self.graphname,
                              sourceVertexType,
                              sourceVertexId,
                              edgeType,
                              targetVertexType,
                              targetVertexId,
                              where,
                              limit,
                              sort,
                              timeout)
        res = await self._req("DELETE", url)
        ret = {}
        for r in res:
            ret[r["e_type"]] = r["deleted_edges"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: delEdges")

        return ret
    
    async def edgeSetToDataFrame(self, edgeSet: list, withId: bool = True, withType: bool = False) -> 'pd.DataFrame':
        """Converts an edge set to a pandas DataFrame.

        Args:
            edgeSet:
                The edge set to convert.
            withId:
                Should the source and target vertex types and IDs be included in the dataframe?
            withType:
                Should the edge type be included in the dataframe?

        Returns:
            The edge set as a pandas DataFrame.
        """
        logger.debug("entry: edgeSetToDataFrame")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = _eS2DF(edgeSet, withId, withType)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: edgeSetToDataFrame")

        return ret