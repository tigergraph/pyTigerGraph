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

from pyTigerGraph.common.edge import PyTigerGraphEdgeBase
# from pyTigerGraph.common.base import PyTigerGraphCore
from pyTigerGraph.pyTigerGraphQuery import pyTigerGraphQuery


logger = logging.getLogger(__name__)


class pyTigerGraphEdge(PyTigerGraphEdgeBase, pyTigerGraphQuery):

    ___trgvtxids = "___trgvtxids"

    def getEdgeTypes(self, force: bool = False) -> list:
        """Returns the list of edge type names of the graph.

        Args:
            force:
                If `True`, forces the retrieval the schema metadata again, otherwise returns a
                cached copy of edge type metadata (if they were already fetched previously).

        Returns:
            The list of edge types defined in the current graph.
        """
        logger.info("entry: getEdgeTypes")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = []
        for et in self.getSchema(force=force)["EdgeTypes"]:
            ret.append(et["Name"])

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getEdgeTypes")

        return ret

    def getEdgeType(self, edgeType: str, force: bool = False) -> dict:
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
        logger.info("entry: getEdgeType")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        for et in self.getSchema(force=force)["EdgeTypes"]:
            if et["Name"] == edgeType:
                if logger.level == logging.DEBUG:
                    logger.debug("return: " + str(et))
                logger.info("exit: getEdgeType (found)")

                return et

        logger.warning("Edge type `" + edgeType + "` was not found.")
        logger.info("exit: getEdgeType (not found)")

        return {}

    def getEdgeAttrs(self, edgeType: str) -> list:
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
        logger.info("entry: getAttributes")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        et = self.getEdgeType(edgeType)
        ret = []

        for at in et["Attributes"]:
            ret.append(
                (at["AttributeName"], self._get_attr_type(at["AttributeType"])))

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getAttributes")

        return ret

    def getEdgeSourceVertexType(self, edgeType: str) -> Union[str, set]:
        """Returns the type(s) of the edge type's source vertex.

        Args:
            edgeType:
                The name of the edge type.

        Returns:
            - A single source vertex type name string if the edge has a single source vertex type.
            - "*" if the edge can originate from any vertex type (notation used in 2.6.1 and earlier
                versions).
                #creating-an-edge-from-or-to-any-vertex-type
                See https://docs.tigergraph.com/v/2.6/dev/gsql-ref/ddl-and-loading/defining-a-graph-schema
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
        logger.info("entry: getEdgeSourceVertexType")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        edgeTypeDetails = self.getEdgeType(edgeType)
        res = self._parse_get_edge_source_vertex_type(edgeTypeDetails)
        return res

    def getEdgeTargetVertexType(self, edgeType: str) -> Union[str, set]:
        """Returns the type(s) of the edge type's target vertex.

        Args:
            edgeType:
                The name of the edge type.

        Returns:
            - A single target vertex type name string if the edge has a single target vertex type.
            - "*" if the edge can end in any vertex type (notation used in 2.6.1 and earlier
                versions).
                #creating-an-edge-from-or-to-any-vertex-type
                See https://docs.tigergraph.com/v/2.6/dev/gsql-ref/ddl-and-loading/defining-a-graph-schema
            - A set of vertex type name strings (unique values) if the edge has multiple target
                vertex types (notation used in 3.0 and later versions). /
                Even if the target vertex types were defined as "*", the REST API will list them as
                pairs (i.e. not as "*" in 2.6.1 and earlier versions), just like as if there were
                defined one by one (e.g. `FROM v1, TO v2 | FROM v3, TO v4 | …`).

            The returned set contains all target vertex types, but does not certainly mean that the
                edge is defined between all source and all target vertex types. You need to look at
                the individual source/target pairs to find out which combinations are valid/defined.
        """
        logger.info("entry: getEdgeTargetVertexType")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        edgeTypeDetails = self.getEdgeType(edgeType)
        ret = self._parse_get_edge_target_vertex_type(edgeTypeDetails)
        return ret

    def isDirected(self, edgeType: str) -> bool:
        """Is the specified edge type directed?

        Args:
            edgeType:
                The name of the edge type.

        Returns:
            `True`, if the edge is directed.
        """
        logger.info("entry: isDirected")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = self.getEdgeType(edgeType)["IsDirected"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: isDirected")

        return ret

    def getReverseEdge(self, edgeType: str) -> str:
        """Returns the name of the reverse edge of the specified edge type, if applicable.

        Args:
            edgeType:
                The name of the edge type.

        Returns:
            The name of the reverse edge, if it was defined.
        """
        logger.info("entry: getReverseEdge")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if not self.isDirected(edgeType):
            logger.error(edgeType + " is not a directed edge")
            logger.info("exit: getReverseEdge (not directed)")

            return ""

        config = self.getEdgeType(edgeType)["Config"]
        if "REVERSE_EDGE" in config:
            ret = config["REVERSE_EDGE"]

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(ret))
            logger.info("exit: getReverseEdge (reverse edge found)")

            return ret

        logger.info("exit: getReverseEdge (reverse edge not found)")

        return ""
        # TODO Should return some other value or raise exception?

    def isMultiEdge(self, edgeType: str) -> bool:
        """Can the edge have multiple instances between the same pair of vertices?

        Args:
            edgeType:
                The name of the edge type.

        Returns:
            `True`, if the edge can have multiple instances between the same pair of vertices.
        """
        logger.info("entry: isMultiEdge")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        et = self.getEdgeType(edgeType)
        ret = ("DiscriminatorCount" in et) and et["DiscriminatorCount"] > 0

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: isMultiEdge")

        return ret

    def getDiscriminators(self, edgeType: str) -> list:
        """Returns the names and types of the discriminators of the edge type.

        Args:
            edgeType:
                The name of the edge type.

        Returns:
            A list of (attribute_name, attribute_type) tuples.
        """
        logger.info("entry: getDiscriminators")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        et = self.getEdgeType(edgeType)
        ret = []

        for at in et["Attributes"]:
            if "IsDiscriminator" in at and at["IsDiscriminator"]:
                ret.append(
                    (at["AttributeName"], self._get_attr_type(at["AttributeType"])))

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getDiscriminators")

        return ret

    def getEdgeCountFrom(self, sourceVertexType: str = "", sourceVertexId: Union[str, int] = None,
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
                #_list_edges_of_a_vertex
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints
            - `POST /builtins/{graph_name}`
                #_run_built_in_functions_on_graph
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints
        """
        logger.info("entry: getEdgeCountFrom")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        url, data = self._prep_get_edge_count_from(sourceVertexType=sourceVertexType, sourceVertexId=sourceVertexId, edgeType=edgeType,
                                                   targetVertexType=targetVertexType, targetVertexId=targetVertexId, where=where)
        if data:
            res = self._req("POST", url, data=data)
        else:
            res = self._req("GET", url)
        ret = self._parse_get_edge_count_from(res, edgeType)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getEdgeCountFrom  (multiple edge types)")

        return ret

    def getEdgeCount(self, edgeType: str = "*", sourceVertexType: str = "",
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
        logger.info("entry: getEdgeCount")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = self.getEdgeCountFrom(edgeType=edgeType, sourceVertexType=sourceVertexType,
                                    targetVertexType=targetVertexType)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getEdgeCount")

        return ret

    def upsertEdge(self, sourceVertexType: str, sourceVertexId: str, edgeType: str,
                   targetVertexType: str, targetVertexId: str, attributes: dict = None, vertexMustExist: bool = False) -> int:
        """Upserts an edge.

        Data is upserted:

        - If edge is not yet present in graph, it will be created (see special case below).
        - If it's already in the graph, it is updated with the values specified in the request.
        - If `vertex_must_exist` is True then edge will only be created if both vertices exists
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
                #operation-codes .
                For valid values of `<operator>` see https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints

        Returns:
            A single number of accepted (successfully upserted) edges (0 or 1).

        Endpoint:
            - `POST /graph/{graph_name}`
                See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#upsert-data-to-graph

        TODO Add ack, new_vertex_only, vertex_must_exist, update_vertex_only and atomic_level
            parameters and functionality.
        """
        logger.info("entry: upsertEdge")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        data = self._prep_upsert_edge(
            attributes, sourceVertexType, sourceVertexId, edgeType, targetVertexType, targetVertexId)

        ret = self._req("POST", self.restppUrl + "/graph/" + self.graphname, data=data)[0][
            "accepted_edges"]

        vals = self._upsert_attrs(attributes)
        data = json.dumps(
            {
                "edges": {
                    sourceVertexType: {
                        sourceVertexId: {
                            edgeType: {targetVertexType: {
                                targetVertexId: vals}}

                        }
                    }
                }
            }
        )

        params = {"vertex_must_exist": vertexMustExist}
        ret = self._post(
            self.restppUrl + "/graph/" + self.graphname,
            data=data,
            params=params,
        )[0]["accepted_edges"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: upsertEdge")

        return ret

    def upsertEdges(self, sourceVertexType: str, edgeType: str, targetVertexType: str,
                    edges: list, vertexMustExist=False) -> int:
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
                    (17, "home_page", {"visits": (35, "+"),
                     "max_duration": (93, "max")}),
                    (42, "search", {"visits": (17, "+"),
                     "max_duration": (41, "max")})
                ]
                ```
                #operation-codes .
                For valid values of `<operator>` see https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints

        Returns:
            A single number of accepted (successfully upserted) edges (0 or positive integer).

        Endpoint:
            - `POST /graph/{graph_name}`
                See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#upsert-data-to-graph

        TODO Add ack, new_vertex_only, vertex_must_exist, update_vertex_only and atomic_level
            parameters and functionality.
        """

        def _dumps(data) -> str:
            """Generates the JSON format expected by the endpoint.

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
                    if k1 == self.___trgvtxids:
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
                                ret += json.dumps(k2) + ":" + json.dumps(v3)
                                c3 += 1
                            c2 += 1
                    else:
                        ret += json.dumps(k1) + ":" + _dumps(data[k1])
                    c1 += 1
            return "{" + ret + "}"

        logger.info("entry: upsertEdges")
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
        data = self._prep_upsert_edges(sourceVertexType=sourceVertexType,
                                       edgeType=edgeType, targetVertexType=targetVertexType, edges=edges)
        ret = self._req("POST", self.restppUrl + "/graph/" + self.graphname, data=data)[0][
            "accepted_edges"]

        data = {sourceVertexType: {}}
        l1 = data[sourceVertexType]
        for e in edges:
            if len(e) > 2:
                vals = self._upsert_attrs(e[2])
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
            if self.___trgvtxids not in l4:
                l4[self.___trgvtxids] = {}
            l4 = l4[self.___trgvtxids]
            # targetVertexId
            # Converted to string as the key in the JSON payload must be a string
            targetVertexId = str(e[1])
            if targetVertexId not in l4:
                l4[targetVertexId] = []
            l4[targetVertexId].append(vals)

        data = _dumps({"edges": data})

        params = {"vertex_must_exist": vertexMustExist}
        ret = self._post(
            self.restppUrl + "/graph/" + self.graphname, data=data, params=params
        )[0]["accepted_edges"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: upsertEdges")

        return ret

    def upsertEdgeDataFrame(self, df: 'pd.DataFrame', sourceVertexType: str, edgeType: str,
                            targetVertexType: str, from_id: str = "", to_id: str = "",
                            attributes: dict = None, vertexMustExist: bool = False) -> int:
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

        Returns:
            The number of edges upserted.
        """
        logger.info("entry: upsertEdgeDataFrame")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        json_up = self._prep_upsert_edge_dataframe(
            df, from_id, to_id, attributes)
        ret = self.upsertEdges(sourceVertexType, edgeType,
                               targetVertexType, json_up)

        json_up = []

        for index in df.index:
            json_up.append(json.loads(df.loc[index].to_json()))
            json_up[-1] = (
                index if from_id is None else json_up[-1][from_id],
                index if to_id is None else json_up[-1][to_id],
                json_up[-1]
                if attributes is None
                else {
                    target: json_up[-1][source] for target, source in attributes.items()
                },
            )

        ret = self.upsertEdges(
            sourceVertexType,
            edgeType,
            targetVertexType,
            json_up,
            vertexMustExist=vertexMustExist,
        )

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: upsertEdgeDataFrame")

        return ret

    def getEdges(self, sourceVertexType: str, sourceVertexId: str, edgeType: str = "",
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
        logger.info("entry: getEdges")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        # TODO Change sourceVertexId to sourceVertexIds and allow passing both str and list<str> as
        #   parameter
        url = self._prep_get_edges(sourceVertexType, sourceVertexId, edgeType,
                                   targetVertexType, targetVertexId, select, where, limit, sort, timeout)
        ret = self._req("GET", url)

        if fmt == "json":
            ret = json.dumps(ret)
        elif fmt == "df":
            ret = self.edgeSetToDataFrame(ret, withId, withType)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getEdges")

        return ret

    def getEdgesDataFrame(self, sourceVertexType: str, sourceVertexId: str, edgeType: str = "",
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
        logger.info("entry: getEdgesDataFrame")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = self.getEdges(sourceVertexType, sourceVertexId, edgeType, targetVertexType,
                            targetVertexId, select, where, limit, sort, fmt="df", timeout=timeout)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getEdgesDataFrame")

        return ret

    def getEdgesDataframe(self, sourceVertexType: str, sourceVertexId: str, edgeType: str = "",
                          targetVertexType: str = "", targetVertexId: str = "", select: str = "", where: str = "",
                          limit: Union[int, str] = None, sort: str = "", timeout: int = 0) -> 'pd.DataFrame':
        """DEPRECATED

        Use `getEdgesDataFrame()` instead.
        """
        warnings.warn(
            "The `getEdgesDataframe()` function is deprecated; use `getEdgesDataFrame()` instead.",
            DeprecationWarning)

        return self.getEdgesDataFrame(sourceVertexType, sourceVertexId, edgeType, targetVertexType,
                                      targetVertexId, select, where, limit, sort, timeout)

    def getEdgesByType(self, edgeType: str, fmt: str = "py", withId: bool = True,
                       withType: bool = False) -> Union[dict, str, 'pd.DataFrame']:
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

        Returns:
            The details of the edge instances of the given edge type as dictionary, JSON or pandas
            DataFrame.

        TODO Add limit parameter
        """
        logger.info("entry: getEdgesByType")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if not edgeType:
            logger.warning("Edge type is not specified")
            logger.info("exit: getEdgesByType")

            return {}

        sourceVertexType = self.getEdgeSourceVertexType(edgeType)
        queryText = self._prep_get_edges_by_type(sourceVertexType, edgeType)
        ret = self.runInterpretedQuery(queryText)

        ret = ret[0]["edges"]

        if fmt == "json":
            ret = json.dumps(ret)
        elif fmt == "df":
            ret = self.edgeSetToDataFrame(ret, withId, withType)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: _upsertAttrs")

        return ret

    # TODO getEdgesDataFrameByType

    def getEdgeStats(self, edgeTypes: Union[str, list], skipNA: bool = False) -> dict:
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
        logger.info("entry: getEdgeStats")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ets = []
        if edgeTypes == "*":
            ets = self.getEdgeTypes()
        elif isinstance(edgeTypes, str):
            ets = [edgeTypes]
        elif isinstance(edgeTypes, list):
            ets = edgeTypes
        else:
            logger.warning("The `edgeTypes` parameter is invalid.")
            logger.info("exit: getEdgeStats")

            return {}

        responses = []
        for et in ets:
            data = '{"function":"stat_edge_attr","type":"' + \
                et + '","from_type":"*","to_type":"*"}'
            res = self._req("POST", self.restppUrl + "/builtins/" + self.graphname, data=data, resKey="",
                            skipCheck=True)
            responses.append((et, res))
        ret = self._parse_get_edge_stats(responses, skipNA)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getEdgeStats")

        return ret

    def delEdges(self, sourceVertexType: str, sourceVertexId: str, edgeType: str = "",
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
        logger.info("entry: delEdges")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        url = self._prep_del_edges(sourceVertexType, sourceVertexId, edgeType,
                                   targetVertexType, targetVertexId, where, limit, sort, timeout)
        res = self._req("DELETE", url)
        ret = {}
        for r in res:
            ret[r["e_type"]] = r["deleted_edges"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: delEdges")

        return ret
