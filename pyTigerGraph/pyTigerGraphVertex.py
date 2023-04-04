"""Vertex Functions.

Functions to upsert, retrieve and delete vertices.

All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import json
import logging
import warnings

from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    import pandas as pd

from pyTigerGraph.pyTigerGraphException import TigerGraphException
from pyTigerGraph.pyTigerGraphSchema import pyTigerGraphSchema
from pyTigerGraph.pyTigerGraphUtils import pyTigerGraphUtils

logger = logging.getLogger(__name__)


class pyTigerGraphVertex(pyTigerGraphUtils, pyTigerGraphSchema):

    def getVertexTypes(self, force: bool = False) -> list:
        """Returns the list of vertex type names of the graph.

        Args:
            force:
                If `True`, forces the retrieval the schema metadata again, otherwise returns a
                cached copy of vertex type metadata (if they were already fetched previously).

        Returns:
            The list of vertex types defined in the current graph.
        """
        logger.info("entry: getVertexTypes")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = []
        for vt in self.getSchema(force=force)["VertexTypes"]:
            ret.append(vt["Name"])

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getVertexTypes")

        return ret

    def getVertexAttrs(self, vertexType: str) -> list:
        """Returns the names and types of the attributes of the vertex type.

        Args:
            vertexType:
                The name of the vertex type.

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

        et = self.getVertexType(vertexType)
        ret = []

        for at in et["Attributes"]:
            ret.append((at["AttributeName"], self._getAttrType(at["AttributeType"])))

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getAttributes")

        return ret

    def getVertexType(self, vertexType: str, force: bool = False) -> dict:
        """Returns the details of the specified vertex type.

        Args:
            vertexType:
                The name of the vertex type.
            force:
                If `True`, forces the retrieval the schema metadata again, otherwise returns a
                cached copy of vertex type details (if they were already fetched previously).

        Returns:
            The metadata of the vertex type.
        """
        logger.info("entry: getVertexType")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        for vt in self.getSchema(force=force)["VertexTypes"]:
            if vt["Name"] == vertexType:
                if logger.level == logging.DEBUG:
                    logger.debug("return: " + str(vt))
                logger.info("exit: getVertexType (found)")

                return vt

        logger.warning("Vertex type `" + vertexType + "` was not found.")
        logger.info("exit: getVertexType (not found)")

        return {}  # Vertex type was not found

    def getVertexCount(self, vertexType: Union[str, list] = "*", where: str = "", realtime: bool = False) -> Union[int, dict]:
        """Returns the number of vertices of the specified type.

        Args:
            vertexType (Union[str, list], optional):
                The name of the vertex type. If `vertexType` == "*", then count the instances of all 
                vertex types (`where` cannot be specified in this case). Defaults to "*".
            where (str, optional):
                A comma separated list of conditions that are all applied on each vertex's
                attributes. The conditions are in logical conjunction (i.e. they are "AND'ed"
                together). Defaults to "".
            realtime (bool, optional):
                Whether to get the most up-to-date number by force. When there are frequent updates happening, 
                a slightly outdated number (up to 30 seconds delay) might be fetched. Set `realtime=True` to
                force the system to recount the vertices, which will get a more up-to-date result but will
                also take more time. This parameter only works with TigerGraph DB 3.6 and above.
                Defaults to False.

        Returns:
            - A dictionary of <vertex_type>: <vertex_count> pairs if `vertexType` is a list or "*".
            - An integer of vertex count if `vertexType` is a single vertex type.

        Uses:
            - If `vertexType` is specified only: count of the instances of the given vertex type(s).
            - If `vertexType` and `where` are specified: count of the instances of the given vertex
                type after being filtered by `where` condition(s).

        Raises:
            `TigerGraphException` when "*" is specified as vertex type and a `where` condition is
            provided; or when invalid vertex type name is specified.

        Endpoints:
            - `GET /graph/{graph_name}/vertices`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_list_vertices[List vertices]
            - `POST /builtins`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_run_built_in_functions_on_graph[Run built-in functions]
        """
        logger.info("entry: getVertexCount")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        # If WHERE condition is not specified, use /builtins else use /vertices
        if isinstance(vertexType, str) and vertexType != "*":
            if where:
                res = self._get(self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType
                    + "?count_only=true" + "&filter=" + where)[0]["count"]
            else:
                res = self._post(self.restppUrl + "/builtins/" + self.graphname + ("?realtime=true" if realtime else ""),
                                 data={"function": "stat_vertex_number", "type": vertexType},
                                 jsonData=True)[0]["count"]

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(res))
            logger.info("exit: getVertexCount (1)")

            return res

        if where:
            if vertexType == "*":
                raise TigerGraphException(
                    "VertexType cannot be \"*\" if where condition is specified.", None)
            else:
                raise TigerGraphException(
                    "VertexType cannot be a list if where condition is specified.", None)

        res = self._post(self.restppUrl + "/builtins/" + self.graphname + ("?realtime=true" if realtime else ""),
                         data={"function": "stat_vertex_number", "type": "*"},
                         jsonData=True)
        ret = {d["v_type"]: d["count"] for d in res}

        if isinstance(vertexType, list):
            ret = {vt: ret[vt] for vt in vertexType}

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getVertexCount (2)")

        return ret

    def upsertVertex(self, vertexType: str, vertexId: str, attributes: dict = None) -> int:
        """Upserts a vertex.

        Data is upserted:

        - If vertex is not yet present in graph, it will be created.
        - If it's already in the graph, its attributes are updated with the values specified in
            the request. An optional operator controls how the attributes are updated.

        Args:
            vertexType:
                The name of the vertex type.
            vertexId:
                The primary ID of the vertex to be upserted.
            attributes:
                The attributes of the vertex to be upserted; a dictionary in this format:
                ```
                    {<attribute_name>: <attribute_value>|(<attribute_name>, <operator>), …}
                ```
                Example:
                ```
                    {"name": "Thorin", points: (10, "+"), "bestScore": (67, "max")}
                ```
                For valid values of `<operator>` see xref:tigergraph-server:API:built-in-endpoints.adoc#_operation_codes[Operation codes].

        Returns:
             A single number of accepted (successfully upserted) vertices (0 or 1).

        Endpoint:
            - `POST /graph/{graph_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_upsert_data_to_graph[Upsert data to graph]
        """
        logger.info("entry: upsertVertex")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        vals = self._upsertAttrs(attributes)
        data = json.dumps({"vertices": {vertexType: {vertexId: vals}}})

        ret = self._post(self.restppUrl + "/graph/" + self.graphname, data=data)[0]["accepted_vertices"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: upsertVertex")

        return ret

    def upsertVertices(self, vertexType: str, vertices: list) -> int:
        """Upserts multiple vertices (of the same type).

        See the description of ``upsertVertex`` for generic information.

        Args:
            vertexType:
                The name of the vertex type.
            vertices:
                A list of tuples in this format:

                [source.wrap,json]
                ----
                [
                    (<vertex_id>, {<attribute_name>: <attribute_value>, …}),
                    (<vertex_id>, {<attribute_name>: (<attribute_value>, <operator>), …}),
                    ⋮
                ]
                ----

                Example:

                [source.wrap, json]
                ----
                [
                    (2, {"name": "Balin", "points": (10, "+"), "bestScore": (67, "max")}),
                    (3, {"name": "Dwalin", "points": (7, "+"), "bestScore": (35, "max")})
                ]
                ----

                For valid values of `<operator>` see xref:tigergraph-server:API:built-in-endpoints.adoc#_operation_codes[Operation codes].

        Returns:
            A single number of accepted (successfully upserted) vertices (0 or positive integer).

        Endpoint:
            - `POST /graph/{graph_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_upsert_data_to_graph[Upsert data to graph]
        """
        logger.info("entry: upsertVertices")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        data = {}
        for v in vertices:
            vals = self._upsertAttrs(v[1])
            data[v[0]] = vals
        data = json.dumps({"vertices": {vertexType: data}})

        ret = self._post(self.restppUrl + "/graph/" + self.graphname, data=data)[0]["accepted_vertices"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: upsertVertices")

        return ret

    def upsertVertexDataFrame(self, df: 'pd.DataFrame', vertexType: str, v_id: bool = None,
            attributes: dict = None) -> int:
        """Upserts vertices from a Pandas DataFrame.

        Args:
            df:
                The DataFrame to upsert.
            vertexType:
                The type of vertex to upsert data to.
            v_id:
                The field name where the vertex primary id is given. If omitted the dataframe index
                would be used instead.
            attributes:
                A dictionary in the form of `{target: source}` where source is the column name in
                the dataframe and target is the attribute name in the graph vertex. When omitted,
                all columns would be upserted with their current names. In this case column names
                must match the vertex's attribute names.

        Returns:
            The number of vertices upserted.
        """
        logger.info("entry: upsertVertexDataFrame")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        json_up = []

        for index in df.index:
            json_up.append(json.loads(df.loc[index].to_json()))
            json_up[-1] = (
                index if v_id is None else json_up[-1][v_id],
                json_up[-1] if attributes is None
                else {target: json_up[-1][source] for target, source in attributes.items()}
            )

        ret = self.upsertVertices(vertexType=vertexType, vertices=json_up)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: upsertVertexDataFrame")

        return ret

    def getVertices(self, vertexType: str, select: str = "", where: str = "",
            limit: Union[int, str] = None, sort: str = "", fmt: str = "py", withId: bool = True,
            withType: bool = False, timeout: int = 0) -> Union[dict, str, 'pd.DataFrame']:
        """Retrieves vertices of the given vertex type.

        *Note*:
            The primary ID of a vertex instance is NOT an attribute, thus cannot be used in
            `select`, `where` or `sort` parameters (unless the `WITH primary_id_as_attribute` clause
            was used when the vertex type was created). /
            Use `getVerticesById()` if you need to retrieve vertices by their primary ID.

        Args:
            vertexType:
                The name of the vertex type.
            select:
                Comma separated list of vertex attributes to be retrieved.
            where:
                Comma separated list of conditions that are all applied on each vertex' attributes.
                The conditions are in logical conjunction (i.e. they are "AND'ed" together).
            sort:
                Comma separated list of attributes the results should be sorted by.
                Must be used with `limit`.
            limit:
                Maximum number of vertex instances to be returned (after sorting).
                Must be used with `sort`.
            fmt:
                Format of the results:
                - "py":   Python objects
                - "json": JSON document
                - "df":   pandas DataFrame
            withId:
                (When the output format is "df") should the vertex ID be included in the dataframe?
            withType:
                (When the output format is "df") should the vertex type be included in the dataframe?
            timeout:
                Time allowed for successful execution (0 = no limit, default).

        Returns:
            The (selected) details of the (matching) vertex instances (sorted, limited) as
            dictionary, JSON or pandas DataFrame.

        Endpoint:
            - `GET /graph/{graph_name}/vertices/{vertex_type}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_list_vertices[List vertices]
        """
        logger.info("entry: getVertices")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        url = self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType
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

        ret = self._get(url)

        if fmt == "json":
            ret = json.dumps(ret)
        elif fmt == "df":
            ret = self.vertexSetToDataFrame(ret, withId, withType)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getVertices")

        return ret

    def getVertexDataFrame(self, vertexType: str, select: str = "", where: str = "",
            limit: Union[int, str] = None, sort: str = "", timeout: int = 0) -> 'pd.DataFrame':
        """Retrieves vertices of the given vertex type and returns them as pandas DataFrame.

        This is a shortcut to `getVertices(..., fmt="df", withId=True, withType=False)`.

        *Note*:
            The primary ID of a vertex instance is NOT an attribute, thus cannot be used in
            `select`, `where` or `sort` parameters (unless the `WITH primary_id_as_attribute` clause
            was used when the vertex type was created). /
            Use `getVerticesById()` if you need to retrieve vertices by their primary ID.

        Args:
            vertexType:
                The name of the vertex type.
            select:
                Comma separated list of vertex attributes to be retrieved.
            where:
                Comma separated list of conditions that are all applied on each vertex' attributes.
                The conditions are in logical conjunction (i.e. they are "AND'ed" together).
            sort:
                Comma separated list of attributes the results should be sorted by.
                Must be used with 'limit'.
            limit:
                Maximum number of vertex instances to be returned (after sorting).
                Must be used with `sort`.
            timeout:
                Time allowed for successful execution (0 = no limit, default).

        Returns:
            The (selected) details of the (matching) vertex instances (sorted, limited) as pandas
            DataFrame.
        """
        logger.info("entry: getVertexDataFrame")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = self.getVertices(vertexType, select=select, where=where, limit=limit, sort=sort,
            fmt="df", withId=True, withType=False, timeout=timeout)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getVertexDataFrame")

        return ret

    def getVertexDataframe(self, vertexType: str, select: str = "", where: str = "",
            limit: Union[int, str] = None, sort: str = "", timeout: int = 0) -> 'pd.DataFrame':
        """DEPRECATED

        Use `getVertexDataFrame()` instead.
        """
        warnings.warn(
            "The `getVertexDataframe()` function is deprecated; use `getVertexDataFrame()` instead.",
            DeprecationWarning)

        return self.getVertexDataFrame(vertexType, select=select, where=where, limit=limit,
            sort=sort, timeout=timeout)

    def getVerticesById(self, vertexType: str, vertexIds: Union[int, str, list], select: str = "",
            fmt: str = "py", withId: bool = True, withType: bool = False,
            timeout: int = 0) -> Union[list, str, 'pd.DataFrame']:
        """Retrieves vertices of the given vertex type, identified by their ID.

        Args:
            vertexType:
                The name of the vertex type.
            vertexIds:
                A single vertex ID or a list of vertex IDs.
            select:
                Comma separated list of vertex attributes to be retrieved.
            fmt:
                Format of the results:
                    "py":   Python objects (in a list)
                    "json": JSON document
                    "df":   pandas DataFrame
            withId:
                (If the output format is "df") should the vertex ID be included in the dataframe?
            withType:
                (If the output format is "df") should the vertex type be included in the dataframe?
            timeout:
                Time allowed for successful execution (0 = no limit, default).

        Returns:
            The (selected) details of the (matching) vertex instances as dictionary, JSON or pandas
            DataFrame.

        Endpoint:
            - `GET /graph/{graph_name}/vertices/{vertex_type}/{vertex_id}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_retrieve_a_vertex[Retrieve a vertex]

        TODO Find out how/if select and timeout can be specified
        """
        logger.info("entry: getVerticesById")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if not vertexIds:
            raise TigerGraphException("No vertex ID was specified.", None)
        vids = []
        if isinstance(vertexIds, (int, str)):
            vids.append(vertexIds)
        else:
            vids = vertexIds
        url = self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType + "/"

        ret = []
        for vid in vids:
            ret += self._get(url + self._safeChar(vid))

        if fmt == "json":
            ret = json.dumps(ret)
        elif fmt == "df":
            ret = self.vertexSetToDataFrame(ret, withId, withType)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getVerticesById")

        return ret

    def getVertexDataFrameById(self, vertexType: str, vertexIds: Union[int, str, list],
            select: str = "") -> 'pd.DataFrame':
        """Retrieves vertices of the given vertex type, identified by their ID.

        This is a shortcut to ``getVerticesById(..., fmt="df", withId=True, withType=False)``.

        Args:
            vertexType:
                The name of the vertex type.
            vertexIds:
                A single vertex ID or a list of vertex IDs.
            select:
                Comma separated list of vertex attributes to be retrieved.

        Returns:
            The (selected) details of the (matching) vertex instances as pandas DataFrame.
        """
        logger.info("entry: getVertexDataFrameById")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = self.getVerticesById(vertexType, vertexIds, select, fmt="df", withId=True,
            withType=False)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getVertexDataFrameById")

        return ret

    def getVertexDataframeById(self, vertexType: str, vertexIds: Union[int, str, list],
            select: str = "") -> 'pd.DataFrame':
        """DEPRECATED

        Use `getVertexDataFrameById()` instead.
        """
        warnings.warn(
            "The `getVertexDataframeById()` function is deprecated; use `getVertexDataFrameById()` instead.",
            DeprecationWarning)

        return self.getVertexDataFrameById(vertexType, vertexIds, select)

    def getVertexStats(self, vertexTypes: Union[str, list], skipNA: bool = False) -> dict:
        """Returns vertex attribute statistics.

        Args:
            vertexTypes:
                A single vertex type name or a list of vertex types names or "*" for all vertex
                types.
            skipNA:
                Skip those non-applicable vertices that do not have attributes or none of their
                attributes have statistics gathered.

        Returns:
            A dictionary of various vertex stats for each vertex type specified.

        Endpoint:
            - `POST /builtins/{graph_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_run_built_in_functions_on_graph[Run built-in functions]
        """
        logger.info("entry: getVertexStats")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        vts = []
        if vertexTypes == "*":
            vts = self.getVertexTypes()
        elif isinstance(vertexTypes, str):
            vts = [vertexTypes]
        else:
            vts = vertexTypes

        ret = {}
        for vt in vts:
            data = '{"function":"stat_vertex_attr","type":"' + vt + '"}'
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, data=data, resKey="",
                skipCheck=True)
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

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getVertexStats")

        return ret

    def delVertices(self, vertexType: str, where: str = "", limit: str = "", sort: str = "",
            permanent: bool = False, timeout: int = 0) -> int:
        """Deletes vertices from graph.

        *Note*:
            The primary ID of a vertex instance is not an attribute. A primary ID cannot be used in
            `select`, `where` or `sort` parameters (unless the `WITH primary_id_as_attribute` clause
            was used when the vertex type was created). /
            Use `delVerticesById()` if you need to retrieve vertices by their primary ID.

        Args:
            vertexType:
                The name of the vertex type.
            where:
                Comma separated list of conditions that are all applied on each vertex' attributes.
                The conditions are in logical conjunction (i.e. they are "AND'ed" together).
            sort:
                Comma separated list of attributes the results should be sorted by.
                Must be used with `limit`.
            limit:
                Maximum number of vertex instances to be returned (after sorting).
                Must be used with `sort`.
            permanent:
                If true, the deleted vertex IDs can never be inserted back, unless the graph is
                dropped or the graph store is cleared.
           timeout:
                Time allowed for successful execution (0 = no limit, default).

        Returns:
             A single number of vertices deleted.

        The primary ID of a vertex instance is NOT an attribute, thus cannot be used in above
            arguments.

        Endpoint:
            - `DELETE /graph/{graph_name}/vertices/{vertex_type}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_delete_vertices[Delete vertices]
        """
        logger.info("entry: delVertices")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        url = self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType
        isFirst = True
        if where:
            url += "?filter=" + where
            isFirst = False
        if limit and sort:  # These two must be provided together
            url += ("?" if isFirst else "&") + "limit=" + str(limit) + "&sort=" + sort
            isFirst = False
        if permanent:
            url += ("?" if isFirst else "&") + "permanent=true"
            isFirst = False
        if timeout and timeout > 0:
            url += ("?" if isFirst else "&") + "timeout=" + str(timeout)

        ret = self._delete(url)["deleted_vertices"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: delVertices")

        return ret

    def delVerticesById(self, vertexType: str, vertexIds: Union[int, str, list],
            permanent: bool = False, timeout: int = 0) -> int:
        """Deletes vertices from graph identified by their ID.

        Args:
            vertexType:
                The name of the vertex type.
            vertexIds:
                A single vertex ID or a list of vertex IDs.
            permanent:
                If true, the deleted vertex IDs can never be inserted back, unless the graph is
                dropped or the graph store is cleared.
            timeout:
                Time allowed for successful execution (0 = no limit, default).

        Returns:
            A single number of vertices deleted.

        Endpoint:
            - `DELETE /graph/{graph_name}/vertices/{vertex_type}/{vertex_id}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_delete_a_vertex[Delete a vertex]
        """
        logger.info("entry: delVerticesById")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if not vertexIds:
            raise TigerGraphException("No vertex ID was specified.", None)
        vids = []
        if isinstance(vertexIds, (int, str)):
            vids.append(self._safeChar(vertexIds))
        else:
            vids = [self._safeChar(f) for f in vertexIds]

        url1 = self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType + "/"
        url2 = ""
        if permanent:
            url2 = "?permanent=true"
        if timeout and timeout > 0:
            url2 += ("&" if url2 else "?") + "timeout=" + str(timeout)
        ret = 0
        for vid in vids:
            ret += self._delete(url1 + str(vid) + url2)["deleted_vertices"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: delVerticesById")

        return ret

    # def delVerticesByType(self, vertexType: str, permanent: bool = False):
    # TODO Implementation
    # TODO DELETE /graph/{graph_name}/delete_by_type/vertices/{vertex_type}/
    # TODO Maybe call it truncateVertex[Type] or delAllVertices?

    # TODO GET /deleted_vertex_check/{graph_name}

    def vertexSetToDataFrame(self, vertexSet: list, withId: bool = True,
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
        logger.info("entry: vertexSetToDataFrame")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

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
        logger.info("exit: vertexSetToDataFrame")

        return ret
