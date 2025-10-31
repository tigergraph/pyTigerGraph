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

from pyTigerGraph.common.vertex import (
    _parse_get_vertex_count,
    _prep_upsert_vertex_dataframe,
    _prep_get_vertices,
    _prep_get_vertices_by_id,
    _parse_get_vertex_stats,
    _prep_del_vertices,
    _prep_del_vertices_by_id,
    _prep_del_vertices_by_type
)

from pyTigerGraph.common.schema import _upsert_attrs, _get_attr_type
from pyTigerGraph.common.vertex import vertexSetToDataFrame as _vS2DF
from pyTigerGraph.common.util import _safe_char
from pyTigerGraph.common.exception import TigerGraphException

from pyTigerGraph.pytgasync.pyTigerGraphSchema import AsyncPyTigerGraphSchema
from pyTigerGraph.pytgasync.pyTigerGraphUtils import AsyncPyTigerGraphUtils

logger = logging.getLogger(__name__)


class AsyncPyTigerGraphVertex(AsyncPyTigerGraphUtils, AsyncPyTigerGraphSchema):

    async def getVertexTypes(self, force: bool = False) -> list:
        """Returns the list of vertex type names of the graph.

        Args:
            force:
                If `True`, forces the retrieval the schema metadata again, otherwise returns a
                cached copy of vertex type metadata (if they were already fetched previously).

        Returns:
            The list of vertex types defined in the current graph.
        """
        logger.debug("entry: getVertexTypes")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = []
        vertexTypes = await self.getSchema(force=force)
        vertexTypes = vertexTypes["VertexTypes"]
        for vt in vertexTypes:
            ret.append(vt["Name"])

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getVertexTypes")

        return ret

    async def getVertexAttrs(self, vertexType: str) -> list:
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
        logger.debug("entry: getAttributes")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        vt = await self.getVertexType(vertexType)
        ret = []

        if "Attributes" in vt:
            for at in vt["Attributes"]:
                ret.append(
                    (at["AttributeName"], _get_attr_type(at["AttributeType"]))
                )

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getAttributes")

        return ret

    async def getVertexVectors(self, vertexType: str) -> list:
        """Returns the names and types of the embedding attributes of the vertex type.

        Args:
            vertexType:
                The name of the vertex type.

        Returns:
            A list of (vector_name, vector_type) tuples.
            The format of vector_type is one of
             - "scalar_type"
             - "complex_type(scalar_type)"
             - "map_type(key_type,value_type)"
            and it is a string.
        """
        logger.debug("entry: getVertexVectors")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        vt = await self.getVertexType(vertexType)
        ret = []

        if "EmbeddingAttributes" in vt:
            for et in vt["EmbeddingAttributes"]:
                ret.append(
                    (et["Name"], et)
                )

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getVertexVectors")

        return ret

    async def getVectorStatus(self, vertexType: str, vectorName: str = "") -> bool:
        """Check the rebuild status of the vertex type or the embedding attribute

        Args:
            vertexType:
                The name of the vertex type.
            vectorName:
                The name of the vector attribute, optional.

        Returns:
            a bool indicates whether vector rebuild is done or not

        Endpoint:
            - `GET /vector/status/{graph_name}/{vertex_type}/[{vector_name}]`
        """
        logger.debug("entry: getVectorStatus")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = await self._req("GET", self.restppUrl + "/vector/status/" +
                        self.graphname + "/" + vertexType + "/" + vectorName)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getVectorStatus")

        return len(ret["NeedRebuildServers"]) == 0

    async def getVertexType(self, vertexType: str, force: bool = False) -> dict:
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
        logger.debug("entry: getVertexType")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        vertexTypes = await self.getSchema(force=force)
        vertexTypes = vertexTypes["VertexTypes"]
        for vt in vertexTypes:
            if vt["Name"] == vertexType:
                if logger.level == logging.DEBUG:
                    logger.debug("return: " + str(vt))
                logger.debug("exit: getVertexType (found)")

                return vt

        logger.warning("Vertex type `" + vertexType + "` was not found.")
        logger.debug("exit: getVertexType (not found)")

        return {}  # Vertex type was not found

    async def getVertexCount(self, vertexType: Union[str, list] = "*", where: str = "", realtime: bool = False) -> Union[int, dict]:
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
        logger.debug("entry: getVertexCount")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        # If WHERE condition is not specified, use /builtins else use /vertices
        if isinstance(vertexType, str) and vertexType != "*":
            if where:
                res = await self._req("GET", self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType
                                      + "?count_only=true" + "&filter=" + where)
                res = res[0]["count"]
            else:
                res = await self._req("POST", self.restppUrl + "/builtins/" + self.graphname + ("?realtime=true" if realtime else ""),
                                      data={"function": "stat_vertex_number",
                                            "type": vertexType},
                                      jsonData=True)

                res = res[0]["count"]

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(res))
            logger.debug("exit: getVertexCount (1)")

            return res

        res = await self._req("POST", self.restppUrl + "/builtins/" + self.graphname + ("?realtime=true" if realtime else ""),
                              data={"function": "stat_vertex_number", "type": "*"},
                              jsonData=True)

        ret = _parse_get_vertex_count(res, vertexType, where)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getVertexCount (2)")

        return ret

    async def upsertVertex(self, vertexType: str, vertexId: str, attributes: dict = None) -> int:
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
                    {"name": "Thorin", points: (10, "+"), "bestScore": (83, "max"), "embedding": [0.1, -0.2, 3.1e-2]}
                ```
                For valid values of `<operator>` see xref:tigergraph-server:API:built-in-endpoints.adoc#_operation_codes[Operation codes].

        Returns:
             A single number of accepted (successfully upserted) vertices (0 or 1).

        Endpoint:
            - `POST /graph/{graph_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_upsert_data_to_graph[Upsert data to graph]
        """
        logger.debug("entry: upsertVertex")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        vals = _upsert_attrs(attributes)
        data = json.dumps({"vertices": {vertexType: {vertexId: vals}}})

        ret = await self._req("POST", self.restppUrl + "/graph/" + self.graphname, data=data)
        ret = ret[0]["accepted_vertices"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: upsertVertex")

        return ret

    async def upsertVertices(self, vertexType: str, vertices: list, atomic: bool = False) -> int:
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
                    (3, {"name": "Dwalin", "points": (7, "+"), "bestScore": (35, "max")}),
                    (4, {"name": "Thorin", points: (10, "+"), "bestScore": (83, "max"), "embedding": [0.1, -0.2, 3.1e-2]})
                ]
                ----

                For valid values of `<operator>` see xref:tigergraph-server:API:built-in-endpoints.adoc#_operation_codes[Operation codes].
            atomic:
                The request is an atomic transaction. An atomic transaction means that updates to
                the database contained in the request are all-or-nothing: either all changes are
                successful, or none are successful. This uses the `gsql-atomic-level` header, and sets
                the value to `atomic` if `True`, and `nonatomic` if `False`. Defaults to False.

        Returns:
            A single number of accepted (successfully upserted) vertices (0 or positive integer).

        Endpoint:
            - `POST /graph/{graph_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_upsert_data_to_graph[Upsert data to graph]
        """
        logger.debug("entry: upsertVertices")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        headers = {}
        if atomic:
            headers["gsql-atomic-level"] = "atomic"

        data = {}
        for v in vertices:
            vals = _upsert_attrs(v[1])
            data[v[0]] = vals
        data = json.dumps({"vertices": {vertexType: data}})

        ret = await self._req("POST", self.restppUrl + "/graph/" + self.graphname, data=data, headers=headers)
        ret = ret[0]["accepted_vertices"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: upsertVertices")

        return ret

    async def upsertVertexDataFrame(self, df: 'pd.DataFrame', vertexType: str, v_id: bool = None,
                                    attributes: dict = None, atomic: bool = False) -> int:
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
            atomic:
                The request is an atomic transaction. An atomic transaction means that updates to
                the database contained in the request are all-or-nothing: either all changes are
                successful, or none are successful. This uses the `gsql-atomic-level` header, and sets
                the value to `atomic` if `True`, and `nonatomic` if `False`. Defaults to False.

        Returns:
            The number of vertices upserted.
        """
        logger.debug("entry: upsertVertexDataFrame")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        json_up = _prep_upsert_vertex_dataframe(
            df=df, v_id=v_id, attributes=attributes)
        ret = await self.upsertVertices(vertexType=vertexType, vertices=json_up, atomic=atomic)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: upsertVertexDataFrame")

        return ret

    async def getVertices(self, vertexType: str, select: str = "", where: str = "",
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
        logger.debug("entry: getVertices")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        url = _prep_get_vertices(
            restppUrl=self.restppUrl,
            graphname=self.graphname,
            vertexType=vertexType,
            select=select,
            where=where,
            limit=limit,
            sort=sort,
            timeout=timeout
        )
        ret = await self._req("GET", url)

        if fmt == "json":
            ret = json.dumps(ret)
        elif fmt == "df":
            ret = _vS2DF(ret, withId, withType)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getVertices")

        return ret

    async def getVertexDataFrame(self, vertexType: str, select: str = "", where: str = "",
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
        logger.debug("entry: getVertexDataFrame")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = await self.getVertices(vertexType, select=select, where=where, limit=limit, sort=sort,
                                     fmt="df", withId=True, withType=False, timeout=timeout)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getVertexDataFrame")

        return ret

    async def getVertexDataframe(self, vertexType: str, select: str = "", where: str = "",
                                 limit: Union[int, str] = None, sort: str = "", timeout: int = 0) -> 'pd.DataFrame':
        """DEPRECATED

        Use `getVertexDataFrame()` instead.
        """
        warnings.warn(
            "The `getVertexDataframe()` function is deprecated; use `getVertexDataFrame()` instead.",
            DeprecationWarning)

        return await self.getVertexDataFrame(vertexType, select=select, where=where, limit=limit,
                                             sort=sort, timeout=timeout)

    async def getVerticesById(self, vertexType: str, vertexIds: Union[int, str, list], select: str = "",
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
        logger.debug("entry: getVerticesById")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        vids, url = _prep_get_vertices_by_id(
            restppUrl=self.restppUrl,
            graphname=self.graphname,
            vertexIds=vertexIds,
            vertexType=vertexType
        )

        ret = []
        for vid in vids:
            ret += await self._req("GET", url + _safe_char(vid))

        if fmt == "json":
            ret = json.dumps(ret)
        elif fmt == "df":
            ret = _vS2DF(ret, withId, withType)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getVerticesById")

        return ret

    async def getVertexDataFrameById(self, vertexType: str, vertexIds: Union[int, str, list],
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
        logger.debug("entry: getVertexDataFrameById")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = await self.getVerticesById(vertexType, vertexIds, select, fmt="df", withId=True,
                                         withType=False)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getVertexDataFrameById")

        return ret

    async def getVertexDataframeById(self, vertexType: str, vertexIds: Union[int, str, list],
                                     select: str = "") -> 'pd.DataFrame':
        """DEPRECATED

        Use `getVertexDataFrameById()` instead.
        """
        warnings.warn(
            "The `getVertexDataframeById()` function is deprecated; use `getVertexDataFrameById()` instead.",
            DeprecationWarning)

        return await self.getVertexDataFrameById(vertexType, vertexIds, select)

    async def getVertexStats(self, vertexTypes: Union[str, list], skipNA: bool = False) -> dict:
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
        logger.debug("entry: getVertexStats")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        vts = []
        if vertexTypes == "*":
            vts = await self.getVertexTypes()
        elif isinstance(vertexTypes, str):
            vts = [vertexTypes]
        else:
            vts = vertexTypes

        responses = []
        for vt in vts:
            data = '{"function":"stat_vertex_attr","type":"' + vt + '"}'
            res = await self._req("POST", self.restppUrl + "/builtins/" + self.graphname, data=data, resKey="",
                                  skipCheck=True)
            responses.append((vt, res))

        ret = _parse_get_vertex_stats(responses, skipNA)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getVertexStats")

        return ret

    async def delVertices(self, vertexType: str, where: str = "", limit: str = "", sort: str = "",
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
        logger.debug("entry: delVertices")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        url = _prep_del_vertices(
            restppUrl=self.restppUrl,
            graphname=self.graphname,
            vertexType=vertexType,
            where=where,
            limit=limit,
            sort=sort,
            permanent=permanent,
            timeout=timeout
        )
        ret = await self._req("DELETE", url)
        ret = ret["deleted_vertices"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: delVertices")

        return ret

    async def delVerticesById(self, vertexType: str, vertexIds: Union[int, str, list],
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
        logger.debug("entry: delVerticesById")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        url1, url2, vids = _prep_del_vertices_by_id(
            restppUrl=self.restppUrl,
            graphname=self.graphname,
            vertexIds=vertexIds,
            vertexType=vertexType,
            permanent=permanent,
            timeout=timeout
        )
        ret = 0
        for vid in vids:
            res = await self._req("DELETE", url1 + str(vid) + url2)
            ret += res["deleted_vertices"]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: delVerticesById")

        return ret

    async def delVerticesByType(self, vertexType: str, permanent: bool = False, ack: str = "none") -> int:
        """Deletes all vertices of the specified type.

        Args:
            vertexType:
                The name of the vertex type.
            permanent:
                If true, the deleted vertex IDs can never be inserted back, unless the graph is
                dropped or the graph store is cleared.
            ack:
                If the parameter is set to "none", the delete operation doesn’t need to get acknowledgment from any GPE.
                If it is set to "all" (default), the operation needs to get acknowledgment from all GPEs.
                Other values will raise an error.

        Returns:
            A single number of vertices deleted.

        Usage:
        ```py
        conn.delVerticesByType("Person")
        ```
        """

        logger.debug("entry: delVerticesByType")
        logger.debug("params: " + str(locals()))
        if ack.lower() not in ["none", "all"]:
            raise TigerGraphException("Invalid value for ack parameter. Use 'none' or 'all'.", None)

        url = _prep_del_vertices_by_type(
            restppUrl=self.restppUrl,
            graphname=self.graphname,
            vertexType=vertexType,
            ack=ack,
            permanent=permanent
        )

        ret = await self._delete(url)["deleted_vertices"]

        logger.debug("return: " + str(ret))
        logger.debug("exit: delVerticesByType")

        return ret


    # TODO GET /deleted_vertex_check/{graph_name}

    async def vertexSetToDataFrame(self, vertexSet: dict, withId: bool = True, withType: bool = False) -> 'pd.DataFrame':
        """Converts a vertex set (dictionary) to a pandas DataFrame.

        Args:
            vertexSet:
                The vertex set to convert.
            withId:
                Should the vertex ID be included in the DataFrame?
            withType:
                Should the vertex type be included in the DataFrame?

        Returns:
            The vertex set as a pandas DataFrame.
        """
        logger.debug("entry: vertexSetToDataFrame")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        ret = _vS2DF(vertexSet, withId, withType)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: vertexSetToDataFrame")

        return ret
