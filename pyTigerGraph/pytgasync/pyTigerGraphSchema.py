"""Schema Functions.

The functions in this page retrieve information about the graph schema.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""

import logging
import re

from typing import Union

from pyTigerGraph.pytgasync.pyTigerGraphBase import AsyncPyTigerGraphBase
from pyTigerGraph.common.schema import (
    _prep_upsert_data,
    _prep_get_endpoints
)

logger = logging.getLogger(__name__)


class AsyncPyTigerGraphSchema(AsyncPyTigerGraphBase):

    async def _getUDTs(self) -> dict:
        """Retrieves all User Defined Types (UDTs) of the graph.

        Returns:
            The list of names of UDTs (defined in the global scope, i.e. not in queries).

        Endpoint:
            GET /gsqlserver/gsql/udtlist (In TigerGraph versions 3.x)
            GET /gsql/v1/udt/tuples (In TigerGraph versions 4.x)
        """
        logger.info("entry: _getUDTs")

        if await self._version_greater_than_4_0():
            res = await self._req("GET", self.gsUrl + "/gsql/v1/udt/tuples?graph=" + self.graphname,
                                  authMode="pwd")
        else:
            res = await self._req("GET", self.gsUrl + "/gsqlserver/gsql/udtlist?graph=" + self.graphname,
                                  authMode="pwd")

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.info("exit: _getUDTs")

        return res

    async def getSchema(self, udts: bool = True, force: bool = False) -> dict:
        """Retrieves the schema metadata (of all vertex and edge type and, if not disabled, the
            User-Defined Type details) of the graph.

        Args:
            udts:
                If `True`, the output includes User-Defined Types in the schema details.
            force:
                If `True`, retrieves the schema metadata again, otherwise returns a cached copy of
                the schema metadata (if they were already fetched previously).

        Returns:
            The schema metadata.

        Endpoint:
            - `GET /gsqlserver/gsql/schema`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_show_graph_schema_metadata[Show graph schema metadata]
            - `GET /gsql/v1/schema/graphs/{graph_name}`
        """
        logger.info("entry: getSchema")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if not self.schema or force:
            if await self._version_greater_than_4_0():
                self.schema = await self._req("GET", self.gsUrl + "/gsql/v1/schema/graphs/" + self.graphname,
                                              authMode="pwd")
            else:
                self.schema = await self._req("GET", self.gsUrl + "/gsqlserver/gsql/schema?graph=" + self.graphname,
                                              authMode="pwd")
        if udts and ("UDTs" not in self.schema or force):
            self.schema["UDTs"] = await self._getUDTs()

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(self.schema))
        logger.info("exit: getSchema")

        return self.schema

    async def upsertData(self, data: Union[str, object], atomic: bool = False, ackAll: bool = False,
                         newVertexOnly: bool = False, vertexMustExist: bool = False,
                         updateVertexOnly: bool = False) -> dict:
        """Upserts data (vertices and edges) from a JSON file or a file with equivalent object structure.

        Args:
            data:
                The data of vertex and edge instances, in a specific format.
            atomic:
                The request is an atomic transaction. An atomic transaction means that updates to
                the database contained in the request are all-or-nothing: either all changes are
                successful, or none are successful. This uses the `gsql-atomic-level` header, and sets
                the value to `atomic` if `True`, and `nonatomic` if `False`.
            ackAll:
                If `True`, the request will return after all GPE instances have acknowledged the
                POST. Otherwise, the request will return immediately after RESTPP processes the POST.
            newVertexOnly:
                If `True`, the request will only insert new vertices and not update existing ones.
            vertexMustExist:
                If `True`, the request will only insert an edge if both the `FROM` and `TO` vertices
                of the edge already exist. If the value is `False`, the request will always insert new
                edges and create the necessary vertices with default values for their attributes.
                Note that this parameter does not affect vertices.
            updateVertexOnly:
                If `True`, the request will only update existing vertices and not insert new
                vertices.

        Returns:
            The result of upsert (number of vertices and edges accepted/upserted).

        Endpoint:
            - `POST /graph/{graph_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_upsert_data_to_graph[Upsert data to graph]
        """
        logger.info("entry: upsertData")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        data, headers, params = _prep_upsert_data(data=data, atomic=atomic, ackAll=ackAll, newVertexOnly=newVertexOnly,
                                                     vertexMustExist=vertexMustExist, updateVertexOnly=updateVertexOnly)

        res = await self._req("POST", self.restppUrl + "/graph/" + self.graphname, headers=headers, data=data,
                              params=params)
        res = res[0]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.info("exit: getSchema")

        return res

    async def getEndpoints(self, builtin: bool = False, dynamic: bool = False,
                           static: bool = False) -> dict:
        """Lists the REST++ endpoints and their parameters.

        Args:
            builtin:
                List the TigerGraph-provided REST++ endpoints.
            dynamic:
                List endpoints for user-installed queries.
            static:
                List static endpoints.

        If none of the above arguments are specified, all endpoints are listed.

        Endpoint:
            - `GET /endpoints/{graph_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_list_all_endpoints[List all endpoints]
        """
        logger.info("entry: getEndpoints")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        bui, dyn, sta, url, ret = _prep_get_endpoints(
            restppUrl=self.restppUrl,
            graphname=self.graphname,
            builtin=builtin,
            dynamic=dynamic,
            static=static
        )
        if bui:
            eps = {}
            res = await self._req("GET", url + "builtin=true", resKey="")
            for ep in res:
                if not re.search(" /graph/", ep) or re.search(" /graph/{graph_name}/", ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if dyn:
            eps = {}
            res = await self._req("GET", url + "dynamic=true", resKey="")
            for ep in res:
                if re.search("^GET /query/" + self.graphname, ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if sta:
            ret.update(await self._req("GET", url + "static=true", resKey=""))

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getEndpoints")

        return ret

    # TODO GET /rebuildnow/{graph_name}
