"""Schema Functions.

The functions in this page retrieve information about the graph schema.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import json
import logging
import re

from typing import Union

from pyTigerGraph.common.schema import (
    _prep_upsert_data,
    _prep_get_endpoints
)
from pyTigerGraph.common.exception import TigerGraphException
from pyTigerGraph.pyTigerGraphBase import pyTigerGraphBase

logger = logging.getLogger(__name__)


class pyTigerGraphSchema(pyTigerGraphBase):

    def _getUDTs(self) -> dict:
        """Retrieves all User Defined Types (UDTs) of the graph.

        Returns:
            The list of names of UDTs (defined in the global scope, i.e. not in queries).

        Endpoint:
            GET /gsqlserver/gsql/udtlist (In TigerGraph versions 3.x)
            GET /gsql/v1/udt/tuples (In TigerGraph versions 4.x)
        """
        logger.debug("entry: _getUDTs")

        if self._version_greater_than_4_0():
            res = self._get(self.gsUrl + "/gsql/v1/udt/tuples?graph=" + self.graphname,
                            authMode="pwd")
        else:
            res = self._get(self.gsUrl + "/gsqlserver/gsql/udtlist?graph=" + self.graphname,
                            authMode="pwd")

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: _getUDTs")

        return res

    def getSchema(self, udts: bool = True, force: bool = False) -> dict:
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
            - `GET /gsqlserver/gsql/schema` (In TigerGraph version 3.x)
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_show_graph_schema_metadata[Show graph schema metadata]
            - `GET /gsql/v1/schema/graphs/{graph_name}` (In TigerGraph version 4.x)
        """
        logger.debug("entry: getSchema")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if not self.schema or force:
            if self._version_greater_than_4_0():
                self.schema = self._get(self.gsUrl + "/gsql/v1/schema/graphs/" + self.graphname,
                                        authMode="pwd")
            else:
                self.schema = self._get(self.gsUrl + "/gsqlserver/gsql/schema?graph=" + self.graphname,
                                        authMode="pwd")
        if udts and ("UDTs" not in self.schema or force):
            self.schema["UDTs"] = self._getUDTs()

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(self.schema))
        logger.debug("exit: getSchema")

        return self.schema

    def getSchemaVer(self) -> int:
        """Retrieves the schema version of the graph by running an interpreted query.

        Returns:
            The schema version as an integer.

        Endpoint:
            - `POST /gsqlserver/interpreted_query` (In TigerGraph versions 3.x)
            - `POST /gsql/v1/queries/interpret` (In TigerGraph versions 4.x)
        """
        logger.debug("entry: getSchemaVer")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        # Create the interpreted query to get schema version
        query_text = f'INTERPRET QUERY () FOR GRAPH {self.graphname} {{ PRINT "OK"; }}'

        try:
            # Run the interpreted query
            if self._version_greater_than_4_0():
                ret = self._req("POST", self.gsUrl + "/gsql/v1/queries/interpret",
                                params={}, data=query_text, authMode="pwd", resKey="version",
                                headers={'Content-Type': 'text/plain'})
            else:
                ret = self._req("POST", self.gsUrl + "/gsqlserver/interpreted_query", data=query_text,
                                params={}, authMode="pwd", resKey="version")

            schema_version_int = None
            if isinstance(ret, dict) and "schema" in ret:
                schema_version = ret["schema"]
                try:
                    schema_version_int = int(schema_version)
                except (ValueError, TypeError):
                    logger.warning(f"Schema version '{schema_version}' could not be converted to integer")
            if schema_version_int is None:
                logger.warning("Schema version not found in query result")
            logger.debug("exit: _get_schema_ver")
            return schema_version_int

        except Exception as e:
            logger.error(f"Error getting schema version: {str(e)}")
            raise Exception(f"Failed to get schema version: {str(e)}")

    def upsertData(self, data: Union[str, object], atomic: bool = False, ackAll: bool = False,
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
                the value to `atomic` if `True`, and `nonatomic` if `False`. Default is `False`.
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
        logger.debug("entry: upsertData")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        data, headers, params = _prep_upsert_data(data=data, atomic=atomic, ackAll=ackAll, newVertexOnly=newVertexOnly,
                                                       vertexMustExist=vertexMustExist, updateVertexOnly=updateVertexOnly)

        res = self._post(self.restppUrl + "/graph/" + self.graphname, headers=headers, data=data,
                         params=params)[0]

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: upsertData")

        return res

    def getEndpoints(self, builtin: bool = False, dynamic: bool = False,
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
        logger.debug("entry: getEndpoints")
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
            res = self._req("GET", url + "builtin=true", resKey="")
            for ep in res:
                if not re.search(" /graph/", ep) or re.search(" /graph/{graph_name}/", ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if dyn:
            eps = {}
            res = self._req("GET", url + "dynamic=true", resKey="")
            for ep in res:
                if re.search("^GET /query/" + self.graphname, ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if sta:
            ret.update(self._req("GET", url + "static=true", resKey=""))

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.debug("exit: getEndpoints")

        return ret

    def createGlobalVertices(self, gsql_commands: Union[str, list]) -> dict:
        """Creates global vertices using GSQL commands.

        Args:
            gsql_commands (str or list):
                GSQL CREATE VERTEX statement(s). Can be a single string or list of strings.

        Returns:
            The response from the database containing the creation result.

        Endpoints:
            - `POST /gsql/v1/schema/vertices` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: createGlobalVertices")
        if not self._version_greater_than_4_0():
            logger.debug("exit: createGlobalVertices")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        # Handle single command
        if isinstance(gsql_commands, str):
            gsql_commands = [gsql_commands]
        elif not isinstance(gsql_commands, list):
            raise TigerGraphException("gsql_commands must be a string or list of strings.", 0)

        if not gsql_commands:
            raise TigerGraphException("gsql_commands cannot be empty.", 0)

        # Validate that all commands are CREATE VERTEX statements
        for cmd in gsql_commands:
            if not cmd.strip().upper().startswith("CREATE VERTEX"):
                raise TigerGraphException(f"Invalid GSQL command: {cmd}. Must be a CREATE VERTEX statement.", 0)

        data = {"gsql": gsql_commands}
        params = {"gsql": "true"}
        res = self._post(self.gsUrl+"/gsql/v1/schema/vertices",
                        params=params, data=data, authMode="pwd", resKey="",
                        headers={'Content-Type': 'application/json'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: createGlobalVertices")

        return res

    def createGlobalVerticesJson(self, vertices_config: Union[dict, list]) -> dict:
        """Creates global vertices using JSON configuration.

        Args:
            vertices_config (dict or list):
                JSON configuration for vertex creation. Can be a single vertex config dict
                or a list of vertex config dicts. Each vertex config should include:
                - Name: Vertex type name
                - PrimaryId: Primary ID configuration with AttributeType and AttributeName
                - Attributes: List of attribute configurations
                - Config: Optional configuration (e.g., STATS)

        Returns:
            The response from the database containing the creation result.

        Endpoints:
            - `POST /gsql/v1/schema/vertices` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: createGlobalVerticesJson")
        if not self._version_greater_than_4_0():
            logger.debug("exit: createGlobalVerticesJson")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        # Handle single vertex config
        if isinstance(vertices_config, dict):
            vertices_config = [vertices_config]
        elif not isinstance(vertices_config, list):
            raise TigerGraphException("vertices_config must be a dict or list of dicts.", 0)

        if not vertices_config:
            raise TigerGraphException("vertices_config cannot be empty.", 0)

        # Validate vertex configurations
        for i, vertex_config in enumerate(vertices_config):
            if not isinstance(vertex_config, dict):
                raise TigerGraphException(f"Vertex config at index {i} must be a dict.", 0)

            required_fields = ["Name", "PrimaryId", "Attributes"]
            for field in required_fields:
                if field not in vertex_config:
                    raise TigerGraphException(f"Vertex config at index {i} missing required field: {field}", 0)

        data = {"createVertices": vertices_config}
        res = self._post(self.gsUrl+"/gsql/v1/schema/vertices",
                        data=data, authMode="pwd", resKey="",
                        headers={'Content-Type': 'application/json'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: createGlobalVerticesJson")

        return res

    def addGlobalVerticesToGraph(self, vertex_names: Union[str, list], target_graph: str = None) -> dict:
        """Adds existing global vertices to a local graph.

        Args:
            vertex_names (str or list):
                Name(s) of the global vertices to add to the graph. Can be a single
                vertex name string or a list of vertex names.
            target_graph (str, optional):
                The graph to which the global vertices should be added. If not provided,
                uses the current connection's graphname.

        Returns:
            The response from the database containing the addition result.

        Endpoints:
            - `POST /gsql/v1/schema/vertices` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: addGlobalVerticesToGraph")
        if not self._version_greater_than_4_0():
            logger.debug("exit: addGlobalVerticesToGraph")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        # Handle single vertex name
        if isinstance(vertex_names, str):
            vertex_names = [vertex_names]
        elif not isinstance(vertex_names, list):
            raise TigerGraphException("vertex_names must be a string or list of strings.", 0)

        if not vertex_names:
            raise TigerGraphException("vertex_names cannot be empty.", 0)

        # Validate that all items are strings
        for i, name in enumerate(vertex_names):
            if not isinstance(name, str):
                raise TigerGraphException(f"Vertex name at index {i} must be a string.", 0)

        # Use target_graph or current graphname
        graph_name = target_graph if target_graph is not None else self.graphname

        data = {"addVertices": vertex_names}
        params = {"graph": graph_name}
        res = self._post(self.gsUrl+"/gsql/v1/schema/vertices",
                        params=params, data=data, authMode="pwd", resKey="",
                        headers={'Content-Type': 'application/json'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: addGlobalVerticesToGraph")

        return res

    def validateGraphSchema(self) -> dict:
        """Validate graph schema.

        Check that the current graph schema is valid.

        Args:
            None

        Returns:
            dict: The response from the database containing the schema validation result.

        Endpoints:
            - `POST /gsql/v1/schema/check` (In TigerGraph versions >= 4.0)
        """
        logger.debug("entry: validateGraphSchema")
        if not self._version_greater_than_4_0():
            logger.debug("exit: validateGraphSchema")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        res = self._post(self.gsUrl+"/gsql/v1/schema/check",
                        authMode="pwd", resKey="",
                        headers={'Content-Type': 'text/plain'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: validateGraphSchema")

        return res
