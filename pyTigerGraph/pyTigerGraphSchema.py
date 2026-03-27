"""Schema Functions.

The functions in this page retrieve information about the graph schema.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import json
import logging
import re
import uuid

from typing import Union

from pyTigerGraph.common.schema import (
    _prep_upsert_data,
    _prep_get_endpoints
)
from pyTigerGraph.common.exception import TigerGraphException
from pyTigerGraph.common.gsql import _wrap_gsql_result, _parse_graph_list
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
            res = self._req("GET", url + "builtin=true", resKey=None)
            for ep in res:
                if not re.search(" /graph/", ep) or re.search(" /graph/{graph_name}/", ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if dyn:
            eps = {}
            res = self._req("GET", url + "dynamic=true", resKey=None)
            for ep in res:
                if re.search("^GET /query/" + self.graphname, ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if sta:
            ret.update(self._req("GET", url + "static=true", resKey=None))

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
                        params=params, data=data, authMode="pwd", resKey=None,
                        headers={'Content-Type': 'application/json'}, jsonData=True)

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
                        data=data, authMode="pwd", resKey=None,
                        headers={'Content-Type': 'application/json'}, jsonData=True)

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
                        params=params, data=data, authMode="pwd", resKey=None,
                        headers={'Content-Type': 'application/json'}, jsonData=True)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: addGlobalVerticesToGraph")

        return res

    def dropVertices(self, vertex_names: Union[str, list], graph: str = None,
                     ignoreErrors: bool = False) -> dict:
        """Drops vertex types from a graph or drops global vertex types.

        Args:
            vertex_names (str or list):
                Name(s) of the vertex types to drop. Can be a single string or a list
                of strings. Use ``"all"`` to drop all vertices.
            graph (str, optional):
                The graph from which vertex types should be dropped.
                If ``None`` and the connection has a ``graphname`` set,
                that graph is used.  If neither is set, drops global
                vertex types.
            ignoreErrors (bool):
                If ``True``, suppress exceptions (e.g. when some vertices do not exist)
                and return the error as a dict instead. Defaults to ``False``.

        Returns:
            The response from the database containing the drop result.

        Raises:
            `TigerGraphException` if the function is called on TigerGraph < 4.0,
            or if the drop fails and ``ignoreErrors`` is ``False``.

        Endpoints:
            - ``DELETE /gsql/v1/schema/vertices`` (In TigerGraph versions >= 4.0)

        See https://docs.tigergraph.com/tigergraph-server/4.2/api/gsql-endpoints#_drop_vertices
        """
        logger.debug("entry: dropVertices")
        if not self._version_greater_than_4_0():
            logger.debug("exit: dropVertices")
            raise TigerGraphException(
                "This function is only supported on versions of TigerGraph >= 4.0.", 0)

        if isinstance(vertex_names, list):
            if not vertex_names:
                raise TigerGraphException("vertex_names cannot be empty.", 0)
            vertex_param = ",".join(vertex_names)
        elif isinstance(vertex_names, str):
            vertex_param = vertex_names
        else:
            raise TigerGraphException("vertex_names must be a string or list of strings.", 0)

        gname = graph or self.graphname

        params = {"vertex": vertex_param}
        if gname:
            params["graph"] = gname

        if not ignoreErrors:
            res = self._delete(self.gsUrl + "/gsql/v1/schema/vertices",
                               params=params, authMode="pwd", resKey=None)
        else:
            try:
                res = self._delete(self.gsUrl + "/gsql/v1/schema/vertices",
                                   params=params, authMode="pwd", resKey=None)
            except Exception:
                # Batch may fail if some vertices don't exist; retry individually.
                names = vertex_param.split(",") if "," in vertex_param else [vertex_param]
                dropped = []
                failed = []
                for name in names:
                    try:
                        self._delete(self.gsUrl + "/gsql/v1/schema/vertices",
                                     params={**params, "vertex": name},
                                     authMode="pwd", resKey=None)
                        dropped.append(name)
                    except Exception:
                        failed.append(name)
                res = {"error": len(failed) > 0,
                       "message": f"Dropped: {dropped}. Failed: {failed}."}

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: dropVertices")

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
                        authMode="pwd", resKey=None,
                        headers={'Content-Type': 'text/plain'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: validateGraphSchema")

        return res

    def createGraph(self, graphName: str,
                    vertexTypes: list = None,
                    edgeTypes: list = None) -> dict:
        """Creates a graph, optionally including existing global vertex/edge types.

        Args:
            graphName:
                Name of the graph to create.
            vertexTypes:
                Optional list of existing global vertex type names to include
                in the graph.  Pass ``["*"]`` to include all global types.
            edgeTypes:
                Optional list of existing global edge type names to include
                in the graph.

        Returns:
            A dict with at least a ``"message"`` key describing the outcome.

        Endpoints:
            - `POST /gsql/v1/schema/graphs` (In TigerGraph versions >= 4.0)
            - Falls back to GSQL ``CREATE GRAPH`` for TigerGraph versions < 4.0
        """
        logger.debug("entry: createGraph")

        type_names = []
        if vertexTypes:
            type_names.extend(vertexTypes)
        if edgeTypes:
            type_names.extend(edgeTypes)
        type_list = ", ".join(type_names)
        gsql_cmd = f"CREATE GRAPH {graphName}({type_list})"

        if self._version_greater_than_4_0():
            data = {"gsql": gsql_cmd}
            res = self._post(self.gsUrl + "/gsql/v1/schema/graphs",
                            data=data, authMode="pwd", resKey=None,
                            params={"gsql": "true", "graphName": graphName},
                            headers={"Content-Type": "application/json"},
                            jsonData=True)
        else:
            res = _wrap_gsql_result(self.gsql(gsql_cmd))

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: createGraph")

        return res

    def dropGraph(self, graphName: str) -> dict:
        """Drops a graph and all its data.

        Args:
            graphName:
                Name of the graph to drop.

        Returns:
            A dict with at least a ``"message"`` key describing the outcome.

        Endpoints:
            - `DELETE /gsql/v1/schema/graphs/{graphName}` (In TigerGraph versions >= 4.0)
            - Falls back to GSQL ``DROP GRAPH`` for TigerGraph versions < 4.0
        """
        logger.debug("entry: dropGraph")

        if self._version_greater_than_4_0():
            res = self._delete(self.gsUrl + "/gsql/v1/schema/graphs/" + graphName,
                              authMode="pwd", resKey=None,
                              headers={'Content-Type': 'application/json'})
        else:
            res = _wrap_gsql_result(
                self.gsql(f"DROP GRAPH {graphName}"))

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: dropGraph")

        return res

    def listGraphs(self) -> list:
        """Lists all graphs in the database.

        Returns:
            A list of dicts, each with at least a ``"GraphName"`` key.

        Endpoints:
            - `GET /gsql/v1/schema/graphs` (In TigerGraph versions >= 4.0)
            - Falls back to GSQL ``SHOW GRAPH *`` for TigerGraph versions < 4.0
        """
        logger.debug("entry: listGraphs")

        if self._version_greater_than_4_0():
            res = self._get(self.gsUrl + "/gsql/v1/schema/graphs",
                           authMode="pwd", resKey="graphs")
        else:
            res = _parse_graph_list(self.gsql("SHOW GRAPH *"))

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: listGraphs")

        return res

    def runSchemaChange(self, gsqlStatements: Union[str, list, dict],
                        graphName: str = None, force: bool = False) -> dict:
        """Runs schema change statements directly (without creating a named job).

        Supports both local (graph-scoped) and global schema changes.

        Args:
            gsqlStatements:
                Schema change specification in one of two formats:

                **dict (JSON format, TG >= 4.0 only)** — Sent directly to
                ``POST /gsql/v1/schema/change`` as JSON.  Supports keys such as
                ``addVertexTypes``, ``dropVertexTypes``, ``alterVertexTypes``,
                ``addEdgeTypes``, ``dropEdgeTypes``, ``alterEdgeTypes``.

                **str or list (GSQL DDL)** — Wrapped in a GSQL schema change
                job and executed via ``gsql()``.  Works on all TigerGraph
                versions.

            graphName:
                Target graph name for a local schema change.  If ``None`` and
                the connection has no ``graphname`` set, a **global** schema
                change is executed instead.

            force:
                If ``True``, abort any loading jobs that conflict with the
                schema change.  Only applies to the JSON (dict) path.

        Returns:
            A dict with at least a ``"message"`` key describing the outcome.

        Endpoints:
            - ``POST /gsql/v1/schema/change`` with JSON body (TG >= 4.0)
            - GSQL schema change job via ``gsql()`` (all versions)
        """
        logger.debug("entry: runSchemaChange")

        gname = graphName or self.graphname

        if isinstance(gsqlStatements, dict):
            if not self._version_greater_than_4_0():
                raise TigerGraphException(
                    "JSON-format schema changes require TigerGraph >= 4.0. "
                    "Pass GSQL DDL statements as a string instead.")
            params = {}
            if gname:
                params["graph"] = gname
            if force:
                params["force"] = "true"
            res = self._post(self.gsUrl + "/gsql/v1/schema/change",
                            params=params, data=json.dumps(gsqlStatements),
                            authMode="pwd", resKey=None,
                            headers={'Content-Type': 'application/json'})
        else:
            if isinstance(gsqlStatements, list):
                gsqlStatements = "\n".join(
                    s if s.rstrip().endswith(";") else s + ";"
                    for s in gsqlStatements
                )
            job_name = f"schema_change_{uuid.uuid4().hex[:8]}"
            if gname:
                gsql_cmd = (
                    f"USE GRAPH {gname}\n"
                    f"CREATE SCHEMA_CHANGE JOB {job_name} FOR GRAPH {gname} {{\n"
                    f"    {gsqlStatements}\n"
                    f"}}\n"
                    f"RUN SCHEMA_CHANGE JOB {job_name}\n"
                    f"DROP JOB {job_name}"
                )
            else:
                gsql_cmd = (
                    f"CREATE GLOBAL SCHEMA_CHANGE JOB {job_name} {{\n"
                    f"    {gsqlStatements}\n"
                    f"}}\n"
                    f"RUN GLOBAL SCHEMA_CHANGE JOB {job_name}\n"
                    f"DROP JOB {job_name}"
                )
            res = _wrap_gsql_result(self.gsql(gsql_cmd))

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: runSchemaChange")

        return res

    def createSchemaChangeJob(self, jobName: str, statements: Union[str, list, dict],
                              graphName: str = None) -> dict:
        """Creates a named schema change job without running it.

        Args:
            jobName:
                Name for the schema change job.

            statements:
                Schema change specification in one of two formats:

                **dict (JSON format)** — Sent as JSON to
                ``POST /gsql/v1/schema/jobs/{jobName}``.
                For global jobs the dict should contain a ``"graphs"`` key;
                for local jobs it should contain keys such as
                ``addVertexTypes``, ``dropVertexTypes``, etc.

                **str or list (GSQL DDL)** — Individual DDL statements
                (e.g. ``"ADD VERTEX Foo (...)"``).
                They are wrapped in a ``CREATE [GLOBAL] SCHEMA_CHANGE JOB``
                command and sent via the ``?gsql=true`` parameter.

            graphName:
                Target graph for a local schema change job.  If ``None`` and
                the connection has no ``graphname`` set, a **global** job is
                created.

        Returns:
            The server response dict.

        Endpoint:
            - ``POST /gsql/v1/schema/jobs/{jobName}``
        """
        logger.debug("entry: createSchemaChangeJob")

        gname = graphName or self.graphname

        url = self.gsUrl + "/gsql/v1/schema/jobs/" + jobName

        if isinstance(statements, dict):
            params = {}
            if gname:
                params["graph"] = gname
            res = self._post(url, params=params,
                            data=json.dumps(statements),
                            authMode="pwd", resKey=None,
                            headers={'Content-Type': 'application/json'})
        else:
            if isinstance(statements, list):
                statements = "\n".join(
                    s if s.rstrip().endswith(";") else s + ";"
                    for s in statements
                )
            if gname:
                gsql_cmd = (
                    f"CREATE SCHEMA_CHANGE JOB {jobName} FOR GRAPH {gname} {{\n"
                    f"    {statements}\n"
                    f"}}"
                )
                params = {"gsql": "true", "graph": gname}
            else:
                gsql_cmd = (
                    f"CREATE GLOBAL SCHEMA_CHANGE JOB {jobName} {{\n"
                    f"    {statements}\n"
                    f"}}"
                )
                params = {"gsql": "true", "type": "global"}
            res = self._post(url, params=params,
                            data=json.dumps({"gsql": gsql_cmd}),
                            authMode="pwd", resKey=None,
                            headers={'Content-Type': 'text/plain'})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: createSchemaChangeJob")

        return res

    def getSchemaChangeJobs(self, jobName: str = None, graphName: str = None,
                            jsonFormat: bool = True) -> Union[dict, list]:
        """Retrieves schema change jobs.

        Args:
            jobName:
                Name of a specific job to retrieve.  If ``None``, all schema
                change jobs are returned.

            graphName:
                Graph whose local jobs to retrieve.  If ``None`` and the
                connection has no ``graphname`` set, global jobs are returned.

            jsonFormat:
                If ``True`` (default), requests JSON-formatted output from the
                server.

        Returns:
            A dict (single job) or list (all jobs) describing the schema
            change job(s).

        Endpoints:
            - ``GET /gsql/v1/schema/jobs`` (all jobs)
            - ``GET /gsql/v1/schema/jobs/{jobName}`` (single job)
        """
        logger.debug("entry: getSchemaChangeJobs")

        gname = graphName or self.graphname

        url = self.gsUrl + "/gsql/v1/schema/jobs"
        if jobName:
            url += "/" + jobName

        params = {}
        if gname:
            params["graph"] = gname
        if jsonFormat:
            params["json"] = "true"

        res = self._get(url, params=params, authMode="pwd", resKey=None)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: getSchemaChangeJobs")

        return res

    def runSchemaChangeJob(self, jobName: str, graphName: str = None,
                           force: bool = False) -> dict:
        """Runs an existing (already created) schema change job.

        Args:
            jobName:
                Name of the schema change job to run.

            graphName:
                Graph on which to run the local job.  If ``None`` and the
                connection has no ``graphname`` set, runs a global job.

            force:
                If ``True``, abort any loading jobs that conflict with the
                schema change.

        Returns:
            The server response dict.

        Endpoint:
            - ``PUT /gsql/v1/schema/jobs/{jobName}``
        """
        logger.debug("entry: runSchemaChangeJob")

        gname = graphName or self.graphname

        query_parts = []
        if gname:
            query_parts.append(f"graph={gname}")
        if force:
            query_parts.append("force=true")

        url = self.gsUrl + "/gsql/v1/schema/jobs/" + jobName
        if query_parts:
            url += "?" + "&".join(query_parts)

        res = self._put(url, authMode="pwd", resKey=None)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: runSchemaChangeJob")

        return res

    def dropSchemaChangeJobs(self, jobNames: Union[str, list],
                             graphName: str = None) -> dict:
        """Drops one or more schema change jobs.

        Args:
            jobNames:
                A single job name (str) or a list of job names to drop.

            graphName:
                Graph whose local jobs to drop.  If ``None`` and the
                connection has no ``graphname`` set, drops global jobs.

        Returns:
            The server response dict.

        Endpoint:
            - ``DELETE /gsql/v1/schema/jobs``
        """
        logger.debug("entry: dropSchemaChangeJobs")

        gname = graphName or self.graphname

        if isinstance(jobNames, list):
            job_param = ",".join(jobNames)
        else:
            job_param = jobNames

        params = {"jobName": job_param}
        if gname:
            params["graph"] = gname

        res = self._delete(self.gsUrl + "/gsql/v1/schema/jobs",
                          params=params, authMode="pwd", resKey=None)

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.debug("exit: dropSchemaChangeJobs")

        return res
