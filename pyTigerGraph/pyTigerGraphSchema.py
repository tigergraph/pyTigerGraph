"""Schema Functions.

The functions in this page retrieve information about the graph schema.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object]. 
"""
import json
import re
from typing import Union

from pyTigerGraph.pyTigerGraphBase import pyTigerGraphBase


class pyTigerGraphSchema(pyTigerGraphBase):
    def _getUDTs(self) -> dict:
        """Retrieves all User Defined Types (UDTs) of the graph.

        Returns:
            The list of names of UDTs (defined in the global scope, i.e. not in queries).

        Endpoint:
            GET /gsqlserver/gsql/udtlist
        """
        return self._get(self.gsUrl + "/gsqlserver/gsql/udtlist?graph=" + self.graphname,
            authMode="pwd")

    def _upsertAttrs(self, attributes: dict) -> dict:
        """Transforms attributes (provided as a table) into a hierarchy as expected by the upsert
            functions.

        Args:
            attributes: A dictionary of attribute/value pairs (with an optional operator) in this
                format:
                    {<attribute_name>: <attribute_value>|(<attribute_name>, <operator>), …}

        Returns:
            A dictionary in this format:
                {
                    <attribute_name>: {"value": <attribute_value>},
                    <attribute_name>: {"value": <attribute_value>, "op": <operator>}
                }

        Documentation:
            xref:tigergraph-server:API:built-in-endpoints.adoc#operation-codes[Operation codes]
        """
        if not isinstance(attributes, dict):
            return {}
            # TODO Should return something else or raise exception?
        vals = {}
        for attr in attributes:
            val = attributes[attr]
            if isinstance(val, tuple):
                vals[attr] = {"value": val[0], "op": val[1]}
            else:
                vals[attr] = {"value": val}
        return vals

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
            - `GET /gsqlserver/gsql/schema`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_show_graph_schema_metadata[Show graph schema metadata]
        """
        if not self.schema or force:
            self.schema = self._get(self.gsUrl + "/gsqlserver/gsql/schema?graph=" + self.graphname,
                authMode="pwd")
        if udts and ("UDTs" not in self.schema or force):
            self.schema["UDTs"] = self._getUDTs()
        return self.schema

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
                successful, or none are successful.
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
        if not isinstance(data, str):
            data = json.dumps(data)
        headers = {}
        if atomic:
            headers["gsql-atomic-level"] = "atomic"
        params = {}
        if ackAll:
            params["ack"] = "all"
        if newVertexOnly:
            params["new_vertex_only"] = True
        if vertexMustExist:
            params["vertex_must_exist"] = True
        if updateVertexOnly:
            params["update_vertex_only"] = True
        return self._post(self.restppUrl + "/graph/" + self.graphname, headers=headers, data=data,
            params=params)[0]

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
        ret = {}
        if not (builtin or dynamic or static):
            bui = dyn = sta = True
        else:
            bui = builtin
            dyn = dynamic
            sta = static
        url = self.restppUrl + "/endpoints/" + self.graphname + "?"
        if bui:
            eps = {}
            res = self._get(url + "builtin=true", resKey="")
            for ep in res:
                if not re.search(" /graph/", ep) or re.search(" /graph/{graph_name}/", ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if dyn:
            eps = {}
            res = self._get(url + "dynamic=true", resKey="")
            for ep in res:
                if re.search("^GET /query/" + self.graphname, ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if sta:
            ret.update(self._get(url + "static=true", resKey=""))
        return ret

    # TODO GET /rebuildnow/{graph_name}
