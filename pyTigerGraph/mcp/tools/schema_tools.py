# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Schema operation tools for MCP."""

import json
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from mcp.types import Tool, TextContent

from ..tool_names import TigerGraphToolName
from ..connection_manager import get_connection
from ..response_formatter import format_success, format_error, gsql_has_error
from pyTigerGraph.common.exception import TigerGraphException


# =============================================================================
# Global Schema Operations (Database level - operates on global schema)
# =============================================================================

class GetGlobalSchemaToolInput(BaseModel):
    """Input schema for getting the global schema (all global vertex/edge types, graphs, etc.)."""
    profile: Optional[str] = Field(None, description="Connection profile name. If not provided, uses TG_PROFILE env var or 'default'. Use 'list_connections' to see available profiles.")


# =============================================================================
# Graph Operations (Database level - operates on graphs within the database)
# =============================================================================

class ListGraphsToolInput(BaseModel):
    """Input schema for listing all graph names in the database."""
    profile: Optional[str] = Field(None, description="Connection profile name. If not provided, uses TG_PROFILE env var or 'default'. Use 'list_connections' to see available profiles.")


class CreateGraphToolInput(BaseModel):
    """Input schema for creating a new graph with its schema."""
    profile: Optional[str] = Field(None, description="Connection profile name. If not provided, uses TG_PROFILE env var or 'default'. Use 'list_connections' to see available profiles.")
    graph_name: str = Field(..., description="Name of the new graph to create.")
    vertex_types: List[Dict[str, Any]] = Field(..., description="List of vertex type definitions for this graph.")
    edge_types: List[Dict[str, Any]] = Field(default_factory=list, description="List of edge type definitions for this graph.")


class DropGraphToolInput(BaseModel):
    """Input schema for dropping a graph from the database."""
    profile: Optional[str] = Field(None, description="Connection profile name. If not provided, uses TG_PROFILE env var or 'default'. Use 'list_connections' to see available profiles.")
    graph_name: str = Field(..., description="Name of the graph to drop.")


class ClearGraphDataToolInput(BaseModel):
    """Input schema for clearing all data from a graph (keeps schema structure)."""
    profile: Optional[str] = Field(None, description="Connection profile name. If not provided, uses TG_PROFILE env var or 'default'. Use 'list_connections' to see available profiles.")
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    vertex_type: Optional[str] = Field(None, description="Type of vertices to clear. If not provided, clears all data.")
    confirm: bool = Field(False, description="Must be True to confirm the deletion. This is a destructive operation.")


# =============================================================================
# Schema Operations (Graph level - operates on schema within a specific graph)
# =============================================================================

class GetGraphSchemaToolInput(BaseModel):
    """Input schema for getting a specific graph's schema (raw JSON)."""
    profile: Optional[str] = Field(None, description="Connection profile name. If not provided, uses TG_PROFILE env var or 'default'. Use 'list_connections' to see available profiles.")
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")


class ShowGraphDetailsToolInput(BaseModel):
    """Input schema for showing details of a graph (schema, queries, jobs)."""
    profile: Optional[str] = Field(None, description="Connection profile name. If not provided, uses TG_PROFILE env var or 'default'. Use 'list_connections' to see available profiles.")
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    detail_type: Optional[str] = Field(
        None,
        description=(
            "Which details to show. Options: 'schema' (vertex/edge types), "
            "'query' (installed queries), 'loading_job' (loading jobs), "
            "'data_source' (data sources). "
            "If not provided, shows everything (equivalent to GSQL LS)."
        ),
    )


# =============================================================================
# Global Schema Operation Tools (Database level)
# =============================================================================

get_global_schema_tool = Tool(
    name=TigerGraphToolName.GET_GLOBAL_SCHEMA,
    description=(
        "Get the complete global schema including all global vertex types, edge types, graphs, and their member types. "
        "Runs GSQL 'LS' command.\n\n"
        
        "**Use When:**\n"
        "  • You need to see all graphs and their schemas at once\n"
        "  • Understanding the complete database structure\n"
        "  • Finding all available vertex and edge types across all graphs\n"
        "  • Database-level schema exploration\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{}\n"
        "```\n"
        "(No parameters needed)\n\n"
        
        "**Tips:**\n"
        "  • Returns output from GSQL 'LS' command\n"
        "  • Shows all graphs in the database\n"
        "  • For single graph details, use 'show_graph_details' instead\n"
        "  • Useful for database administrators\n\n"
        
        "**Related Tools:** list_graphs, show_graph_details, get_graph_schema"
    ),
    inputSchema=GetGlobalSchemaToolInput.model_json_schema(),
)

# =============================================================================
# Graph Operation Tools (Database level)
# =============================================================================

list_graphs_tool = Tool(
    name=TigerGraphToolName.LIST_GRAPHS,
    description=(
        "List all graph names in the TigerGraph database. "
        "Returns only graph names — no schema, query, or job details.\n\n"

        "**Use When:**\n"
        "  • Discovering what graphs exist in the database\n"
        "  • First step when connecting to a new TigerGraph instance\n"
        "  • Verifying a graph was created or dropped successfully\n\n"

        "**Quick Start:**\n"
        "```json\n"
        "{}\n"
        "```\n"
        "(No parameters needed)\n\n"

        "**Next Steps:**\n"
        "  • Use 'show_graph_details' to see everything under a graph (schema, queries, jobs)\n"
        "  • Use 'get_graph_schema' to get just the schema (vertex/edge types)\n\n"

        "**Related Tools:** show_graph_details, get_graph_schema, create_graph"
    ),
    inputSchema=ListGraphsToolInput.model_json_schema(),
)

create_graph_tool = Tool(
    name=TigerGraphToolName.CREATE_GRAPH,
    description=(
        "Create a new graph in the TigerGraph database with its schema (vertex types and edge types). "
        "Each graph has its own independent schema.\n\n"
        
        "**Use When:**\n"
        "  • Creating a new graph from scratch\n"
        "  • Setting up a graph with specific vertex and edge types\n"
        "  • Initializing a new project or data model\n"
        "  • Defining the structure before loading data\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "graph_name": "SocialNetwork",\n'
        '  "vertex_types": [\n'
        '    {\n'
        '      "name": "Person",\n'
        '      "primary_id": "id",\n'
        '      "primary_id_type": "STRING",\n'
        '      "attributes": [\n'
        '        {"name": "name", "type": "STRING"},\n'
        '        {"name": "age", "type": "INT"}\n'
        "      ]\n"
        "    }\n"
        "  ],\n"
        '  "edge_types": [\n'
        '    {\n'
        '      "name": "FOLLOWS",\n'
        '      "from_vertex": "Person",\n'
        '      "to_vertex": "Person",\n'
        '      "directed": true,\n'
        '      "attributes": [\n'
        '        {"name": "since", "type": "STRING"}\n'
        "      ]\n"
        "    }\n"
        "  ]\n"
        "}\n"
        "```\n\n"
        
        "**Common Workflow:**\n"
        "1. Use 'list_graphs' to check if graph name is available\n"
        "2. Design your vertex types and edge types\n"
        "3. Call 'create_graph' with the schema\n"
        "4. Use 'show_graph_details' to verify it was created correctly\n"
        "5. Start loading data with 'add_node' and 'add_edge'\n\n"
        
        "**Vertex Primary Key Options:**\n"
        "  • Default: auto-generates ``PRIMARY_ID id STRING`` with ``primary_id_as_attribute``\n"
        "  • Explicit PRIMARY_ID: set ``primary_id`` (string) and ``primary_id_type`` on vertex type\n"
        "  • PRIMARY KEY mode: set ``primary_key: true`` on one attribute (not GraphStudio compatible)\n"
        "  • Composite key: set ``primary_id`` to a list of attribute names, e.g. ``[\"title\", \"year\"]``\n"
        "    All listed attributes must exist in the attribute list (not GraphStudio compatible)\n"
        "  • The key is always queryable as a regular attribute\n\n"
        
        "**Tips:**\n"
        "  • Define all vertex types before edge types\n"
        "  • Edge types reference vertex types by name\n"
        "  • Set 'directed': false on edge types for undirected edges (default: directed)\n"
        "  • Consider using 'get_workflow' for step-by-step guidance\n\n"
        
        "**Related Tools:** list_graphs, show_graph_details, drop_graph"
    ),
    inputSchema=CreateGraphToolInput.model_json_schema(),
)

drop_graph_tool = Tool(
    name=TigerGraphToolName.DROP_GRAPH,
    description=(
        "Drop (delete) a graph and its schema from the TigerGraph database. "
        "**This permanently removes the graph, its schema, and all data.**\n\n"
        
        "**Use When:**\n"
        "  • Removing a graph that's no longer needed\n"
        "  • Cleaning up test graphs\n"
        "  • Starting fresh with a new schema\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "graph_name": "TestGraph"\n'
        "}\n"
        "```\n\n"
        
        "**Warning: DANGER:**\n"
        "  • This deletes EVERYTHING: schema, vertices, edges, queries, loading jobs\n"
        "  • Operation is PERMANENT and cannot be undone\n"
        "  • Double-check the graph_name before executing\n"
        "  • Consider using 'clear_graph_data' if you only want to remove data\n\n"
        
        "**Tips:**\n"
        "  • Use 'list_graphs' first to confirm the graph name\n"
        "  • For production graphs, always backup first\n"
        "  • To keep schema but clear data, use 'clear_graph_data'\n\n"
        
        "**Related Tools:** create_graph, clear_graph_data, list_graphs"
    ),
    inputSchema=DropGraphToolInput.model_json_schema(),
)

clear_graph_data_tool = Tool(
    name=TigerGraphToolName.CLEAR_GRAPH_DATA,
    description=(
        "Clear all data (vertices and edges) from a specific graph while keeping its schema structure intact. "
        "**This is a destructive operation that removes all graph data.**\n\n"
        
        "**Use When:**\n"
        "  • Resetting a graph to empty state\n"
        "  • Clearing test data before loading production data\n"
        "  • Starting data reload with same schema\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "graph_name": "MyGraph",\n'
        '  "confirm": true\n'
        "}\n"
        "```\n\n"
        
        "**WARNING:**\n"
        "  • Deletes ALL vertices and edges in the graph\n"
        "  • Operation is PERMANENT and cannot be undone\n"
        "  • Must set 'confirm': true to execute\n"
        "  • Schema (vertex/edge types) remains intact\n\n"
        
        "**Tips:**\n"
        "  • Preserves schema, only clears data\n"
        "  • To delete everything including schema, use 'drop_graph'\n"
        "  • Always backup important data first\n"
        "  • Can specify 'vertex_type' to clear only specific type\n\n"
        
        "**Related Tools:** drop_graph, get_vertex_count, delete_nodes"
    ),
    inputSchema=ClearGraphDataToolInput.model_json_schema(),
)

# =============================================================================
# Schema Operation Tools (Graph level - operates on a specific graph's schema)
# =============================================================================

get_graph_schema_tool = Tool(
    name=TigerGraphToolName.GET_GRAPH_SCHEMA,
    description=(
        "Get the schema of a specific graph — vertex types, edge types, and their "
        "attributes — as structured JSON. Returns schema only, not queries or jobs.\n\n"

        "**Use When:**\n"
        "  • You need to know vertex/edge types and their attributes\n"
        "  • Building or validating queries against the schema\n"
        "  • Programmatic schema inspection or comparison\n\n"

        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "graph_name": "SocialNetwork"\n'
        "}\n"
        "```\n\n"

        "**Tips:**\n"
        "  • Returns structured JSON (vertex types, edge types, attributes)\n"
        "  • For a full listing including queries and jobs, use 'show_graph_details'\n"
        "  • For just graph names, use 'list_graphs'\n\n"

        "**Related Tools:** show_graph_details (full listing), list_graphs (names only)"
    ),
    inputSchema=GetGraphSchemaToolInput.model_json_schema(),
)

show_graph_details_tool = Tool(
    name=TigerGraphToolName.SHOW_GRAPH_DETAILS,
    description=(
        "Show details of a specific graph. By default shows everything (schema, queries, "
        "loading jobs, data sources). Use 'detail_type' to show only a specific category.\n\n"

        "**Use When:**\n"
        "  • You need a full picture of a graph (schema + queries + jobs)\n"
        "  • Starting work with a graph (call this first!)\n"
        "  • Checking which queries or loading jobs are installed\n"
        "  • Debugging schema or job issues\n\n"

        "**Quick Start:**\n"
        "```json\n"
        '{ "graph_name": "SocialNetwork" }\n'
        "```\n"
        "(Shows everything under the graph)\n\n"

        "**Filter by category:**\n"
        "```json\n"
        '{ "graph_name": "SocialNetwork", "detail_type": "query" }\n'
        "```\n"
        "Options: 'schema', 'query', 'loading_job', 'data_source'\n\n"

        "**Tips:**\n"
        "  • No detail_type → shows all (GSQL ``LS`` output)\n"
        "  • For structured JSON schema, use 'get_graph_schema' instead\n"
        "  • For just graph names, use 'list_graphs'\n"
        "  • For vector attributes, use 'list_vector_attributes' instead\n\n"

        "**Related Tools:** get_graph_schema (schema JSON), list_graphs (names only), "
        "list_vector_attributes (vector attribute details)"
    ),
    inputSchema=ShowGraphDetailsToolInput.model_json_schema(),
)


async def get_graph_schema(profile: Optional[str] = None, graph_name: Optional[str] = None) -> List[TextContent]:
    """Get the schema of a specific graph."""
    try:
        conn = get_connection(profile=profile, graph_name=graph_name)
        schema = await conn.getSchema()
        
        vertex_count = len(schema.get("VertexTypes", []))
        edge_count = len(schema.get("EdgeTypes", []))
        
        return format_success(
            operation="get_graph_schema",
            summary=f"Success: Schema retrieved for graph '{conn.graphname}'",
            data={
                "graph_name": conn.graphname,
                "schema": schema,
                "vertex_type_count": vertex_count,
                "edge_type_count": edge_count
            },
            suggestions=[
                f"Full listing (schema + queries + jobs): show_graph_details(graph_name='{conn.graphname}')",
                "Start working with data: add_node(...) or add_edge(...)",
            ]
        )
    except Exception as e:
        return format_error(
            operation="get_graph_schema",
            error=e,
            context={"graph_name": graph_name or "default"}
        )


def _format_attr(attr: Dict[str, Any]) -> str:
    """Format a single attribute definition for GSQL DDL."""
    aname = attr.get("name", "")
    atype = attr.get("type", "STRING")
    default = attr.get("default")
    part = f"{aname} {atype}"
    if default is not None:
        if isinstance(default, str):
            part += f' DEFAULT "{default}"'
        else:
            part += f" DEFAULT {default}"
    return part


def _build_vertex_stmt(vtype: Dict[str, Any], keyword: str = "ADD") -> tuple:
    """Build a VERTEX DDL statement from a vertex-type dict.

    Supports three TigerGraph primary-key modes (see
    https://docs.tigergraph.com/gsql-ref/4.2/ddl-and-loading/defining-a-graph-schema#_primary_idkey_options):

    1. **Composite PRIMARY KEY** (``primary_id`` is a list):
       ``ADD VERTEX V (a1 T1, a2 T2, PRIMARY KEY (a1, a2))``
       All listed attributes must exist in the attribute list.
       Not GraphStudio-compatible.

    2. **Single-attribute PRIMARY KEY** (attribute has ``primary_key: true``):
       ``ADD VERTEX V (id STRING PRIMARY KEY, …)``
       Not GraphStudio-compatible.

    3. **PRIMARY_ID** (default, GraphStudio-compatible):
       ``ADD VERTEX V (PRIMARY_ID id STRING, …) WITH primary_id_as_attribute="true"``
       Used when ``primary_id`` is a single string, an attribute has
       ``is_primary_id: true``, or when no key is specified (defaults to ``id``).
       ``primary_id_as_attribute="true"`` is always set so the ID is
       queryable as a regular attribute.

    Args:
        vtype: Vertex type definition with *name*, *attributes*, and
               optional *primary_id* (``str`` or ``list[str]``) /
               *primary_id_type*.
        keyword: ``"ADD"`` for schema-change jobs, ``"CREATE"`` for global DDL.

    Returns:
        ``(vertex_name, statement_string)`` or ``(None, None)`` if *name*
        is missing.

    Raises:
        ValueError: If a composite key references attributes not present
            in the attribute list.
    """
    vname = vtype.get("name", "")
    if not vname:
        return None, None

    attrs = vtype.get("attributes", [])
    attr_map = {a.get("name"): a for a in attrs}
    primary_id = vtype.get("primary_id", None)
    primary_id_type = vtype.get("primary_id_type", "STRING")

    # ── Mode 1: Composite PRIMARY KEY ────────────────────────────────
    # Triggered when primary_id is a non-empty list of attribute names.
    # Syntax: ADD VERTEX V (a1 T1, a2 T2, …, PRIMARY KEY (a1, a2))
    if isinstance(primary_id, list) and primary_id:
        missing = [k for k in primary_id if k not in attr_map]
        if missing:
            raise ValueError(
                f"Composite PRIMARY KEY for vertex '{vname}' references "
                f"attributes not defined in the attribute list: {missing}. "
                f"Available attributes: {list(attr_map.keys())}"
            )
        attr_parts = [_format_attr(a) for a in attrs]
        key_list = ", ".join(primary_id)
        attr_parts.append(f"PRIMARY KEY ({key_list})")
        stmt = f"{keyword} VERTEX {vname} ({', '.join(attr_parts)})"
        return vname, stmt

    # ── Mode 2: Single-attribute PRIMARY KEY ─────────────────────────
    # Triggered when an attribute has "primary_key": true.
    # Syntax: ADD VERTEX V (id STRING PRIMARY KEY, other_attr TYPE, …)
    pk_attr = None
    for attr in attrs:
        if attr.get("primary_key"):
            pk_attr = attr
            break

    if pk_attr:
        pk_name = pk_attr["name"]
        pk_type = pk_attr.get("type", "STRING")
        other_attrs = [a for a in attrs if a.get("name") != pk_name]

        parts = [f"{pk_name} {pk_type} PRIMARY KEY"]
        parts.extend(_format_attr(a) for a in other_attrs)

        stmt = f"{keyword} VERTEX {vname} ({', '.join(parts)})"
        return vname, stmt

    # ── Mode 3: PRIMARY_ID + primary_id_as_attribute="true" ──────────
    # Default mode — always ensures the ID is queryable as an attribute.
    #
    # Resolve the primary ID name from (in priority order):
    #   a) explicit ``primary_id`` string on the vertex type dict
    #   b) an attribute with ``is_primary_id: true``
    #   c) default name ``"id"``
    primary_id_name: Optional[str] = primary_id if isinstance(primary_id, str) and primary_id else None

    if not primary_id_name:
        for attr in attrs:
            if attr.get("is_primary_id"):
                primary_id_name = attr["name"]
                primary_id_type = attr.get("type", "STRING")
                break

    if not primary_id_name:
        primary_id_name = "id"
        if "id" in attr_map:
            primary_id_type = attr_map["id"].get("type", "STRING")
    elif primary_id_name in attr_map:
        primary_id_type = attr_map[primary_id_name].get("type", primary_id_type)

    # Remaining attributes (everything except the one used as PRIMARY_ID)
    non_pk_attrs = [a for a in attrs if a.get("name") != primary_id_name]
    attr_parts = [_format_attr(a) for a in non_pk_attrs]

    stmt = f"{keyword} VERTEX {vname} (PRIMARY_ID {primary_id_name} {primary_id_type}"
    if attr_parts:
        stmt += ", " + ", ".join(attr_parts)
    stmt += ') WITH primary_id_as_attribute="true"'
    return vname, stmt


def _build_edge_stmt(etype: Dict[str, Any], keyword: str = "ADD") -> tuple:
    """Build an EDGE DDL statement from an edge-type dict.

    Args:
        etype: Edge type definition with *name*, *from_vertex*, *to_vertex*,
               optional *directed* / *is_directed*, and *attributes*.
        keyword: ``"ADD"`` for schema-change jobs, ``"CREATE"`` for global DDL.

    Returns:
        ``(edge_name, statement_string)`` or ``(None, None)`` if *name*
        is missing.
    """
    ename = etype.get("name", "")
    if not ename:
        return None, None

    from_type = etype.get("from_vertex", "")
    to_type = etype.get("to_vertex", "")
    is_directed = etype.get("directed", etype.get("is_directed", True))
    attrs = etype.get("attributes", [])

    direction = "DIRECTED" if is_directed else "UNDIRECTED"
    attr_parts = [_format_attr(a) for a in attrs]

    stmt = f"{keyword} {direction} EDGE {ename} (FROM {from_type}, TO {to_type}"
    if attr_parts:
        stmt += ", " + ", ".join(attr_parts)
    stmt += ")"
    return ename, stmt


async def create_graph(
    profile: Optional[str] = None,
    graph_name: str = None,
    vertex_types: List[Dict[str, Any]] = None,
    edge_types: List[Dict[str, Any]] = None,
) -> List[TextContent]:
    """Create a new graph with local vertex/edge types via a schema change job.

    Workflow (follows TigerGraph best practice for local schema):
        1. ``CREATE GRAPH <name>()``  — empty graph
        2. ``CREATE SCHEMA_CHANGE JOB … FOR GRAPH <name> { ADD VERTEX …; ADD EDGE …; }``
        3. ``RUN SCHEMA_CHANGE JOB …``
        4. ``DROP JOB …``  — clean up the job definition

    Using a local schema change job keeps vertex/edge types scoped to this
    graph, avoiding global-scope privilege requirements and name collisions.
    See: https://docs.tigergraph.com/gsql-ref/4.2/ddl-and-loading/modifying-a-graph-schema
    """
    try:
        conn = get_connection(profile=profile)

        vertex_names: list[str] = []
        edge_names: list[str] = []

        # ── Step 1: Create an empty graph ────────────────────────────
        create_graph_gsql = f"CREATE GRAPH {graph_name}()"
        create_result = await conn.gsql(create_graph_gsql)
        create_result_str = str(create_result) if create_result else ""

        if gsql_has_error(create_result_str):
            return format_error(
                operation="create_graph",
                error=TigerGraphException(create_result_str),
                context={
                    "graph_name": graph_name,
                    "step": "CREATE GRAPH",
                    "gsql_command": create_graph_gsql,
                },
                suggestions=[
                    "Use list_graphs() to check if the graph already exists",
                    "Use drop_graph() first if you need to recreate an existing graph",
                ],
            )

        # ── Step 2: Build ADD VERTEX / ADD EDGE statements ───────────
        job_stmts: list[str] = []

        for vtype in vertex_types:
            vname, stmt = _build_vertex_stmt(vtype, keyword="ADD")
            if vname:
                job_stmts.append(stmt + ";")
                vertex_names.append(vname)

        if edge_types:
            for etype in edge_types:
                ename, stmt = _build_edge_stmt(etype, keyword="ADD")
                if ename:
                    job_stmts.append(stmt + ";")
                    edge_names.append(ename)

        # If no types to add, return the empty graph as-is
        if not job_stmts:
            return format_success(
                operation="create_graph",
                summary=f"Success: Empty graph '{graph_name}' created (no vertex/edge types defined)",
                data={
                    "graph_name": graph_name,
                    "vertex_type_count": 0,
                    "edge_type_count": 0,
                    "gsql_command": create_graph_gsql,
                },
                suggestions=[
                    f"View graph: show_graph_details(graph_name='{graph_name}')",
                    "Add types later with a schema change job",
                ],
                metadata={"operation_type": "DDL"},
            )

        # ── Step 3: Create, run, and drop the schema change job ──────
        job_name = f"setup_{graph_name}"
        job_body = "\n    ".join(job_stmts)
        schema_gsql = (
            f"USE GRAPH {graph_name}\n"
            f"CREATE SCHEMA_CHANGE JOB {job_name} FOR GRAPH {graph_name} {{\n"
            f"    {job_body}\n"
            f"}}\n"
            f"RUN SCHEMA_CHANGE JOB {job_name}\n"
            f"DROP JOB {job_name}"
        )

        schema_result = await conn.gsql(schema_gsql)
        schema_result_str = str(schema_result) if schema_result else ""

        if gsql_has_error(schema_result_str):
            return format_error(
                operation="create_graph",
                error=TigerGraphException(schema_result_str),
                context={
                    "graph_name": graph_name,
                    "step": "SCHEMA_CHANGE JOB",
                    "vertex_types": vertex_names,
                    "edge_types": edge_names,
                    "gsql_command": schema_gsql,
                },
                suggestions=[
                    "Check vertex/edge type definitions for syntax errors",
                    "Ensure from_vertex/to_vertex reference vertex types defined in this call",
                    f"The empty graph '{graph_name}' was created; use drop_graph('{graph_name}') to clean up if needed",
                ],
            )

        # ── Success ──────────────────────────────────────────────────
        full_gsql = f"{create_graph_gsql}\n\n{schema_gsql}"

        return format_success(
            operation="create_graph",
            summary=(
                f"Success: Graph '{graph_name}' created with "
                f"{len(vertex_names)} vertex type(s) and {len(edge_names)} edge type(s)"
            ),
            data={
                "graph_name": graph_name,
                "vertex_type_count": len(vertex_names),
                "edge_type_count": len(edge_names),
                "vertex_types": vertex_names,
                "edge_types": edge_names,
                "gsql_command": full_gsql,
            },
            suggestions=[
                f"View graph: show_graph_details(graph_name='{graph_name}')",
                f"Start adding data: add_node(graph_name='{graph_name}', ...)",
                "List all graphs: list_graphs()",
            ],
            metadata={"operation_type": "DDL"},
        )
    except Exception as e:
        return format_error(
            operation="create_graph",
            error=e,
            context={
                "graph_name": graph_name,
                "vertex_types": len(vertex_types),
                "edge_types": len(edge_types) if edge_types else 0,
            },
        )


async def drop_graph(profile: Optional[str] = None, graph_name: str = None) -> List[TextContent]:
    """Drop a graph."""
    try:
        conn = get_connection(profile=profile, graph_name=graph_name)
        result = await conn.gsql(f"DROP GRAPH {graph_name}")
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="drop_graph",
                error=TigerGraphException(result_str),
                context={"graph_name": graph_name},
                suggestions=[
                    "Use list_graphs() to verify the graph name exists",
                    "Ensure you have the required permissions to drop the graph",
                ],
            )

        return format_success(
            operation="drop_graph",
            summary=f"Success: Graph '{graph_name}' dropped successfully",
            data={
                "graph_name": graph_name,
                "result": result_str,
            },
            suggestions=[
                "Warning: This operation is permanent and cannot be undone",
                "Verify deletion: list_graphs()",
                "Tip: To delete only data (keep schema): use 'clear_graph_data' instead",
            ],
            metadata={"operation_type": "DDL", "destructive": True},
        )
    except Exception as e:
        return format_error(
            operation="drop_graph",
            error=e,
            context={"graph_name": graph_name},
        )


async def get_global_schema(profile: Optional[str] = None, **kwargs) -> List[TextContent]:
    """Get the complete global schema via GSQL LS command.

    Args:
        profile: Connection profile name.
        **kwargs: No parameters required

    Returns:
        Full global schema including global vertex types, edge types, graphs, and their members.
    """
    try:
        conn = get_connection(profile=profile)
        result = await conn.gsql("LS")
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="get_global_schema",
                error=TigerGraphException(result_str),
                context={},
            )

        return format_success(
            operation="get_global_schema",
            summary="Success: Global schema retrieved successfully",
            data={"global_schema": result},
            suggestions=[
                "List graphs: list_graphs()",
                "View specific graph: show_graph_details(graph_name='<name>')",
                "Tip: This shows ALL vertex/edge types and graphs in the database",
            ],
            metadata={"format": "GSQL_LS_output"},
        )
    except Exception as e:
        return format_error(
            operation="get_global_schema",
            error=e,
            context={},
        )


async def list_graphs(profile: Optional[str] = None, **kwargs) -> List[TextContent]:
    """List all graph names in the TigerGraph database.

    Args:
        profile: Connection profile name.
        **kwargs: No parameters required - lists all graph names in the database

    Returns:
        List of graph names only (without detailed schema information).
    """
    try:
        conn = get_connection(profile=profile)
        result = await conn.gsql("SHOW GRAPH *")
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="list_graphs",
                error=TigerGraphException(result_str),
                context={},
            )

        # Extract graph names from "SHOW GRAPH *" output.
        # Typical output lines look like:
        #   - Graph MyGraph(Person:v, Knows:e)
        #   - Graph AnotherGraph(...)
        # We match lines containing "Graph " and extract the name before '('.
        import re
        graph_names: list[str] = []
        for match in re.finditer(r'Graph\s+(\w+)', result_str):
            name = match.group(1)
            if name.lower() not in ('graph', 'graphs'):
                graph_names.append(name)

        # Deduplicate while preserving order
        seen: set[str] = set()
        unique_graphs = [g for g in graph_names if not (g in seen or seen.add(g))]

        if unique_graphs:
            return format_success(
                operation="list_graphs",
                summary=f"Found {len(unique_graphs)} graph(s)",
                data={
                    "graphs": unique_graphs,
                    "count": len(unique_graphs),
                },
                suggestions=[
                    f"Full listing: show_graph_details(graph_name='{unique_graphs[0]}')",
                    f"Schema only: get_graph_schema(graph_name='{unique_graphs[0]}')",
                ],
            )
        else:
            # Parsing found nothing — return raw output so user/LLM can read it
            return format_success(
                operation="list_graphs",
                summary="No graph names extracted (raw output included)",
                data={
                    "graphs": [],
                    "count": 0,
                    "raw_output": result_str,
                },
                suggestions=[
                    "Create a graph: create_graph(...)",
                ],
            )
    except Exception as e:
        return format_error(
            operation="list_graphs",
            error=e,
            context={}
        )


async def clear_graph_data(
    profile: Optional[str] = None,
    graph_name: Optional[str] = None,
    vertex_type: Optional[str] = None,
    confirm: bool = False,
) -> List[TextContent]:
    """Clear all data from a graph (keeps schema)."""
    if not confirm:
        return format_error(
            operation="clear_graph_data",
            error=ValueError("Confirmation required"),
            context={"graph_name": graph_name, "vertex_type": vertex_type},
            suggestions=[
                "Warning: Set confirm=True to proceed with this destructive operation",
                "This will delete ALL DATA but keep the schema intact",
                "Tip: To delete the entire graph including schema: use 'drop_graph' instead"
            ]
        )

    try:
        conn = get_connection(profile=profile, graph_name=graph_name)

        if vertex_type:
            # Clear specific vertex type and its connected edges
            result = await conn.delVertices(vertex_type)
            
            return format_success(
                operation="clear_graph_data",
                summary=f"Success: Cleared all vertices of type '{vertex_type}' and their connected edges",
                data={
                    "graph_name": conn.graphname,
                    "vertex_type": vertex_type,
                    "deletion_result": result
                },
                suggestions=[
                    "Warning: This operation is permanent and cannot be undone",
                    f"Verify: get_vertex_count(vertex_type='{vertex_type}')",
                    "Reload data: use 'add_nodes' or loading jobs"
                ],
                metadata={"destructive": True}
            )
        else:
            # Clear all data from the graph
            vertex_types = await conn.getVertexTypes()
            total_deleted = {}
            for vtype in vertex_types:
                result = await conn.delVertices(vtype)
                total_deleted[vtype] = result
            
            return format_success(
                operation="clear_graph_data",
                summary=f"Success: Cleared all data from graph '{conn.graphname}'",
                data={
                    "graph_name": conn.graphname,
                    "deleted_vertex_types": len(vertex_types),
                    "deletion_results": total_deleted
                },
                suggestions=[
                    "Warning: This operation is permanent and cannot be undone",
                    f"Verify: get_vertex_count() should return 0 for all types",
                    "Reload data: use 'add_nodes', 'add_edges', or loading jobs",
                    "Tip: Schema is intact - only data was removed"
                ],
                metadata={"destructive": True}
            )
    except Exception as e:
        return format_error(
            operation="clear_graph_data",
            error=e,
            context={
                "graph_name": graph_name or "default",
                "vertex_type": vertex_type
            }
        )


_DETAIL_TYPE_COMMANDS = {
    "schema": "SHOW VERTEX *\nSHOW EDGE *",
    "query": "SHOW QUERY *",
    "loading_job": "SHOW LOADING JOB *",
    "data_source": "SHOW DATA_SOURCE *",
}


async def show_graph_details(
    profile: Optional[str] = None,
    graph_name: Optional[str] = None,
    detail_type: Optional[str] = None,
) -> List[TextContent]:
    """Show details of a graph, optionally filtered by category.

    Args:
        profile: Connection profile name.
        graph_name: Graph to inspect. Uses default if omitted.
        detail_type: One of 'schema', 'query', 'loading_job', 'data_source'.
                     If omitted, runs ``LS`` to show everything.
    """
    try:
        conn = get_connection(profile=profile, graph_name=graph_name)
        gname = conn.graphname

        if detail_type and detail_type in _DETAIL_TYPE_COMMANDS:
            gsql_cmd = f"USE GRAPH {gname}\n{_DETAIL_TYPE_COMMANDS[detail_type]}"
            label = detail_type
        else:
            gsql_cmd = f"USE GRAPH {gname}\nLS"
            label = "all"

        result = await conn.gsql(gsql_cmd)
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="show_graph_details",
                error=TigerGraphException(result_str),
                context={"graph_name": gname, "detail_type": label},
            )

        return format_success(
            operation="show_graph_details",
            summary=f"Graph '{gname}' — {label} details",
            data={
                "graph_name": gname,
                "detail_type": label,
                "listing": result_str,
            },
            suggestions=[
                f"Schema as JSON: get_graph_schema(graph_name='{gname}')",
                "Start adding data: add_node(...) or add_edge(...)",
                "Run a query: run_installed_query(...) or run_query(...)",
            ],
        )
    except Exception as e:
        return format_error(
            operation="show_graph_details",
            error=e,
            context={"graph_name": graph_name or "default"},
        )



