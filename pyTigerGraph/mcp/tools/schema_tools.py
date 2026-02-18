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
from ..response_formatter import format_success, format_error
from pyTigerGraph.common.exception import TigerGraphException


# =============================================================================
# Global Schema Operations (Database level - operates on global schema)
# =============================================================================

class GetGlobalSchemaToolInput(BaseModel):
    """Input schema for getting the global schema (all global vertex/edge types, graphs, etc.)."""
    # No parameters needed - returns full global schema via GSQL LS command


# =============================================================================
# Graph Operations (Database level - operates on graphs within the database)
# =============================================================================

class ListGraphsToolInput(BaseModel):
    """Input schema for listing all graph names in the database."""
    # No parameters needed - lists all graph names in the database


class CreateGraphToolInput(BaseModel):
    """Input schema for creating a new graph with its schema."""
    graph_name: str = Field(..., description="Name of the new graph to create.")
    vertex_types: List[Dict[str, Any]] = Field(..., description="List of vertex type definitions for this graph.")
    edge_types: List[Dict[str, Any]] = Field(default_factory=list, description="List of edge type definitions for this graph.")


class DropGraphToolInput(BaseModel):
    """Input schema for dropping a graph from the database."""
    graph_name: str = Field(..., description="Name of the graph to drop.")


class ClearGraphDataToolInput(BaseModel):
    """Input schema for clearing all data from a graph (keeps schema structure)."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    vertex_type: Optional[str] = Field(None, description="Type of vertices to clear. If not provided, clears all data.")
    confirm: bool = Field(False, description="Must be True to confirm the deletion. This is a destructive operation.")


# =============================================================================
# Schema Operations (Graph level - operates on schema within a specific graph)
# =============================================================================

class GetGraphSchemaToolInput(BaseModel):
    """Input schema for getting a specific graph's schema (raw JSON)."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")


class DescribeGraphToolInput(BaseModel):
    """Input schema for getting a human-readable description of a graph's schema."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")


class GetGraphMetadataToolInput(BaseModel):
    """Input schema for getting metadata about a specific graph."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    metadata_type: Optional[str] = Field(None, description="Type of metadata to retrieve: 'vertex_types', 'edge_types', 'queries', 'loading_jobs', or 'all' (default).")


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
        "  • For single graph schema, use 'describe_graph' instead\n"
        "  • Useful for database administrators\n\n"
        
        "**Related Tools:** list_graphs, describe_graph, get_graph_schema"
    ),
    inputSchema=GetGlobalSchemaToolInput.model_json_schema(),
)

# =============================================================================
# Graph Operation Tools (Database level)
# =============================================================================

list_graphs_tool = Tool(
    name=TigerGraphToolName.LIST_GRAPHS,
    description=(
        "List all graph names in the TigerGraph database. Returns just the graph names without detailed schema information.\n\n"
        
        "**Use When:**\n"
        "  • Discovering what graphs exist in the database\n"
        "  • First step when connecting to a new TigerGraph instance\n"
        "  • Verifying a graph was created successfully\n"
        "  • Choosing which graph to work with\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{}\n"
        "```\n"
        "(No parameters needed)\n\n"
        
        "**Common Workflow:**\n"
        "1. Use 'list_graphs' to see available graphs\n"
        "2. Pick a graph to work with\n"
        "3. Call 'describe_graph' to understand its structure\n"
        "4. Begin data operations\n\n"
        
        "**Tips:**\n"
        "  • This is often the first tool to call\n"
        "  • For detailed schema, use 'describe_graph' next\n"
        "  • No parameters required\n\n"
        
        "**Related Tools:** describe_graph, create_graph, get_graph_schema"
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
        '      "to_vertex": "Person"\n'
        "    }\n"
        "  ]\n"
        "}\n"
        "```\n\n"
        
        "**Common Workflow:**\n"
        "1. Use 'list_graphs' to check if graph name is available\n"
        "2. Design your vertex types and edge types\n"
        "3. Call 'create_graph' with the schema\n"
        "4. Use 'describe_graph' to verify it was created correctly\n"
        "5. Start loading data with 'add_node' and 'add_edge'\n\n"
        
        "**Tips:**\n"
        "  • Define all vertex types before edge types\n"
        "  • Edge types reference vertex types (must exist)\n"
        "  • Each vertex type needs attributes defined\n"
        "  • Consider using 'get_workflow' for step-by-step guidance\n\n"
        
        "**Related Tools:** list_graphs, describe_graph, drop_graph"
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
        "Get the schema (vertex types, edge types, attributes) of a specific graph as raw JSON. "
        "Each graph has its own schema.\n\n"
        
        "**Use When:**\n"
        "  • You need raw JSON schema for programmatic processing\n"
        "  • Building schema visualization tools\n"
        "  • Extracting detailed schema metadata\n"
        "  • Comparing schemas programmatically\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "graph_name": "SocialNetwork"\n'
        "}\n"
        "```\n\n"
        
        "**Tips:**\n"
        "  • Returns raw JSON (not human-readable)\n"
        "  • For human-readable format, use 'describe_graph' instead\n"
        "  • Contains complete schema details\n"
        "  • Good for advanced/programmatic use cases\n\n"
        
        "**Related Tools:** describe_graph (human-readable), get_graph_metadata"
    ),
    inputSchema=GetGraphSchemaToolInput.model_json_schema(),
)

describe_graph_tool = Tool(
    name=TigerGraphToolName.DESCRIBE_GRAPH,
    description=(
        "Get a human-readable description of a specific graph's schema including vertex types, edge types, and their attributes. "
        "**This is the most important tool for understanding a graph's structure.**\n\n"
        
        "**Use When:**\n"
        "  • Starting work with a graph (call this first!)\n"
        "  • Understanding what vertex and edge types exist\n"
        "  • Learning what attributes are available\n"
        "  • Before writing queries or adding data\n"
        "  • Debugging schema-related errors\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "graph_name": "SocialNetwork"\n'
        "}\n"
        "```\n"
        "(Or omit graph_name to use default)\n\n"
        
        "**Common Workflow:**\n"
        "1. Call 'describe_graph' first to understand structure\n"
        "2. Note the vertex types and their primary keys\n"
        "3. Note the edge types and their connections\n"
        "4. Use this information for add_node, add_edge, run_query, etc.\n\n"
        
        "**Tips:**\n"
        "  • ALWAYS call this before working with an unfamiliar graph\n"
        "  • Provides human-readable markdown format\n"
        "  • Shows vertex types, edge types, and all attributes\n"
        "  • For raw JSON schema, use 'get_graph_schema' instead\n\n"
        
        "**What You'll Learn:**\n"
        "  • All vertex types and their attributes\n"
        "  • All edge types and their connections\n"
        "  • Data types for each attribute\n"
        "  • Which edges connect which vertex types\n\n"
        
        "**Related Tools:** get_graph_schema, list_graphs, get_graph_metadata"
    ),
    inputSchema=DescribeGraphToolInput.model_json_schema(),
)

get_graph_metadata_tool = Tool(
    name=TigerGraphToolName.GET_GRAPH_METADATA,
    description=(
        "Get comprehensive metadata about a specific graph including vertex types, edge types, installed queries, and loading jobs.\n\n"
        
        "**Use When:**\n"
        "  • Getting a complete overview of graph resources\n"
        "  • Discovering what queries and jobs are available\n"
        "  • Understanding the full graph configuration\n"
        "  • Auditing graph resources\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "graph_name": "MyGraph",\n'
        '  "metadata_type": "all"\n'
        "}\n"
        "```\n\n"
        
        "**Tips:**\n"
        "  • Returns vertex types, edge types, queries, and loading jobs\n"
        "  • Can filter by 'metadata_type': 'vertex_types', 'edge_types', 'queries', 'loading_jobs', or 'all'\n"
        "  • More comprehensive than 'describe_graph'\n"
        "  • Useful for discovering installed queries\n\n"
        
        "**Related Tools:** describe_graph, get_graph_schema, show_query"
    ),
    inputSchema=GetGraphMetadataToolInput.model_json_schema(),
)


async def get_graph_schema(graph_name: Optional[str] = None) -> List[TextContent]:
    """Get the schema of a specific graph."""
    try:
        conn = get_connection(graph_name=graph_name)
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
                "Tip: For human-readable format: use 'describe_graph' instead",
                f"View detailed descriptions: describe_graph(graph_name='{conn.graphname}')",
                f"Get metadata summary: get_graph_metadata(graph_name='{conn.graphname}')",
                "Start working with data: use 'add_node' or 'add_edge' tools"
            ],
            metadata={"format": "raw_json"}
        )
    except Exception as e:
        return format_error(
            operation="get_graph_schema",
            error=e,
            context={"graph_name": graph_name or "default"}
        )


async def create_graph(
    graph_name: str,
    vertex_types: List[Dict[str, Any]],
    edge_types: List[Dict[str, Any]] = None,
) -> List[TextContent]:
    """Create a new graph with its schema in the TigerGraph database."""
    try:
        conn = get_connection(graph_name=graph_name)
        # Build GSQL CREATE GRAPH statement
        gsql_cmd = f"CREATE GRAPH {graph_name} ("

        # Add vertex types
        vertex_defs = []
        for vtype in vertex_types:
            vname = vtype.get("name", "")
            attrs = vtype.get("attributes", [])
            attr_str = ", ".join([f"{attr['name']} {attr['type']}" for attr in attrs])
            vertex_defs.append(f"{vname}({attr_str})" if attr_str else vname)

        # Add edge types
        edge_defs = []
        if edge_types:
            for etype in edge_types:
                ename = etype.get("name", "")
                from_type = etype.get("from_vertex", "")
                to_type = etype.get("to_vertex", "")
                attrs = etype.get("attributes", [])
                attr_str = ", ".join([f"{attr['name']} {attr['type']}" for attr in attrs])
                edge_def = f"{ename}(FROM {from_type}, TO {to_type}"
                if attr_str:
                    edge_def += f", {attr_str}"
                edge_def += ")"
                edge_defs.append(edge_def)

        gsql_cmd += ", ".join(vertex_defs + edge_defs)
        gsql_cmd += ")"

        result = await conn.gsql(gsql_cmd)
        
        return format_success(
            operation="create_graph",
            summary=f"Success: Graph '{graph_name}' created successfully",
            data={
                "graph_name": graph_name,
                "vertex_type_count": len(vertex_types),
                "edge_type_count": len(edge_types) if edge_types else 0,
                "gsql_command": gsql_cmd,
                "result": result
            },
            suggestions=[
                f"View schema: describe_graph(graph_name='{graph_name}')",
                f"Start adding data: add_node(graph_name='{graph_name}', ...)",
                f"List all graphs: list_graphs()"
            ],
            metadata={"operation_type": "DDL"}
        )
    except Exception as e:
        return format_error(
            operation="create_graph",
            error=e,
            context={
                "graph_name": graph_name,
                "vertex_types": len(vertex_types),
                "edge_types": len(edge_types) if edge_types else 0
            }
        )


async def drop_graph(graph_name: str) -> List[TextContent]:
    """Drop a graph."""
    try:
        conn = get_connection(graph_name=graph_name)
        # Drop graph using GSQL
        result = await conn.gsql(f"DROP GRAPH {graph_name}")
        
        return format_success(
            operation="drop_graph",
            summary=f"Success: Graph '{graph_name}' dropped successfully",
            data={
                "graph_name": graph_name,
                "result": result
            },
            suggestions=[
                "Warning: This operation is permanent and cannot be undone",
                "Verify deletion: list_graphs()",
                "Tip: To delete only data (keep schema): use 'clear_graph_data' instead"
            ],
            metadata={"operation_type": "DDL", "destructive": True}
        )
    except Exception as e:
        return format_error(
            operation="drop_graph",
            error=e,
            context={"graph_name": graph_name}
        )


async def get_global_schema(**kwargs) -> List[TextContent]:
    """Get the complete global schema via GSQL LS command.

    Args:
        **kwargs: No parameters required

    Returns:
        Full global schema including global vertex types, edge types, graphs, and their members.
    """
    try:
        conn = get_connection()
        # LS command returns the complete global schema
        result = await conn.gsql("LS")
        
        return format_success(
            operation="get_global_schema",
            summary="Success: Global schema retrieved successfully",
            data={"global_schema": result},
            suggestions=[
                "List graphs: list_graphs()",
                "View specific graph: describe_graph(graph_name='<name>')",
                "Tip: This shows ALL vertex/edge types and graphs in the database"
            ],
            metadata={"format": "GSQL_LS_output"}
        )
    except Exception as e:
        return format_error(
            operation="get_global_schema",
            error=e,
            context={}
        )


async def list_graphs(**kwargs) -> List[TextContent]:
    """List all graph names in the TigerGraph database.

    Args:
        **kwargs: No parameters required - lists all graph names in the database

    Returns:
        List of graph names only (without detailed schema information).
    """
    try:
        conn = get_connection()
        # Use SHOW GRAPH * to get just graph names
        result = await conn.gsql("SHOW GRAPH *")

        # Parse the result to extract just graph names
        # The output typically contains lines with graph names
        lines = result.strip().split('\n') if result else []
        graph_names = []
        for line in lines:
            line = line.strip()
            # Skip empty lines and header lines
            if line and not line.startswith('-') and not line.startswith('='):
                # Extract graph name (usually the first word or between quotes)
                if 'Graph' in line or 'graph' in line:
                    # Try to extract the graph name
                    parts = line.split()
                    for part in parts:
                        if part and part not in ['Graph', 'graph', '-', ':', 'Vertex', 'Edge']:
                            graph_names.append(part.strip('",'))
                            break
                elif line and not any(x in line.lower() for x in ['vertex', 'edge', 'total', 'type']):
                    graph_names.append(line.strip('",'))

        if graph_names:
            # Remove duplicates while preserving order
            seen = set()
            unique_graphs = []
            for g in graph_names:
                if g not in seen:
                    seen.add(g)
                    unique_graphs.append(g)
            
            return format_success(
                operation="list_graphs",
                summary=f"Found {len(unique_graphs)} graph(s) in TigerGraph database",
                data={
                    "graphs": unique_graphs,
                    "count": len(unique_graphs)
                },
                suggestions=[
                    f"View schema: describe_graph(graph_name='{unique_graphs[0]}')" if unique_graphs else "Create a graph: create_graph(...)",
                    "Get global schema: get_global_schema()",
                    "Tip: Use describe_graph to see detailed schema for each graph"
                ],
                metadata={"raw_output": result}
            )
        else:
            # Fallback: just show the raw result
            return format_success(
                operation="list_graphs",
                summary="Success: Retrieved graphs list (raw format)",
                data={"raw_output": result},
                suggestions=[
                    "Create a graph: create_graph(...)",
                    "Check global schema: get_global_schema()"
                ]
            )
    except Exception as e:
        return format_error(
            operation="list_graphs",
            error=e,
            context={}
        )


async def clear_graph_data(
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
        conn = get_connection(graph_name=graph_name)

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


async def describe_graph(graph_name: Optional[str] = None) -> List[TextContent]:
    """Get a human-readable description of a specific graph's schema."""
    try:
        conn = get_connection(graph_name=graph_name)
        schema = await conn.getSchema()

        # Build human-readable description
        lines = [f"# Graph Schema: {conn.graphname}\n"]

        # Vertex types
        vertex_types = schema.get("VertexTypes", [])
        if vertex_types:
            lines.append("## Vertex Types\n")
            for vtype in vertex_types:
                vname = vtype.get("Name", "Unknown")
                lines.append(f"### {vname}")
                attrs = vtype.get("Attributes", [])
                if attrs:
                    lines.append("**Attributes:**")
                    for attr in attrs:
                        attr_name = attr.get("AttributeName", "")
                        attr_type = attr.get("AttributeType", {}).get("Name", "")
                        lines.append(f"  - `{attr_name}`: {attr_type}")
                lines.append("")

        # Edge types
        edge_types = schema.get("EdgeTypes", [])
        if edge_types:
            lines.append("## Edge Types\n")
            for etype in edge_types:
                ename = etype.get("Name", "Unknown")
                from_type = etype.get("FromVertexTypeName", "")
                to_type = etype.get("ToVertexTypeName", "")
                is_directed = etype.get("IsDirected", True)
                direction = "→" if is_directed else "↔"
                lines.append(f"### {ename}")
                lines.append(f"**Connection:** {from_type} {direction} {to_type}")
                attrs = etype.get("Attributes", [])
                if attrs:
                    lines.append("**Attributes:**")
                    for attr in attrs:
                        attr_name = attr.get("AttributeName", "")
                        attr_type = attr.get("AttributeType", {}).get("Name", "")
                        lines.append(f"  - `{attr_name}`: {attr_type}")
                lines.append("")

        # Summary
        lines.append("## Summary")
        lines.append(f"- **Total Vertex Types:** {len(vertex_types)}")
        lines.append(f"- **Total Edge Types:** {len(edge_types)}")

        description = "\n".join(lines)
        
        return format_success(
            operation="describe_graph",
            summary=f"Success: Graph description for '{conn.graphname}'",
            data={
                "graph_name": conn.graphname,
                "description": description,
                "vertex_type_count": len(vertex_types),
                "edge_type_count": len(edge_types)
            },
            suggestions=[
                "Tip: This is the MOST IMPORTANT tool for understanding graph structure",
                f"Get raw schema: get_graph_schema(graph_name='{conn.graphname}')",
                "Start adding data: add_node(...) or add_edge(...)",
                f"Get metadata: get_graph_metadata(graph_name='{conn.graphname}')"
            ],
            metadata={"format": "human_readable"}
        )
    except Exception as e:
        return format_error(
            operation="describe_graph",
            error=e,
            context={"graph_name": graph_name or "default"}
        )


async def get_graph_metadata(
    graph_name: Optional[str] = None,
    metadata_type: Optional[str] = None,
) -> List[TextContent]:
    """Get metadata about a specific graph including vertex types, edge types, queries, and loading jobs."""
    try:
        conn = get_connection(graph_name=graph_name)
        metadata = {}

        if metadata_type in [None, "all", "vertex_types"]:
            vertex_types = await conn.getVertexTypes()
            metadata["vertex_types"] = vertex_types

        if metadata_type in [None, "all", "edge_types"]:
            edge_types = await conn.getEdgeTypes()
            metadata["edge_types"] = edge_types

        if metadata_type in [None, "all", "queries"]:
            # List installed queries using GSQL
            try:
                result = await conn.gsql(f"USE GRAPH {conn.graphname}\nSHOW QUERY *")
                metadata["queries"] = result
            except Exception:
                metadata["queries"] = "Unable to list queries"

        if metadata_type in [None, "all", "loading_jobs"]:
            # List loading jobs using GSQL
            try:
                result = await conn.gsql(f"USE GRAPH {conn.graphname}\nSHOW LOADING JOB *")
                metadata["loading_jobs"] = result
            except Exception:
                metadata["loading_jobs"] = "Unable to list loading jobs"

        return format_success(
            operation="get_graph_metadata",
            summary=f"Success: Metadata retrieved for graph '{conn.graphname}'",
            data={
                "graph_name": conn.graphname,
                "metadata": metadata,
                "metadata_type": metadata_type or "all"
            },
            suggestions=[
                f"View detailed schema: describe_graph(graph_name='{conn.graphname}')",
                "Run a query: run_installed_query(...) or run_query(...)",
                "Create loading job: create_loading_job(...)",
                "Tip: Use metadata_type parameter to filter: 'vertex_types', 'edge_types', 'queries', or 'loading_jobs'"
            ],
            metadata={"components_retrieved": list(metadata.keys())}
        )
    except Exception as e:
        return format_error(
            operation="get_graph_metadata",
            error=e,
            context={
                "graph_name": graph_name or "default",
                "metadata_type": metadata_type
            }
        )

