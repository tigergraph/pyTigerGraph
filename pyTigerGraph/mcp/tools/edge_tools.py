# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Edge operation tools for MCP."""

import json
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from mcp.types import Tool, TextContent

from ..tool_names import TigerGraphToolName
from ..connection_manager import get_connection
from ..response_formatter import format_success, format_error
from pyTigerGraph.common.exception import TigerGraphException


class AddEdgeToolInput(BaseModel):
    """Input schema for adding an edge."""
    profile: Optional[str] = Field(None, description="Connection profile name. If not provided, uses TG_PROFILE env var or 'default'. Use 'list_connections' to see available profiles.")
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    source_vertex_type: str = Field(..., description="Type of the source vertex.")
    source_vertex_id: Union[str, int] = Field(..., description="ID of the source vertex.")
    edge_type: str = Field(..., description="Type of the edge.")
    target_vertex_type: str = Field(..., description="Type of the target vertex.")
    target_vertex_id: Union[str, int] = Field(..., description="ID of the target vertex.")
    attributes: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Edge attributes. Example: {'weight': 0.5, 'date': '2023-01-01'}")


class AddEdgesToolInput(BaseModel):
    """Input schema for adding multiple edges."""
    profile: Optional[str] = Field(None, description="Connection profile name. If not provided, uses TG_PROFILE env var or 'default'. Use 'list_connections' to see available profiles.")
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    edge_type: str = Field(..., description="Type of the edges.")
    edges: List[Dict[str, Any]] = Field(
        ..., 
        description=(
            "List of edges. Each edge is a dict with source and target info. "
            "Must specify 'source_id' (or 'source_vertex_id') and 'target_id' (or 'target_vertex_id'). "
            "Can also optionally specify 'source_type' and 'target_type' per edge if different from defaults, though batch edges usually share types. "
            "All edges in one batch MUST have the same source/target types due to API limitations. "
            "Example: [{'source_id': 'u1', 'target_id': 'p1', 'date': '2023'}]"
        )
    )


class GetEdgeToolInput(BaseModel):
    """Input schema for getting an edge."""
    profile: Optional[str] = Field(None, description="Connection profile name. If not provided, uses TG_PROFILE env var or 'default'. Use 'list_connections' to see available profiles.")
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    source_vertex_type: str = Field(..., description="Type of the source vertex.")
    source_vertex_id: Union[str, int] = Field(..., description="ID of the source vertex.")
    edge_type: str = Field(..., description="Type of the edge.")
    target_vertex_type: str = Field(..., description="Type of the target vertex.")
    target_vertex_id: Union[str, int] = Field(..., description="ID of the target vertex.")


class GetEdgesToolInput(BaseModel):
    """Input schema for getting multiple edges."""
    profile: Optional[str] = Field(None, description="Connection profile name. If not provided, uses TG_PROFILE env var or 'default'. Use 'list_connections' to see available profiles.")
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    source_vertex_type: Optional[str] = Field(None, description="Type of the source vertex. If not provided, gets all types.")
    source_vertex_id: Optional[Union[str, int]] = Field(None, description="ID of the source vertex. If not provided, gets all edges.")
    edge_type: Optional[str] = Field(None, description="Type of the edge. If not provided, gets all types.")
    limit: Optional[int] = Field(None, description="Maximum number of edges to return.")


class DeleteEdgeToolInput(BaseModel):
    """Input schema for deleting an edge."""
    profile: Optional[str] = Field(None, description="Connection profile name. If not provided, uses TG_PROFILE env var or 'default'. Use 'list_connections' to see available profiles.")
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    source_vertex_type: str = Field(..., description="Type of the source vertex.")
    source_vertex_id: Union[str, int] = Field(..., description="ID of the source vertex.")
    edge_type: str = Field(..., description="Type of the edge.")
    target_vertex_type: str = Field(..., description="Type of the target vertex.")
    target_vertex_id: Union[str, int] = Field(..., description="ID of the target vertex.")


class DeleteEdgesToolInput(BaseModel):
    """Input schema for deleting multiple edges."""
    profile: Optional[str] = Field(None, description="Connection profile name. If not provided, uses TG_PROFILE env var or 'default'. Use 'list_connections' to see available profiles.")
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    edge_type: str = Field(..., description="Type of the edges.")
    edges: List[Dict[str, Any]] = Field(..., description="List of edges with source and target vertex IDs.")


class HasEdgeToolInput(BaseModel):
    """Input schema for checking if an edge exists."""
    profile: Optional[str] = Field(None, description="Connection profile name. If not provided, uses TG_PROFILE env var or 'default'. Use 'list_connections' to see available profiles.")
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    source_vertex_type: str = Field(..., description="Type of the source vertex.")
    source_vertex_id: Union[str, int] = Field(..., description="ID of the source vertex.")
    edge_type: str = Field(..., description="Type of the edge.")
    target_vertex_type: str = Field(..., description="Type of the target vertex.")
    target_vertex_id: Union[str, int] = Field(..., description="ID of the target vertex.")


add_edge_tool = Tool(
    name=TigerGraphToolName.ADD_EDGE,
    description=(
        "Add a single edge (relationship) to a TigerGraph graph connecting two vertices.\n\n"
        
        "**Use When:**\n"
        "  • Creating a relationship between two entities\n"
        "  • Connecting vertices in the graph\n"
        "  • Building graph structure\n"
        "  • Adding individual relationships\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "source_vertex_type": "Person",\n'
        '  "source_vertex_id": "user1",\n'
        '  "edge_type": "FOLLOWS",\n'
        '  "target_vertex_type": "Person",\n'
        '  "target_vertex_id": "user2",\n'
        '  "attributes": {"since": "2024-01-15"}\n'
        "}\n"
        "```\n\n"
        
        "**Common Workflow:**\n"
        "1. Ensure both source and target vertices exist (use 'add_node')\n"
        "2. Call 'add_edge' to create relationship\n"
        "3. Optionally add edge attributes (like timestamps)\n"
        "4. Verify with 'get_neighbors' or 'get_node_edges'\n\n"
        
        "**Tips:**\n"
        "  • Both vertices must exist before adding edge\n"
        "  • Edge type must match schema definition\n"
        "  • For multiple edges, use 'add_edges' (more efficient)\n"
        "  • Edge attributes are optional\n\n"
        
        "**Related Tools:** add_edges, add_node, get_neighbors, delete_edge"
    ),
    inputSchema=AddEdgeToolInput.model_json_schema(),
)

add_edges_tool = Tool(
    name=TigerGraphToolName.ADD_EDGES,
    description=(
        "Add multiple edges (relationships) to a TigerGraph graph in a single batch operation. "
        "More efficient than calling 'add_edge' multiple times.\n\n"
        
        "**Use When:**\n"
        "  • Loading multiple relationships\n"
        "  • Building graph connections in bulk\n"
        "  • Importing relationship data from files\n"
        "  • Initial graph construction\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "edge_type": "FOLLOWS",\n'
        '  "edges": [\n'
        '    {"from_type": "Person", "from_id": "u1", "to_type": "Person", "to_id": "u2"},\n'
        '    {"from_type": "Person", "from_id": "u2", "to_type": "Person", "to_id": "u3"}\n'
        "  ]\n"
        "}\n"
        "```\n\n"
        
        "**Common Workflow:**\n"
        "1. Add all vertices first with 'add_nodes'\n"
        "2. Use 'add_edges' to create relationships\n"
        "3. Verify with 'get_edge_count'\n\n"
        
        "**Tips:**\n"
        "  • All edges in one call must be same edge type\n"
        "  • All referenced vertices must exist\n"
        "  • Batch size: 1000-5000 edges per call is optimal\n"
        "  • Much faster than individual 'add_edge' calls\n\n"
        
        "**Related Tools:** add_edge, add_nodes, get_edge_count"
    ),
    inputSchema=AddEdgesToolInput.model_json_schema(),
)

get_edge_tool = Tool(
    name=TigerGraphToolName.GET_EDGE,
    description=(
        "Get a single edge (relationship) from a TigerGraph graph by specifying source, target, and edge type.\n\n"
        
        "**Use When:**\n"
        "  • Retrieving a specific relationship\n"
        "  • Checking edge attributes\n"
        "  • Verifying edge was created\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "source_vertex_type": "Person",\n'
        '  "source_vertex_id": "user1",\n'
        '  "edge_type": "FOLLOWS",\n'
        '  "target_vertex_type": "Person",\n'
        '  "target_vertex_id": "user2"\n'
        "}\n"
        "```\n\n"
        
        "**Tips:**\n"
        "  • Requires full edge specification (source, target, type)\n"
        "  • Returns edge attributes if any\n"
        "  • Use 'get_neighbors' for simpler neighbor queries\n\n"
        
        "**Related Tools:** get_edges, has_edge, get_neighbors"
    ),
    inputSchema=GetEdgeToolInput.model_json_schema(),
)

get_edges_tool = Tool(
    name=TigerGraphToolName.GET_EDGES,
    description=(
        "Get multiple edges (relationships) from a TigerGraph graph, optionally filtered by type.\n\n"
        
        "**Use When:**\n"
        "  • Retrieving multiple edges\n"
        "  • Exploring graph relationships\n"
        "  • Data export and analysis\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "source_vertex_type": "Person",\n'
        '  "source_vertex_id": "user1",\n'
        '  "edge_type": "FOLLOWS"\n'
        "}\n"
        "```\n\n"
        
        "**Tips:**\n"
        "  • Can filter by edge type\n"
        "  • Returns all edges from a source vertex\n"
        "  • Use 'get_neighbors' for simpler use cases\n\n"
        
        "**Related Tools:** get_edge, get_neighbors, get_edge_count"
    ),
    inputSchema=GetEdgesToolInput.model_json_schema(),
)

delete_edge_tool = Tool(
    name=TigerGraphToolName.DELETE_EDGE,
    description=(
        "Delete a single edge (relationship) from a TigerGraph graph.\n\n"
        
        "**Use When:**\n"
        "  • Removing a specific relationship\n"
        "  • Disconnecting two vertices\n"
        "  • Graph maintenance\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "source_vertex_type": "Person",\n'
        '  "source_vertex_id": "user1",\n'
        '  "edge_type": "FOLLOWS",\n'
        '  "target_vertex_type": "Person",\n'
        '  "target_vertex_id": "user2"\n'
        "}\n"
        "```\n\n"
        
        "**Warning:**\n"
        "  • Operation is permanent\n"
        "  • Does not delete the vertices, only the edge\n\n"
        
        "**Related Tools:** delete_edges, add_edge, has_edge"
    ),
    inputSchema=DeleteEdgeToolInput.model_json_schema(),
)

delete_edges_tool = Tool(
    name=TigerGraphToolName.DELETE_EDGES,
    description=(
        "Delete multiple edges (relationships) from a TigerGraph graph.\n\n"
        
        "**Use When:**\n"
        "  • Removing multiple relationships\n"
        "  • Bulk edge deletion\n"
        "  • Graph restructuring\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "edge_type": "FOLLOWS",\n'
        '  "edges": [\n'
        '    {"from_type": "Person", "from_id": "u1", "to_type": "Person", "to_id": "u2"},\n'
        '    {"from_type": "Person", "from_id": "u2", "to_type": "Person", "to_id": "u3"}\n'
        "  ]\n"
        "}\n"
        "```\n\n"
        
        "**Warning:**\n"
        "  • Operation is permanent and cannot be undone\n"
        "  • Does not delete vertices\n\n"
        
        "**Related Tools:** delete_edge, add_edges"
    ),
    inputSchema=DeleteEdgesToolInput.model_json_schema(),
)

has_edge_tool = Tool(
    name=TigerGraphToolName.HAS_EDGE,
    description=(
        "Check if an edge (relationship) exists between two vertices without retrieving its data.\n\n"
        
        "**Use When:**\n"
        "  • Verifying relationship existence\n"
        "  • Validation logic\n"
        "  • More efficient than get_edge when you only need existence check\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "source_vertex_type": "Person",\n'
        '  "source_vertex_id": "user1",\n'
        '  "edge_type": "FOLLOWS",\n'
        '  "target_vertex_type": "Person",\n'
        '  "target_vertex_id": "user2"\n'
        "}\n"
        "```\n\n"
        
        "**Tips:**\n"
        "  • Returns boolean (true/false)\n"
        "  • Faster than get_edge when you don't need the data\n"
        "  • Use before add_edge to avoid duplicates\n\n"
        
        "**Related Tools:** get_edge, add_edge"
    ),
    inputSchema=HasEdgeToolInput.model_json_schema(),
)


async def add_edge(
    source_vertex_type: str,
    source_vertex_id: Union[str, int],
    edge_type: str,
    target_vertex_type: str,
    target_vertex_id: Union[str, int],
    attributes: Optional[Dict[str, Any]] = None,
    profile: Optional[str] = None,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Add an edge to the graph."""
    try:
        conn = get_connection(profile=profile, graph_name=graph_name)
        await conn.upsertEdge(
            source_vertex_type,
            str(source_vertex_id),
            edge_type,
            target_vertex_type,
            str(target_vertex_id),
            attributes or {},
        )
        
        return format_success(
            operation="add_edge",
            summary=f"Success: Edge '{edge_type}' from '{source_vertex_id}' to '{target_vertex_id}' added successfully",
            data=None,  # Simple success - no need to echo inputs
            suggestions=[
                f"Verify: get_edge(source_vertex_type='{source_vertex_type}', source_vertex_id='{source_vertex_id}', edge_type='{edge_type}', target_vertex_type='{target_vertex_type}', target_vertex_id='{target_vertex_id}')",
                f"View all edges from source: get_node_edges(vertex_type='{source_vertex_type}', vertex_id='{source_vertex_id}')",
                "Tip: Adding multiple edges? Use 'add_edges' for better performance"
            ],
            metadata={"graph_name": conn.graphname}
        )
    except Exception as e:
        return format_error(
            operation="add_edge",
            error=e,
            context={
                "source_vertex_type": source_vertex_type,
                "source_vertex_id": str(source_vertex_id),
                "edge_type": edge_type,
                "target_vertex_type": target_vertex_type,
                "target_vertex_id": str(target_vertex_id),
                "graph_name": graph_name or "default"
            }
        )


async def add_edges(
    edge_type: str,
    edges: List[Dict[str, Any]],
    profile: Optional[str] = None,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Add multiple edges to the graph."""
    try:
        conn = get_connection(profile=profile, graph_name=graph_name)
        # Convert edges list to format expected by upsertEdges: [(source_id, target_id, {attributes}), ...]
        # Note: upsertEdges requires all edges to have the same source/target vertex types
        if not edges:
            raise ValueError("Edges list cannot be empty")

        # Get source and target types from first edge
        first_edge = edges[0]
        source_type = first_edge.get("source_type") or first_edge.get("source_vertex_type")
        target_type = first_edge.get("target_type") or first_edge.get("target_vertex_type")

        if not source_type or not target_type:
            raise ValueError("source_type and target_type must be specified in edges")

        edge_data = []
        for e in edges:
            source_id = e.get("source_id") or e.get("source_vertex_id")
            target_id = e.get("target_id") or e.get("target_vertex_id")
            attrs = {k: v for k, v in e.items() if k not in ["source_type", "source_id", "source_vertex_type", "source_vertex_id", "target_type", "target_id", "target_vertex_type", "target_vertex_id"]}
            edge_data.append((source_id, target_id, attrs))

        await conn.upsertEdges(source_type, edge_type, target_type, edge_data)
        
        return format_success(
            operation="add_edges",
            summary=f"Success: Added {len(edges)} edges of type '{edge_type}' successfully",
            data={"edge_count": len(edges)},  # Only return the count
            suggestions=[
                f"Verify with: get_edges(edge_type='{edge_type}', limit=10)",
                f"Check source vertex: get_node_edges(vertex_type='{source_type}', vertex_id='<vertex_id>')",
                "Tip: View edge statistics with: get_edge_count(edge_type='{edge_type}')"
            ],
            metadata={"graph_name": conn.graphname}
        )
    except Exception as e:
        return format_error(
            operation="add_edges",
            error=e,
            context={
                "edge_type": edge_type,
                "edge_count": len(edges),
                "graph_name": graph_name or "default"
            }
        )


async def get_edge(
    source_vertex_type: str,
    source_vertex_id: Union[str, int],
    edge_type: str,
    target_vertex_type: str,
    target_vertex_id: Union[str, int],
    profile: Optional[str] = None,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Get an edge from the graph."""
    try:
        conn = get_connection(profile=profile, graph_name=graph_name)
        result = await conn.getEdges(
            source_vertex_type,
            str(source_vertex_id),
            edge_type,
            target_vertex_type,
            str(target_vertex_id),
        )
        
        if result:
            # Wrap list in dict for proper ToolResponse.data typing
            edge_data = result[0] if isinstance(result, list) and len(result) > 0 else result
            return format_success(
                operation="get_edge",
                summary=f"Found edge '{edge_type}' from '{source_vertex_id}' to '{target_vertex_id}'",
                data={"edge": edge_data} if edge_data else None,
                suggestions=[
                    f"View all edges from source: get_node_edges(vertex_type='{source_vertex_type}', vertex_id='{source_vertex_id}')",
                    f"Check source vertex: get_node(vertex_type='{source_vertex_type}', vertex_id='{source_vertex_id}')",
                    f"Check target vertex: get_node(vertex_type='{target_vertex_type}', vertex_id='{target_vertex_id}')"
                ],
                metadata={"graph_name": conn.graphname}
            )
        else:
            return format_success(
                operation="get_edge",
                summary=f"Error: Edge '{edge_type}' from '{source_vertex_id}' to '{target_vertex_id}' not found",
                suggestions=[
                    "Verify the edge exists with: has_edge(...)",
                    f"List all edges of this type: get_edges(edge_type='{edge_type}')",
                    "Check if the source/target vertices exist"
                ],
                metadata={"graph_name": conn.graphname}
            )
    except Exception as e:
        return format_error(
            operation="get_edge",
            error=e,
            context={
                "source_vertex_type": source_vertex_type,
                "source_vertex_id": str(source_vertex_id),
                "edge_type": edge_type,
                "target_vertex_type": target_vertex_type,
                "target_vertex_id": str(target_vertex_id),
                "graph_name": graph_name or "default"
            }
        )


async def get_edges(
    source_vertex_type: Optional[str] = None,
    source_vertex_id: Optional[Union[str, int]] = None,
    edge_type: Optional[str] = None,
    limit: Optional[int] = None,
    profile: Optional[str] = None,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Get multiple edges from the graph."""
    try:
        conn = get_connection(profile=profile, graph_name=graph_name)
        if source_vertex_id and source_vertex_type:
            result = await conn.getEdges(source_vertex_type, str(source_vertex_id), edge_type, limit=limit)
        else:
            # Get all edges of a type or all types
            if edge_type:
                result = await conn.getEdgesByType(edge_type, limit=limit)
            else:
                edge_types = await conn.getEdgeTypes()
                result = {}
                for etype in edge_types:
                    result[etype] = await conn.getEdgesByType(etype, limit=limit)
        
        edge_count = len(result) if isinstance(result, list) else sum(len(v) for v in result.values()) if isinstance(result, dict) else 0
        
        # Wrap result in dict for proper ToolResponse.data typing
        return format_success(
            operation="get_edges",
            summary=f"Success: Retrieved {edge_count} edges",
            data={"edges": result, "count": edge_count},
            suggestions=[
                "Tip: Add filters: specify source_vertex_type, source_vertex_id, or edge_type" if not edge_type else f"View specific edge: get_edge(...)",
                f"Check edge count: get_edge_count(edge_type='{edge_type}')" if edge_type else "Get edge statistics: get_edge_count()",
                "Delete edges: delete_edge(...) or delete_edges(...)"
            ],
            metadata={"graph_name": conn.graphname}
        )
    except Exception as e:
        return format_error(
            operation="get_edges",
            error=e,
            context={
                "source_vertex_type": source_vertex_type,
                "source_vertex_id": str(source_vertex_id) if source_vertex_id else None,
                "edge_type": edge_type,
                "limit": limit,
                "graph_name": graph_name or "default"
            }
        )


async def delete_edge(
    source_vertex_type: str,
    source_vertex_id: Union[str, int],
    edge_type: str,
    target_vertex_type: str,
    target_vertex_id: Union[str, int],
    profile: Optional[str] = None,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Delete an edge from the graph."""
    try:
        conn = get_connection(profile=profile, graph_name=graph_name)
        await conn.delEdges(
            sourceVertexType=source_vertex_type,
            sourceVertexId=str(source_vertex_id),
            edgeType=edge_type,
            targetVertexType=target_vertex_type,
            targetVertexId=str(target_vertex_id),
        )
        
        return format_success(
            operation="delete_edge",
            summary=f"Success: Edge '{edge_type}' from '{source_vertex_id}' to '{target_vertex_id}' deleted successfully",
            data=None,  # Simple success
            suggestions=[
                f"Verify deletion: has_edge(source_vertex_type='{source_vertex_type}', source_vertex_id='{source_vertex_id}', edge_type='{edge_type}', target_vertex_type='{target_vertex_type}', target_vertex_id='{target_vertex_id}')",
                "Warning: This operation is permanent and cannot be undone",
                f"View remaining edges: get_node_edges(vertex_type='{source_vertex_type}', vertex_id='{source_vertex_id}')"
            ],
            metadata={"graph_name": conn.graphname}
        )
    except Exception as e:
        return format_error(
            operation="delete_edge",
            error=e,
            context={
                "source_vertex_type": source_vertex_type,
                "source_vertex_id": str(source_vertex_id),
                "edge_type": edge_type,
                "target_vertex_type": target_vertex_type,
                "target_vertex_id": str(target_vertex_id),
                "graph_name": graph_name or "default"
            }
        )


async def delete_edges(
    edge_type: str,
    edges: List[Dict[str, Any]],
    profile: Optional[str] = None,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Delete multiple edges from the graph."""
    try:
        conn = get_connection(profile=profile, graph_name=graph_name)
        deleted_count = 0
        # Delete edges one by one
        for e in edges:
            source_type = e.get("source_type") or e.get("source_vertex_type")
            source_id = e.get("source_id") or e.get("source_vertex_id")
            target_type = e.get("target_type") or e.get("target_vertex_type")
            target_id = e.get("target_id") or e.get("target_vertex_id")
            result = await conn.delEdges(
                sourceVertexType=source_type,
                sourceVertexId=str(source_id),
                edgeType=edge_type,
                targetVertexType=target_type,
                targetVertexId=str(target_id)
            )
            if isinstance(result, dict):
                deleted_count += sum(result.values())
            elif isinstance(result, int):
                deleted_count += result
        
        return format_success(
            operation="delete_edges",
            summary=f"Success: Deleted {deleted_count} edges of type '{edge_type}' successfully",
            data={
                "deleted_count": deleted_count,
                "requested_count": len(edges)
            },
            suggestions=[
                "Warning: This operation is permanent and cannot be undone",
                f"Check remaining edges: get_edges(edge_type='{edge_type}')",
                f"Verify edge count: get_edge_count(edge_type='{edge_type}')"
            ],
            metadata={"graph_name": conn.graphname}
        )
    except Exception as e:
        return format_error(
            operation="delete_edges",
            error=e,
            context={
                "edge_type": edge_type,
                "edge_count": len(edges),
                "graph_name": graph_name or "default"
            }
        )


async def has_edge(
    source_vertex_type: str,
    source_vertex_id: Union[str, int],
    edge_type: str,
    target_vertex_type: str,
    target_vertex_id: Union[str, int],
    profile: Optional[str] = None,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Check if an edge exists in the graph."""
    try:
        conn = get_connection(profile=profile, graph_name=graph_name)
        
        try:
            result = await conn.getEdges(
                source_vertex_type,
                str(source_vertex_id),
                edge_type,
                target_vertex_type,
                str(target_vertex_id),
            )
            exists = len(result) > 0 if result else False
        except Exception:
            # If getEdges throws (e.g., source vertex doesn't exist), edge doesn't exist
            exists = False
        
        if exists:
            return format_success(
                operation="has_edge",
                summary=f"Success: Edge '{edge_type}' from '{source_vertex_id}' to '{target_vertex_id}' EXISTS",
                data={"exists": True},  # Only the boolean answer
                suggestions=[
                    f"Get edge details: get_edge(source_vertex_type='{source_vertex_type}', source_vertex_id='{source_vertex_id}', edge_type='{edge_type}', target_vertex_type='{target_vertex_type}', target_vertex_id='{target_vertex_id}')",
                    f"Delete edge: delete_edge(...)"
                ],
                metadata={"graph_name": conn.graphname}
            )
        else:
            return format_success(
                operation="has_edge",
                summary=f"Error: Edge '{edge_type}' from '{source_vertex_id}' to '{target_vertex_id}' DOES NOT EXIST",
                data={"exists": False},
                suggestions=[
                    f"Create edge: add_edge(source_vertex_type='{source_vertex_type}', source_vertex_id='{source_vertex_id}', edge_type='{edge_type}', target_vertex_type='{target_vertex_type}', target_vertex_id='{target_vertex_id}')",
                    "Verify source and target vertices exist first"
                ],
                metadata={"graph_name": conn.graphname}
            )
    except Exception as e:
        return format_error(
            operation="has_edge",
            error=e,
            context={
                "source_vertex_type": source_vertex_type,
                "source_vertex_id": str(source_vertex_id),
                "edge_type": edge_type,
                "target_vertex_type": target_vertex_type,
                "target_vertex_id": str(target_vertex_id),
                "graph_name": graph_name or "default"
            }
        )

