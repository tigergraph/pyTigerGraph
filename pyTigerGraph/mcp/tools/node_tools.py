# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Node operation tools for MCP.

Provides tools for creating, reading, updating, and deleting vertices (nodes)
in TigerGraph graphs with structured responses and contextual suggestions.
"""

import json
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from mcp.types import Tool, TextContent

from ..tool_names import TigerGraphToolName
from ..connection_manager import get_connection
from ..response_formatter import format_success, format_error, format_list_response
from ..tool_metadata import TOOL_METADATA
from pyTigerGraph.common.exception import TigerGraphException


class AddNodeToolInput(BaseModel):
    """Input schema for adding a node."""
    graph_name: Optional[str] = Field(
        None,
        description=(
            "Name of the graph. If not provided, uses the default connection.\n"
            "Tip: Use 'list_graphs' to see available graphs."
        )
    )
    vertex_type: str = Field(
        ...,
        description=(
            "Type of the vertex (must exist in graph schema).\n"
            "Example: 'Person', 'Product', 'Company'\n"
            "Tip: Use 'show_graph_details' to see available vertex types."
        )
    )
    vertex_id: Union[str, int] = Field(
        ...,
        description=(
            "ID of the vertex (primary key value).\n"
            "Format: String or integer depending on schema.\n"
            "Example: 'user123', 'product_456', or 12345\n"
            "Note: Must be unique within the vertex type."
        )
    )
    attributes: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description=(
            "Vertex attributes as key-value pairs.\n"
            "Keys must match the vertex type schema.\n"
            "Values should match the expected data types.\n"
            "Example: {'name': 'Alice', 'age': 30, 'email': 'alice@example.com'}\n"
            "Tip: Use 'show_graph_details' to see required attributes and types."
        ),
        json_schema_extra={
            "examples": [
                {"name": "Alice", "age": 30, "city": "San Francisco"},
                {"title": "Document 1", "content": "..."},
                {"price": 99.99, "category": "Electronics", "in_stock": True}
            ]
        }
    )


# Enhanced tool definition with rich context
add_node_tool = Tool(
    name=TigerGraphToolName.ADD_NODE,
    description=(
        "Add a single node (vertex) to a TigerGraph graph. This performs an upsert operation - "
        "creates a new vertex if it doesn't exist, or updates attributes if it does.\n\n"
        
        "**Use When:**\n"
        "  • Creating a single new entity (user, product, document, etc.)\n"
        "  • Updating an existing vertex's attributes\n"
        "  • You have individual entities to add (not batch loading)\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "vertex_type": "Person",\n'
        '  "vertex_id": "user123",\n'
        '  "attributes": {"name": "Alice", "age": 30}\n'
        "}\n"
        "```\n\n"
        
        "**Common Workflow:**\n"
        "1. Call 'show_graph_details' to understand vertex types and attributes\n"
        "2. Use 'add_node' to create individual vertices\n"
        "3. Call 'get_node' to verify the vertex was created\n"
        "4. Use 'add_edge' to connect this vertex to others\n\n"
        
        "**Tips:**\n"
        "  • For multiple vertices: Use 'add_nodes' instead (more efficient)\n"
        "  • Primary key is required (usually the 'id' attribute)\n"
        "  • Attribute names must match the schema exactly (case-sensitive)\n"
        "  • This is an upsert: existing vertices are updated, not duplicated\n\n"
        
        "**More Examples:**\n"
        "```json\n"
        "// Add a product\n"
        "{\n"
        '  "vertex_type": "Product",\n'
        '  "vertex_id": "prod456",\n'
        '  "attributes": {"name": "Laptop", "price": 999.99, "category": "Electronics"}\n'
        "}\n\n"
        "// Add a document with minimal attributes\n"
        "{\n"
        '  "vertex_type": "Document",\n'
        '  "vertex_id": "doc789",\n'
        '  "attributes": {"title": "Report Q4 2024"}\n'
        "}\n"
        "```\n\n"
        
        "**Related Tools:**\n"
        "  • add_nodes - Batch insert multiple vertices\n"
        "  • get_node - Retrieve a vertex by ID\n"
        "  • delete_node - Remove a vertex\n"
        "  • has_node - Check if vertex exists"
    ),
    inputSchema=AddNodeToolInput.model_json_schema(),
)


async def add_node(
    vertex_type: str,
    vertex_id: Union[str, int],
    attributes: Optional[Dict[str, Any]] = None,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Add a node to the graph with enhanced error handling and suggestions."""
    
    try:
        conn = get_connection(graph_name=graph_name)
        
        # Perform the upsert
        await conn.upsertVertex(vertex_type, str(vertex_id), attributes or {})
        
        # Success response with contextual suggestions
        suggestions = [
            f"Verify creation with: get_node(vertex_type='{vertex_type}', vertex_id='{vertex_id}')",
            f"Connect to other nodes with: add_edge(from_vertex_type='{vertex_type}', from_vertex_id='{vertex_id}', ...)",
            f"View all edges with: get_node_edges(vertex_type='{vertex_type}', vertex_id='{vertex_id}')",
        ]
        
        # Add batch suggestion if this seems to be part of a series
        suggestions.append("Tip: Adding multiple nodes? Use 'add_nodes' for better performance")
        
        return format_success(
            operation="add_node",
            summary=f"Successfully added/updated vertex '{vertex_id}' of type '{vertex_type}'",
            data=None,  # Simple success - no need to echo inputs
            suggestions=suggestions,
            metadata={"graph_name": conn.graphname}
        )
        
    except TigerGraphException as e:
        # Enhanced error handling with specific recovery hints
        return format_error(
            operation="add_node",
            error=e,
            context={
                "vertex_type": vertex_type,
                "vertex_id": str(vertex_id),
                "graph_name": graph_name or "default"
            }
        )
    except Exception as e:
        return format_error(
            operation="add_node",
            error=e,
            context={
                "vertex_type": vertex_type,
                "vertex_id": str(vertex_id),
                "graph_name": graph_name or "default"
            }
        )


class AddNodesToolInput(BaseModel):
    """Input schema for adding multiple nodes."""
    graph_name: Optional[str] = Field(
        None,
        description="Name of the graph. If not provided, uses default connection."
    )
    vertex_type: str = Field(
        ...,
        description=(
            "Type of the vertices (all vertices must be the same type).\n"
            "Example: 'Person', 'Product'\n"
            "Tip: Use 'show_graph_details' to see available types."
        )
    )

    vertex_id: str = Field(
        "id",
        description=(
            "Name of the primary key field in the vertex dictionaries.\n"
            "This tells the tool which field contains the vertex ID.\n"
            "Default: 'id'. Set to match your schema's primary key name.\n"
            "Examples: 'id', 'ACCOUNT_ID', 'TX_ID'"
        )
    )
    vertices: List[Dict[str, Any]] = Field(
        ...,
        description=(
            "List of vertices to add. Each vertex must contain the primary key field "
            "(specified by 'vertex_id' parameter) and other attributes matching the schema.\n\n"
            "Example with default vertex_id='id':\n"
            "```json\n"
            "[\n"
            '  {"id": "user1", "name": "Alice", "age": 30},\n'
            '  {"id": "user2", "name": "Bob", "age": 25}\n'
            "]\n"
            "```\n\n"
            "Example with vertex_id='ACCOUNT_ID':\n"
            "```json\n"
            "[\n"
            '  {"ACCOUNT_ID": 1001, "COUNTRY": "US", "ACCOUNT_TYPE": "savings"},\n'
            '  {"ACCOUNT_ID": 1002, "COUNTRY": "UK", "ACCOUNT_TYPE": "checking"}\n'
            "]\n"
            "```\n\n"
            "Note: All vertices will be processed in a single batch operation for efficiency."
        ),
        min_length=1
    )


add_nodes_tool = Tool(
    name=TigerGraphToolName.ADD_NODES,
    description=(
        "Add multiple nodes (vertices) to a TigerGraph graph in a single batch operation. "
        "This is significantly more efficient than calling 'add_node' multiple times.\n\n"
        
        "**Use When:**\n"
        "  • Loading multiple vertices of the same type\n"
        "  • Importing data from CSV, JSON, or database\n"
        "  • Initial data population\n"
        "  • Bulk updates to existing vertices\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "vertex_type": "Person",\n'
        '  "vertices": [\n'
        '    {"id": "user1", "name": "Alice", "age": 30},\n'
        '    {"id": "user2", "name": "Bob", "age": 25}\n'
        "  ]\n"
        "}\n"
        "```\n\n"
        
        "**Common Workflow:**\n"
        "1. Call 'show_graph_details' to understand the schema\n"
        "2. Prepare your data with primary keys and attributes\n"
        "3. Use 'add_nodes' to load vertices in batches\n"
        "4. Call 'get_vertex_count' to verify loading\n"
        "5. Use 'add_edges' to create relationships\n\n"
        
        "**Tips:**\n"
        "  • Set 'vertex_id' to match your schema's primary key name (default: 'id')\n"
        "  • For SARGraph: vertex_id='ACCOUNT_ID' for Account vertices\n"
        "  • All vertices must be the same type\n"
        "  • For very large datasets (>10K vertices), consider using loading jobs\n"
        "  • Batch size: 1000-5000 vertices per call is optimal\n\n"
        
        "**Warning: Common Mistakes:**\n"
        "  • Missing primary key in one or more vertices\n"
        "  • Using wrong vertex_id name (check schema with show_graph_details)\n"
        "  • Mixing different vertex types in one call\n"
        "  • Attribute name typos (must match schema exactly)\n"
        "  • Wrong data types (e.g., string instead of int)"
    ),
    inputSchema=AddNodesToolInput.model_json_schema(),
)


async def add_nodes(
    vertex_type: str,
    vertices: List[Dict[str, Any]],
    vertex_id: str = "id",
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Add multiple nodes to the graph with progress tracking."""
    
    try:
        conn = get_connection(graph_name=graph_name)
        
        # Convert vertices list to format expected by upsertVertices
        vertex_data = []
        failed_vertices = []
        
        for i, v in enumerate(vertices):
            try:
                vid = v.get(vertex_id)
                if vid is None:
                    failed_vertices.append({
                        "index": i,
                        "reason": f"Missing primary key '{vertex_id}'",
                        "vertex": v
                    })
                    continue
                
                # Extract attributes (exclude primary key field)
                attrs = {k: val for k, val in v.items() if k != vertex_id}
                vertex_data.append((vid, attrs))
                
            except Exception as e:
                failed_vertices.append({
                    "index": i,
                    "reason": str(e),
                    "vertex": v
                })
        
        # Perform batch upsert
        if vertex_data:
            await conn.upsertVertices(vertex_type, vertex_data)
        
        # Build response based on success/partial success
        success_count = len(vertex_data)
        total_count = len(vertices)
        
        if failed_vertices:
            # Partial success
            summary = f"Warning: Added {success_count}/{total_count} vertices of type '{vertex_type}' (some failed)"
            suggestions = [
                f"Check the {len(failed_vertices)} failed vertices for missing or invalid fields",
                f"Ensure all vertices have primary key field '{vertex_id}'",
                "Verify attribute names match the schema exactly"
            ]
        else:
            # Full success
            summary = f"Successfully added {success_count} vertices of type '{vertex_type}'"
            suggestions = [
                f"Verify with: get_vertex_count(vertex_type='{vertex_type}')",
                f"View sample data with: get_nodes(vertex_type='{vertex_type}', limit=5)",
                "Next step: Use 'add_edges' to create relationships between vertices"
            ]
        
        return format_success(
            operation="add_nodes",
            summary=summary,
            data={
                "success_count": success_count,
                "failed_count": len(failed_vertices),
                "success_rate": f"{(success_count/total_count)*100:.1f}%",
                "failed": failed_vertices if failed_vertices else None  # Only if there were failures
            },
            suggestions=suggestions,
            metadata={"graph_name": conn.graphname}
        )
        
    except TigerGraphException as e:
        return format_error(
            operation="add_nodes",
            error=e,
            context={
                "vertex_type": vertex_type,
                "vertex_count": len(vertices),
                "graph_name": graph_name or "default"
            }
        )
    except Exception as e:
        return format_error(
            operation="add_nodes",
            error=e,
            context={
                "vertex_type": vertex_type,
                "vertex_count": len(vertices),
                "graph_name": graph_name or "default"
            }
        )


class GetNodeToolInput(BaseModel):
    """Input schema for getting a node."""
    graph_name: Optional[str] = Field(None, description="Name of the graph.")
    vertex_type: str = Field(..., description="Type of the vertex to retrieve.")
    vertex_id: Union[str, int] = Field(..., description="ID of the vertex to retrieve.")


get_node_tool = Tool(
    name=TigerGraphToolName.GET_NODE,
    description=(
        "Get a single node (vertex) from a TigerGraph graph by its type and ID.\n\n"
        
        "**Use When:**\n"
        "  • Retrieving a specific entity by its ID\n"
        "  • Verifying a vertex was created successfully\n"
        "  • Checking current attribute values\n"
        "  • Fetching details before updating\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "vertex_type": "Person",\n'
        '  "vertex_id": "user123"\n'
        "}\n"
        "```\n\n"
        
        "**Related Tools:**\n"
        "  • get_nodes - Get multiple vertices\n"
        "  • has_node - Check if vertex exists\n"
        "  • get_node_edges - Get edges connected to vertex"
    ),
    inputSchema=GetNodeToolInput.model_json_schema(),
)


async def get_node(
    vertex_type: str,
    vertex_id: Union[str, int],
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Get a node from the graph."""
    try:
        conn = get_connection(graph_name=graph_name)
        # Use getVerticesById instead of getVertices with WHERE clause
        result = await conn.getVerticesById(vertex_type, vertex_id)
        
        if result and len(result) > 0:
            node_data = result[0] if isinstance(result, list) else result
            
            return format_success(
                operation="get_node",
                summary=f"Found vertex '{vertex_id}' of type '{vertex_type}'",
                data=node_data,  # Only return the actual node data
                suggestions=[
                    f"View connected edges with: get_node_edges(vertex_type='{vertex_type}', vertex_id='{vertex_id}')",
                    f"Find neighbors with: get_neighbors(vertex_type='{vertex_type}', vertex_id='{vertex_id}')",
                    "Update attributes with: add_node(...) - uses upsert"
                ],
                metadata={"graph_name": conn.graphname}
            )
        else:
            return format_success(
                operation="get_node",
                summary=f"Error: Vertex '{vertex_id}' of type '{vertex_type}' not found",
                suggestions=[
                    "Verify the vertex_id is correct",
                    f"Check if vertex exists with: has_node(vertex_type='{vertex_type}', vertex_id='{vertex_id}')",
                    f"List all vertices of this type with: get_nodes(vertex_type='{vertex_type}', limit=10)"
                ],
                metadata={"graph_name": conn.graphname}
            )
            
    except Exception as e:
        return format_error(
            operation="get_node",
            error=e,
            context={
                "vertex_type": vertex_type,
                "vertex_id": str(vertex_id),
                "graph_name": graph_name or "default"
            }
        )


# ============================================================================
# Get Multiple Nodes (get_nodes)
# ============================================================================

class GetNodesToolInput(BaseModel):
    vertex_type: str = Field(
        description="The type of vertices to retrieve"
    )
    where: Optional[str] = Field(
        default=None,
        description=(
            "Optional filter condition. Use TigerGraph WHERE syntax.\n"
            "Examples:\n"
            "- 'age > 25'\n"
            "- 'name == \"John\"'\n"
            "- 'active == true'"
        )
    )
    limit: Optional[int] = Field(
        default=100,
        description="Maximum number of vertices to return (default: 100)"
    )
    sort: Optional[str] = Field(
        default=None,
        description="Sort results by attribute. Use '-' prefix for descending (e.g., '-age')"
    )
    graph_name: Optional[str] = Field(
        default=None,
        description="Name of the graph to query (uses default if not specified)"
    )


get_nodes_tool = Tool(
    name=TigerGraphToolName.GET_NODES,
    description=(
        "**Purpose**: Retrieve multiple vertices (nodes) from the graph with optional filtering and sorting.\n\n"
        "**When to Use**:\n"
        "- List vertices of a specific type\n"
        "- Search for vertices matching certain criteria\n"
        "- Browse graph data with pagination\n"
        "- Find vertices based on attribute values\n\n"
        "**Key Features**:\n"
        "- WHERE clause for filtering\n"
        "- Sorting by attributes (ascending/descending)\n"
        "- Limit results for pagination\n"
        "- Returns complete vertex data including all attributes\n\n"
        "**Common Workflows**:\n"
        "1. List all vertices: `get_nodes(vertex_type='Person', limit=10)`\n"
        "2. Filter by attribute: `get_nodes(vertex_type='Person', where='age > 25')`\n"
        "3. Sort results: `get_nodes(vertex_type='Person', sort='-created_at', limit=20)`\n\n"
        "**Tips**:\n"
        "- Use limit to avoid retrieving too many vertices\n"
        "- WHERE clause syntax follows TigerGraph conventions\n"
        "- Sort with '-' prefix for descending order\n"
        "- Combine where, sort, and limit for precise queries\n\n"
        "**Related Tools**:\n"
        "- get_node: Get a single specific vertex\n"
        "- get_vertex_count: Count vertices before retrieving\n"
        "- run_query: For complex multi-hop queries"
    ),
    inputSchema=GetNodesToolInput.model_json_schema(),
)


async def get_nodes(
    vertex_type: str,
    where: Optional[str] = None,
    limit: Optional[int] = 100,
    sort: Optional[str] = None,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Get multiple nodes from the graph with optional filtering."""
    try:
        conn = get_connection(graph_name=graph_name)
        
        # Build query parameters
        kwargs = {
            "select": "",  # Get all attributes
            "limit": limit
        }
        
        if where:
            kwargs["where"] = where
        if sort:
            kwargs["sort"] = sort
        
        result = await conn.getVertices(vertex_type, **kwargs)
        
        count = len(result) if result else 0
        
        return format_success(
            operation="get_nodes",
            summary=f"Retrieved {count} vertices of type '{vertex_type}'{f' (filtered)' if where else ''}",
            data={
                "vertices": result,
                "count": count,
                "has_more": count == limit  # Might have more if we hit the limit
            },
            suggestions=[s for s in [
                f"View details: get_node(vertex_type='{vertex_type}', vertex_id='<id>')" if count > 0 else None,
                f"Count total: get_vertex_count(vertex_type='{vertex_type}')" if where else None,
                f"Increase limit: get_nodes(vertex_type='{vertex_type}', limit={limit * 2})" if count == limit else None,
            ] if s is not None],
            metadata={"graph_name": conn.graphname}
        )
        
    except Exception as e:
        return format_error(
            operation="get_nodes",
            error=e,
            context={
                "vertex_type": vertex_type,
                "where": where,
                "limit": limit,
                "graph_name": graph_name or "default"
            }
        )


# ============================================================================
# Delete Single Node (delete_node)
# ============================================================================

class DeleteNodeToolInput(BaseModel):
    vertex_type: str = Field(
        description="The type of the vertex to delete"
    )
    vertex_id: Union[str, int] = Field(
        description="The unique identifier of the vertex to delete"
    )
    graph_name: Optional[str] = Field(
        default=None,
        description="Name of the graph (uses default if not specified)"
    )


delete_node_tool = Tool(
    name=TigerGraphToolName.DELETE_NODE,
    description=(
        "**Purpose**: Delete a single vertex (node) from the graph by its ID.\n\n"
        "**When to Use**:\n"
        "- Remove a specific vertex from the graph\n"
        "- Clean up obsolete data\n"
        "- Delete test data\n"
        "- Remove entities based on business logic\n\n"
        "**Important Notes**:\n"
        "- Warning: This operation is permanent and cannot be undone\n"
        "- Connected edges will also be deleted (CASCADE behavior)\n"
        "- Verify the vertex exists before deletion if needed\n\n"
        "**Common Workflows**:\n"
        "1. Safe delete: `has_node()` → `delete_node()` → verify with `get_node()`\n"
        "2. Bulk delete: Use `delete_nodes()` with WHERE clause instead\n\n"
        "**Tips**:\n"
        "- Use has_node() first to verify existence\n"
        "- Consider the impact on connected edges\n"
        "- For multiple deletions, use delete_nodes() for better performance\n\n"
        "**Related Tools**:\n"
        "- delete_nodes: Delete multiple vertices at once\n"
        "- has_node: Check if vertex exists before deletion\n"
        "- get_node: Verify deletion completed"
    ),
    inputSchema=DeleteNodeToolInput.model_json_schema(),
)


async def delete_node(
    vertex_type: str,
    vertex_id: Union[str, int],
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Delete a single node from the graph."""
    try:
        conn = get_connection(graph_name=graph_name)
        
        # Delete using delVerticesById
        result = await conn.delVerticesById(vertex_type, vertex_id)
        
        deleted_count = result if isinstance(result, int) else 0
        
        if deleted_count > 0:
            return format_success(
                operation="delete_node",
                summary=f"Deleted vertex '{vertex_id}' of type '{vertex_type}'",
                data={"deleted_count": deleted_count},
                suggestions=[
                    f"Verify deletion: get_node(vertex_type='{vertex_type}', vertex_id='{vertex_id}')",
                    f"Check remaining count: get_vertex_count(vertex_type='{vertex_type}')"
                ],
                metadata={"graph_name": conn.graphname}
            )
        else:
            return format_success(
                operation="delete_node",
                summary=f"No vertex found with ID '{vertex_id}' of type '{vertex_type}'",
                suggestions=[
                    f"Verify ID: get_nodes(vertex_type='{vertex_type}', limit=10)",
                    f"Check type: show_graph_details()"
                ],
                metadata={"graph_name": conn.graphname}
            )
        
    except Exception as e:
        return format_error(
            operation="delete_node",
            error=e,
            context={
                "vertex_type": vertex_type,
                "vertex_id": str(vertex_id),
                "graph_name": graph_name or "default"
            }
        )


# ============================================================================
# Delete Multiple Nodes (delete_nodes)
# ============================================================================

class DeleteNodesToolInput(BaseModel):
    vertex_type: str = Field(
        description="The type of vertices to delete"
    )
    where: Optional[str] = Field(
        default=None,
        description=(
            "Filter condition to select vertices to delete. Use TigerGraph WHERE syntax.\n"
            "Examples:\n"
            "- 'age > 70'\n"
            "- 'status == \"inactive\"'\n"
            "- 'created_date < \"2020-01-01\"'\n\n"
            "Warning: If omitted, ALL vertices of this type will be deleted!"
        )
    )
    vertex_ids: Optional[List[Union[str, int]]] = Field(
        default=None,
        description="Optional list of specific vertex IDs to delete (alternative to WHERE clause)"
    )
    graph_name: Optional[str] = Field(
        default=None,
        description="Name of the graph (uses default if not specified)"
    )


delete_nodes_tool = Tool(
    name=TigerGraphToolName.DELETE_NODES,
    description=(
        "**Purpose**: Delete multiple vertices (nodes) from the graph in a single operation.\n\n"
        "**When to Use**:\n"
        "- Bulk deletion of vertices matching criteria\n"
        "- Delete specific set of vertices by IDs\n"
        "- Clear all vertices of a type\n"
        "- Data cleanup operations\n\n"
        "**Important Notes**:\n"
        "- Warning: This operation is permanent and cannot be undone\n"
        "- Warning: Omitting WHERE will delete ALL vertices of the specified type\n"
        "- Connected edges will also be deleted (CASCADE behavior)\n"
        "- More efficient than multiple delete_node() calls\n\n"
        "**Usage Modes**:\n"
        "1. By WHERE clause: `delete_nodes(vertex_type='Person', where='age > 70')`\n"
        "2. By ID list: `delete_nodes(vertex_type='Person', vertex_ids=['id1', 'id2'])`\n"
        "3. Delete all: `delete_nodes(vertex_type='TempData')` (no where/ids)\n\n"
        "**Safety Tips**:\n"
        "- ALWAYS test WHERE clause with get_nodes() first\n"
        "- Use get_vertex_count() to verify expected deletion count\n"
        "- Consider backing up data before bulk deletions\n\n"
        "**Related Tools**:\n"
        "- delete_node: Delete a single vertex\n"
        "- get_nodes: Preview vertices before deletion\n"
        "- get_vertex_count: Check deletion impact"
    ),
    inputSchema=DeleteNodesToolInput.model_json_schema(),
)


async def delete_nodes(
    vertex_type: str,
    where: Optional[str] = None,
    vertex_ids: Optional[List[Union[str, int]]] = None,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Delete multiple nodes from the graph."""
    try:
        conn = get_connection(graph_name=graph_name)
        
        if vertex_ids:
            # Delete by specific IDs
            result = await conn.delVerticesById(vertex_type, vertex_ids)
            deleted_count = result if isinstance(result, int) else 0
            operation_desc = f"{len(vertex_ids)} specified IDs"
        elif where:
            # Delete by WHERE condition
            result = await conn.delVertices(vertex_type, where=where)
            deleted_count = result if isinstance(result, int) else 0
            operation_desc = f"WHERE {where}"
        else:
            # Delete all vertices of this type
            result = await conn.delVertices(vertex_type)
            deleted_count = result if isinstance(result, int) else 0
            operation_desc = "all vertices"
        
        return format_success(
            operation="delete_nodes",
            summary=f"Deleted {deleted_count} vertices of type '{vertex_type}' ({operation_desc})",
            data={"deleted_count": deleted_count},
            suggestions=[
                f"Verify deletion: get_vertex_count(vertex_type='{vertex_type}')",
                f"View remaining: get_nodes(vertex_type='{vertex_type}', limit=10)"
            ],
            metadata={"graph_name": conn.graphname}
        )
        
    except Exception as e:
        return format_error(
            operation="delete_nodes",
            error=e,
            context={
                "vertex_type": vertex_type,
                "where": where,
                "vertex_ids": vertex_ids,
                "graph_name": graph_name or "default"
            }
        )


# ============================================================================
# Check Node Existence (has_node)
# ============================================================================

class HasNodeToolInput(BaseModel):
    vertex_type: str = Field(
        description="The type of the vertex to check"
    )
    vertex_id: Union[str, int] = Field(
        description="The unique identifier of the vertex"
    )
    graph_name: Optional[str] = Field(
        default=None,
        description="Name of the graph (uses default if not specified)"
    )


has_node_tool = Tool(
    name=TigerGraphToolName.HAS_NODE,
    description=(
        "**Purpose**: Check if a vertex (node) exists in the graph without retrieving its full data.\n\n"
        "**When to Use**:\n"
        "- Verify a vertex exists before operations\n"
        "- Validation in data pipelines\n"
        "- Conditional logic based on vertex existence\n"
        "- Lightweight existence checks (faster than get_node)\n\n"
        "**Key Features**:\n"
        "- Returns simple boolean result (exists: true/false)\n"
        "- More efficient than get_node() for existence checks\n"
        "- No data transfer overhead\n\n"
        "**Common Workflows**:\n"
        "1. Safe operations: `has_node()` → if true, proceed with get_node()/delete_node()\n"
        "2. Validation: Check required vertices exist before adding edges\n"
        "3. Conditional creation: If not exists, create with add_node()\n\n"
        "**Tips**:\n"
        "- Use this instead of get_node() when you only need existence confirmation\n"
        "- Combine with add_node() for upsert logic\n"
        "- Faster than catching errors from get_node()\n\n"
        "**Related Tools**:\n"
        "- get_node: Retrieve full vertex data if it exists\n"
        "- add_node: Create vertex if it doesn't exist\n"
        "- delete_node: Remove vertex after confirming existence"
    ),
    inputSchema=HasNodeToolInput.model_json_schema(),
)


async def has_node(
    vertex_type: str,
    vertex_id: Union[str, int],
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Check if a node exists in the graph."""
    try:
        conn = get_connection(graph_name=graph_name)
        
        # Use getVerticesById for existence check
        result = await conn.getVerticesById(vertex_type, vertex_id)
        
        exists = result and len(result) > 0
        
        return format_success(
            operation="has_node",
            summary=f"Vertex '{vertex_id}' of type '{vertex_type}' {'exists' if exists else 'does not exist'}",
            data={"exists": exists},
            suggestions=[s for s in [
                f"View details: get_node(vertex_type='{vertex_type}', vertex_id='{vertex_id}')" if exists else None,
                f"Create vertex: add_node(vertex_type='{vertex_type}', vertex_id='{vertex_id}', attributes={{...}})" if not exists else None,
                f"View all: get_nodes(vertex_type='{vertex_type}', limit=10)" if not exists else None
            ] if s is not None],
            metadata={"graph_name": conn.graphname}
        )
        
    except Exception as e:
        return format_error(
            operation="has_node",
            error=e,
            context={
                "vertex_type": vertex_type,
                "vertex_id": str(vertex_id),
                "graph_name": graph_name or "default"
            }
        )


# ============================================================================
# Get Node Edges (get_node_edges)
# ============================================================================

class GetNodeEdgesToolInput(BaseModel):
    vertex_type: str = Field(
        description="The type of the source vertex"
    )
    vertex_id: Union[str, int] = Field(
        description="The unique identifier of the source vertex"
    )
    edge_type: Optional[str] = Field(
        default=None,
        description="Optional: Filter by specific edge type. If omitted, returns all edge types."
    )
    limit: Optional[int] = Field(
        default=100,
        description="Maximum number of edges to return (default: 100)"
    )
    graph_name: Optional[str] = Field(
        default=None,
        description="Name of the graph (uses default if not specified)"
    )


get_node_edges_tool = Tool(
    name=TigerGraphToolName.GET_NODE_EDGES,
    description=(
        "**Purpose**: Retrieve all edges connected to a specific vertex (node).\n\n"
        "**When to Use**:\n"
        "- Explore connections from a vertex\n"
        "- Find relationships of a specific type\n"
        "- Analyze node connectivity patterns\n"
        "- Get edge attributes and target vertices\n\n"
        "**What You Get**:\n"
        "- Edge type and ID\n"
        "- Edge attributes\n"
        "- Target vertex information\n"
        "- Edge direction (outgoing from the specified vertex)\n\n"
        "**Common Workflows**:\n"
        "1. Explore all connections: `get_node_edges(vertex_type='Person', vertex_id='123')`\n"
        "2. Specific relationship type: `get_node_edges(..., edge_type='FRIEND_OF')`\n"
        "3. Degree analysis: Count returned edges to get outgoing degree\n\n"
        "**Tips**:\n"
        "- Returns OUTGOING edges only (edges starting from this vertex)\n"
        "- Use get_node_degree() for quick connection count\n"
        "- Use get_neighbors() to get target vertices without edge details\n"
        "- Combine with pagination (limit) for highly connected vertices\n\n"
        "**Note**: This returns edges where the specified vertex is the SOURCE.\n"
        "For incoming edges, use a reverse traversal query or get_neighbors().\n\n"
        "**Related Tools**:\n"
        "- get_node_degree: Count connections without retrieving edges\n"
        "- get_neighbors: Get connected vertices\n"
        "- get_edges: Query edges by type across the graph"
    ),
    inputSchema=GetNodeEdgesToolInput.model_json_schema(),
)


async def get_node_edges(
    vertex_type: str,
    vertex_id: Union[str, int],
    edge_type: Optional[str] = None,
    limit: Optional[int] = 100,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Get all edges connected to a specific node."""
    try:
        conn = get_connection(graph_name=graph_name)
        
        # Get outgoing edges from this vertex
        edges = await conn.getEdges(
            sourceVertexType=vertex_type,
            sourceVertexId=str(vertex_id),
            edgeType=edge_type
        )
        
        # Handle result format
        if edges is None:
            edges = []
        elif not isinstance(edges, list):
            edges = [edges]
        
        # Apply limit
        edges = edges[:limit] if limit and len(edges) > limit else edges
        
        count = len(edges)
        edge_desc = f" of type '{edge_type}'" if edge_type else " (all types)"
        
        return format_success(
            operation="get_node_edges",
            summary=f"Found {count} outgoing edges{edge_desc} from vertex '{vertex_id}'",
            data={
                "edges": edges,
                "count": count,
                "has_more": count == limit
            },
            suggestions=[s for s in [
                f"Get degree: get_node_degree(vertex_type='{vertex_type}', vertex_id='{vertex_id}')",
                f"Get neighbors: get_neighbors(vertex_type='{vertex_type}', vertex_id='{vertex_id}')",
                f"Filter by type: get_node_edges(..., edge_type='<type>')" if not edge_type and count > 0 else None
            ] if s is not None],
            metadata={"graph_name": conn.graphname}
        )
        
    except Exception as e:
        return format_error(
            operation="get_node_edges",
            error=e,
            context={
                "vertex_type": vertex_type,
                "vertex_id": str(vertex_id),
                "edge_type": edge_type,
                "graph_name": graph_name or "default"
            }
        )

