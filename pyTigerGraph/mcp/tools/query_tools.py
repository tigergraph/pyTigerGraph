# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Query operation tools for MCP."""

import json
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from mcp.types import Tool, TextContent

from ..tool_names import TigerGraphToolName
from ..connection_manager import get_connection
from ..response_formatter import format_success, format_error, gsql_has_error
from pyTigerGraph.common.exception import TigerGraphException


class RunQueryToolInput(BaseModel):
    """Input schema for running an interpreted query."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    query_text: str = Field(
        ..., 
        description=(
            "Query text to interpret and run. Supports both GSQL and openCypher queries. "
            "For GSQL: use 'INTERPRET QUERY () FOR GRAPH <graph> { <gsql_statements> }'. "
            "For openCypher: use 'INTERPRET OPENCYPHER QUERY () FOR GRAPH <graph> { <cypher_statements> }'. "
            "The query type is auto-detected based on the INTERPRET keyword used. "
            "Example (GSQL): `INTERPRET QUERY () FOR GRAPH MyGraph { SELECT v FROM Person:v }`"
            "Example (Cypher): `INTERPRET OPENCYPHER QUERY () FOR GRAPH MyGraph { MATCH (n) RETURN n LIMIT 5 }`"
        )
    )


class RunInstalledQueryToolInput(BaseModel):
    """Input schema for running an installed query."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    query_name: str = Field(..., description="Name of the installed query.")
    params: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Query parameters.")


class InstallQueryToolInput(BaseModel):
    """Input schema for installing a query."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    query_text: str = Field(..., description="GSQL query text to install.")


class ShowQueryToolInput(BaseModel):
    """Input schema for showing a query."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    query_name: str = Field(..., description="Name of the query to show.")


class GetQueryMetadataToolInput(BaseModel):
    """Input schema for getting query metadata."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    query_name: str = Field(..., description="Name of the query.")


class DropQueryToolInput(BaseModel):
    """Input schema for dropping a query."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    query_name: str = Field(..., description="Name of the query to drop.")


class IsQueryInstalledToolInput(BaseModel):
    """Input schema for checking if a query is installed."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    query_name: str = Field(..., description="Name of the query to check.")


class GetNeighborsToolInput(BaseModel):
    """Input schema for getting neighbors of a node."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    vertex_type: str = Field(..., description="Type of the source vertex (e.g., 'Person', 'Product').")
    vertex_id: str = Field(..., description="ID of the source vertex.")
    edge_type: Optional[str] = Field(None, description="Type of edges to traverse (e.g., 'purchased', 'friend_of'). If not provided, traverses all edge types.")
    target_vertex_type: Optional[str] = Field(None, description="Type of target vertices to return. If not provided, returns all types.")
    limit: Optional[int] = Field(None, description="Maximum number of neighbors to return.")


run_query_tool = Tool(
    name=TigerGraphToolName.RUN_QUERY,
    description=(
         "Run an interpreted query on a TigerGraph graph. Supports both GSQL and openCypher query languages. "
        "Use this for ad-hoc queries without needing to install them first.\n\n"
        
        "**Use When:**\n"
        "  • Running one-time or ad-hoc queries\n"
        "  • Testing queries before installation\n"
        "  • Simple data retrieval operations\n"
        "  • Prototyping and exploration\n\n"
        
        "**Quick Start (GSQL):**\n"
        "```json\n"
        "{\n"
        '  "query_text": "INTERPRET QUERY () FOR GRAPH MyGraph { SELECT v FROM Person:v LIMIT 5; PRINT v; }"\n'
        "}\n"
        "```\n\n"
        
        "**Quick Start (Cypher):**\n"
        "```json\n"
        "{\n"
        '  "query_text": "INTERPRET OPENCYPHER QUERY () FOR GRAPH MyGraph { MATCH (n:Person) RETURN n LIMIT 5 }"\n'
        "}\n"
        "```\n\n"
        
        "**Common Workflow:**\n"
        "1. Call 'show_graph_details' to understand the schema\n"
        "2. Write your query using vertex/edge types from schema\n"
        "3. Run with 'run_query' to test\n"
        "4. For repeated use, install with 'install_query'\n\n"
        
        "**Tips:**\n"
        "  • Query type auto-detected (GSQL vs Cypher)\n"
        "  • For frequent queries, use 'install_query' + 'run_installed_query' for better performance\n"
        "  • Always include 'FOR GRAPH' clause\n"
        "  • Use LIMIT to avoid retrieving too much data\n\n"
        
        "**Warning: Syntax Notes:**\n"
        "  • GSQL: `INTERPRET QUERY () FOR GRAPH <name> { <statements> }`\n"
        "  • Cypher: `INTERPRET OPENCYPHER QUERY () FOR GRAPH <name> { <cypher> }`\n\n"
        
        "**Related Tools:** run_installed_query, install_query, get_neighbors"
    ),
    inputSchema=RunQueryToolInput.model_json_schema(),
)

run_installed_query_tool = Tool(
    name=TigerGraphToolName.RUN_INSTALLED_QUERY,
    description=(
        "Run an installed GSQL query on a TigerGraph graph with parameters. "
        "Faster than interpreted queries for repeated execution.\n\n"
        
        "**Use When:**\n"
        "  • Running pre-installed, compiled queries\n"
        "  • Queries that are executed frequently\n"
        "  • Performance-critical operations\n"
        "  • Parameterized queries with different inputs\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "query_name": "getPersonFriends",\n'
        '  "params": {"personId": "user123", "maxHops": 2}\n'
        "}\n"
        "```\n\n"
        
        "**Common Workflow:**\n"
        "1. Install query once with 'install_query'\n"
        "2. Run multiple times with 'run_installed_query' and different params\n"
        "3. Much faster than 'run_query' for repeated use\n\n"
        
        "**Tips:**\n"
        "  • Queries must be installed first with 'install_query'\n"
        "  • Use 'is_query_installed' to check if query exists\n"
        "  • Provide params as dictionary matching query signature\n"
        "  • Faster than interpreted queries\n\n"
        
        "**Related Tools:** install_query, is_query_installed, show_query"
    ),
    inputSchema=RunInstalledQueryToolInput.model_json_schema(),
)

install_query_tool = Tool(
    name=TigerGraphToolName.INSTALL_QUERY,
    description=(
        "Install a GSQL query on a TigerGraph graph, compiling it for faster repeated execution.\n\n"
        
        "**Use When:**\n"
        "  • You have a query you'll run multiple times\n"
        "  • You want better query performance\n"
        "  • Creating reusable query logic\n"
        "  • Building query libraries\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "query_text": "CREATE QUERY getPersonFriends(VERTEX<Person> p) FOR GRAPH MyGraph { ... }"\n'
        "}\n"
        "```\n\n"
        
        "**Common Workflow:**\n"
        "1. Write and test query with 'run_query' first\n"
        "2. Once working, install with 'install_query'\n"
        "3. Run with 'run_installed_query' (faster)\n\n"
        
        "**Tips:**\n"
        "  • Query text should start with 'CREATE QUERY'\n"
        "  • Installation compiles the query for better performance\n"
        "  • Can define parameters in query signature\n"
        "  • Use 'show_query' to view installed query text\n\n"
        
        "**Related Tools:** run_installed_query, drop_query, show_query"
    ),
    inputSchema=InstallQueryToolInput.model_json_schema(),
)

show_query_tool = Tool(
    name=TigerGraphToolName.SHOW_QUERY,
    description=(
        "Show the GSQL text of an installed query.\n\n"
        
        "**Use When:**\n"
        "  • Reviewing what an installed query does\n"
        "  • Debugging query behavior\n"
        "  • Understanding existing queries\n"
        "  • Documenting installed queries\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "query_name": "getPersonFriends"\n'
        "}\n"
        "```\n\n"
        
        "**Tips:**\n"
        "  • Returns the full GSQL query text\n"
        "  • Query must be installed first\n"
        "  • Use 'is_query_installed' to check existence\n\n"
        
        "**Related Tools:** install_query, get_query_metadata, is_query_installed"
    ),
    inputSchema=ShowQueryToolInput.model_json_schema(),
)

get_query_metadata_tool = Tool(
    name=TigerGraphToolName.GET_QUERY_METADATA,
    description=(
        "Get metadata about an installed query including parameters, return type, and other details.\n\n"
        
        "**Use When:**\n"
        "  • Understanding query parameters and signature\n"
        "  • Discovering what queries are available\n"
        "  • Building query documentation\n"
        "  • Programmatic query discovery\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "query_name": "getPersonFriends"\n'
        "}\n"
        "```\n\n"
        
        "**Tips:**\n"
        "  • Shows query parameters, types, and metadata\n"
        "  • Helps understand how to call the query\n"
        "  • Use 'show_query' to see the actual query text\n\n"
        
        "**Related Tools:** show_query, is_query_installed, run_installed_query"
    ),
    inputSchema=GetQueryMetadataToolInput.model_json_schema(),
)

drop_query_tool = Tool(
    name=TigerGraphToolName.DROP_QUERY,
    description=(
        "Drop (delete) an installed query from TigerGraph.\n\n"
        
        "**Use When:**\n"
        "  • Removing queries no longer needed\n"
        "  • Cleaning up test queries\n"
        "  • Before re-installing a modified query\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "query_name": "oldQuery"\n'
        "}\n"
        "```\n\n"
        
        "**Warning:**\n"
        "  • Permanently deletes the installed query\n"
        "  • Cannot be undone\n"
        "  • Any code calling this query will fail\n\n"
        
        "**Tips:**\n"
        "  • Use 'show_query' first to review before dropping\n"
        "  • Cannot drop queries being used by other queries\n\n"
        
        "**Related Tools:** install_query, show_query, is_query_installed"
    ),
    inputSchema=DropQueryToolInput.model_json_schema(),
)

is_query_installed_tool = Tool(
    name=TigerGraphToolName.IS_QUERY_INSTALLED,
    description=(
        "Check if a query is installed in TigerGraph without running it.\n\n"
        
        "**Use When:**\n"
        "  • Verifying query installation\n"
        "  • Before trying to run an installed query\n"
        "  • Conditional query logic\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "query_name": "getPersonFriends"\n'
        "}\n"
        "```\n\n"
        
        "**Tips:**\n"
        "  • Returns true/false\n"
        "  • Faster than trying to run and catching errors\n"
        "  • Use before 'run_installed_query'\n\n"
        
        "**Related Tools:** install_query, run_installed_query, show_query"
    ),
    inputSchema=IsQueryInstalledToolInput.model_json_schema(),
)

get_neighbors_tool = Tool(
    name=TigerGraphToolName.GET_NEIGHBORS,
    description=(
        "Get neighbor vertices connected to a source vertex via edges. "
        "Useful for 1-hop graph traversal to find connected entities.\n\n"
        
        "**Use When:**\n"
        "  • Finding vertices directly connected to a vertex\n"
        "  • 1-hop traversal (immediate neighbors)\n"
        "  • Discovering relationships\n"
        "  • Building recommendation lists\n\n"
        
        "**Quick Start:**\n"
        "```json\n"
        "{\n"
        '  "vertex_type": "Person",\n'
        '  "vertex_id": "user123",\n'
        '  "edge_type": "FOLLOWS"\n'
        "}\n"
        "```\n\n"
        
        "**Common Workflow:**\n"
        "1. Have a source vertex ID\n"
        "2. Call 'get_neighbors' with vertex info\n"
        "3. Optionally filter by edge type\n"
        "4. Receive list of connected vertices\n\n"
        
        "**Tips:**\n"
        "  • Simpler than writing a query for 1-hop traversal\n"
        "  • Can filter by edge type (e.g., only 'FOLLOWS' edges)\n"
        "  • Can specify target vertex type\n"
        "  • For multi-hop traversal, use 'run_query' instead\n\n"
        
        "**Examples:**\n"
        "  • Find friends: edge_type='FRIENDS'\n"
        "  • Find purchases: edge_type='PURCHASED', target_vertex_type='Product'\n"
        "  • Find all connections: omit edge_type\n\n"
        
        "**Related Tools:** get_node_edges, run_query, add_edge"
    ),
    inputSchema=GetNeighborsToolInput.model_json_schema(),
)


async def run_query(
    query_text: str,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Run an interpreted query.
    
    Supports both GSQL and openCypher queries. The query type is auto-detected
    based on the INTERPRET keyword used in the query text.
    
    Args:
        query_text: The query text to run. Must include the full INTERPRET wrapper:
            - GSQL: INTERPRET QUERY () FOR GRAPH <graph> { <statements> }
            - openCypher: INTERPRET OPENCYPHER QUERY () FOR GRAPH <graph> { <statements> }
        graph_name: Optional graph name.
    """
    try:
        conn = get_connection(graph_name=graph_name)
        
        # Auto-detect query type from the query text
        query_upper = query_text.strip().upper()
        if "OPENCYPHER" in query_upper:
            query_type = "openCypher"
        else:
            query_type = "GSQL"
        
        result = await conn.runInterpretedQuery(query_text)
        
        return format_success(
            operation="run_query",
            summary=f"Success: {query_type} query executed successfully",
            data={
                "query_type": query_type,
                "result": result,
                "query_text_preview": query_text[:200] + "..." if len(query_text) > 200 else query_text
            },
            suggestions=[
                "Tip: For better performance: install the query with 'install_query' and use 'run_installed_query'",
                "Interpreted queries are slower but good for ad-hoc exploration",
                "View installed queries: show_graph_details()"
            ],
            metadata={
                "graph_name": conn.graphname,
                "execution_mode": "interpreted"
            }
        )
    except TigerGraphException as e:
        error_msg = e.message if hasattr(e, 'message') else str(e)
        error_code = f" (Code: {e.code})" if hasattr(e, 'code') and e.code else ""
        return format_error(
            operation="run_query",
            error=Exception(f"{error_msg}{error_code}"),
            context={
                "query_type": "GSQL/openCypher",
                "graph_name": graph_name or "default"
            }
        )
    except Exception as e:
        return format_error(
            operation="run_query",
            error=e,
            context={
                "graph_name": graph_name or "default"
            }
        )


async def run_installed_query(
    query_name: str,
    params: Optional[Dict[str, Any]] = None,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Run an installed query."""
    try:
        conn = get_connection(graph_name=graph_name)
        result = await conn.runInstalledQuery(query_name, params or {})
        
        return format_success(
            operation="run_installed_query",
            summary=f"Success: Query '{query_name}' executed successfully",
            data={
                "query_name": query_name,
                "parameters": params or {},
                "result": result
            },
            suggestions=[
                f"View query details: show_query(query_name='{query_name}')",
                f"Get metadata: get_query_metadata(query_name='{query_name}')",
                "Tip: Installed queries are much faster than interpreted queries"
            ],
            metadata={
                "graph_name": conn.graphname,
                "execution_mode": "installed"
            }
        )
    except Exception as e:
        return format_error(
            operation="run_installed_query",
            error=e,
            context={
                "query_name": query_name,
                "parameters": params or {},
                "graph_name": graph_name or "default"
            }
        )


async def install_query(
    query_text: str,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Install a query."""
    try:
        conn = get_connection(graph_name=graph_name)
        result = await conn.gsql(query_text)
        result_str = str(result) if result else ""

        # Try to extract query name from query_text
        query_name = "unknown"
        if "CREATE QUERY" in query_text.upper():
            parts = query_text.split("CREATE QUERY", 1)[1].strip().split("(")
            if parts:
                query_name = parts[0].strip()

        if gsql_has_error(result_str):
            return format_error(
                operation="install_query",
                error=TigerGraphException(result_str),
                context={
                    "query_name": query_name if query_name != "unknown" else None,
                    "graph_name": conn.graphname,
                },
                suggestions=[
                    "Check the query syntax for errors",
                    "Ensure all referenced vertex/edge types exist: show_graph_details()",
                    "Verify attribute names match the schema",
                ],
            )

        return format_success(
            operation="install_query",
            summary="Success: Query installed successfully",
            data={
                "result": result_str,
                "query_name": query_name if query_name != "unknown" else None,
            },
            suggestions=[
                f"Run the query: run_installed_query(query_name='{query_name}')" if query_name != "unknown" else "Run your query: run_installed_query(...)",
                "List all queries: show_graph_details()",
                "Tip: Installed queries are compiled and much faster than interpreted",
            ],
            metadata={
                "graph_name": conn.graphname,
                "operation_type": "DDL",
            },
        )
    except Exception as e:
        return format_error(
            operation="install_query",
            error=e,
            context={"graph_name": graph_name or "default"},
        )


async def show_query(
    query_name: str,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Show a query."""
    try:
        conn = get_connection(graph_name=graph_name)
        result = await conn.showQuery(query_name)
        
        return format_success(
            operation="show_query",
            summary=f"Success: Query '{query_name}' details retrieved",
            data={
                "query_name": query_name,
                "query_code": result
            },
            suggestions=[
                f"Run the query: run_installed_query(query_name='{query_name}')",
                f"Get metadata: get_query_metadata(query_name='{query_name}')",
                f"Delete query: drop_query(query_name='{query_name}')"
            ],
            metadata={"graph_name": conn.graphname}
        )
    except Exception as e:
        return format_error(
            operation="show_query",
            error=e,
            context={
                "query_name": query_name,
                "graph_name": graph_name or "default"
            }
        )


async def get_query_metadata(
    query_name: str,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Get query metadata."""
    try:
        conn = get_connection(graph_name=graph_name)
        result = await conn.getQueryMetadata(query_name)
        
        return format_success(
            operation="get_query_metadata",
            summary=f"Success: Query metadata retrieved for '{query_name}'",
            data={
                "query_name": query_name,
                "metadata": result
            },
            suggestions=[
                f"View query code: show_query(query_name='{query_name}')",
                f"Run the query: run_installed_query(query_name='{query_name}')",
                "Tip: Metadata includes parameters, return types, and other details"
            ],
            metadata={"graph_name": conn.graphname}
        )
    except Exception as e:
        return format_error(
            operation="get_query_metadata",
            error=e,
            context={
                "query_name": query_name,
                "graph_name": graph_name or "default"
            }
        )


async def drop_query(
    query_name: str,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Drop (delete) an installed query."""
    try:
        conn = get_connection(graph_name=graph_name)
        result = await conn.gsql(f"DROP QUERY {query_name}")
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="drop_query",
                error=TigerGraphException(result_str),
                context={
                    "query_name": query_name,
                    "graph_name": conn.graphname,
                },
                suggestions=[
                    "Verify the query name is correct",
                    "List installed queries: show_graph_details()",
                ],
            )

        return format_success(
            operation="drop_query",
            summary=f"Success: Query '{query_name}' dropped successfully",
            data={
                "query_name": query_name,
                "result": result_str,
            },
            suggestions=[
                "Warning: This operation is permanent and cannot be undone",
                f"Verify deletion: is_query_installed(query_name='{query_name}')",
                "List remaining queries: show_graph_details()",
            ],
            metadata={
                "graph_name": conn.graphname,
                "destructive": True,
            },
        )
    except Exception as e:
        return format_error(
            operation="drop_query",
            error=e,
            context={
                "query_name": query_name,
                "graph_name": graph_name or "default",
            },
        )


async def is_query_installed(
    query_name: str,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Check if a query is installed."""
    try:
        conn = get_connection(graph_name=graph_name)
        # Try to get query metadata - if it succeeds, the query exists
        try:
            result = await conn.getQueryMetadata(query_name)
            installed = True
        except Exception:
            installed = False
        
        if installed:
            return format_success(
                operation="is_query_installed",
                summary=f"Success: Query '{query_name}' IS installed",
                data={
                    "query_name": query_name,
                    "installed": True
                },
                suggestions=[
                    f"Run the query: run_installed_query(query_name='{query_name}')",
                    f"View details: show_query(query_name='{query_name}')",
                    f"Get metadata: get_query_metadata(query_name='{query_name}')"
                ],
                metadata={"graph_name": conn.graphname}
            )
        else:
            return format_success(
                operation="is_query_installed",
                summary=f"Error: Query '{query_name}' is NOT installed",
                data={
                    "query_name": query_name,
                    "installed": False
                },
                suggestions=[
                    f"Install it: install_query(query_text='CREATE QUERY {query_name} ...')",
                    "List all installed queries: show_graph_details()"
                ],
                metadata={"graph_name": conn.graphname}
            )
    except Exception as e:
        return format_error(
            operation="is_query_installed",
            error=e,
            context={
                "query_name": query_name,
                "graph_name": graph_name or "default"
            }
        )


async def get_neighbors(
    vertex_type: str,
    vertex_id: str,
    edge_type: Optional[str] = None,
    target_vertex_type: Optional[str] = None,
    limit: Optional[int] = None,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Get neighbor vertices connected to a source vertex."""
    try:
        conn = get_connection(graph_name=graph_name)

        # Build the edge pattern
        edge_pattern = f"(({edge_type}):e)" if edge_type else "(ANY:e)"
        target_pattern = f"{target_vertex_type}:t" if target_vertex_type else "ANY:t"
        limit_clause = f"LIMIT {limit}" if limit else ""

        query = f"""
        INTERPRET QUERY () FOR GRAPH {conn.graphname} {{
            SetAccum<VERTEX> @@seeds;

            @@seeds += to_vertex("{vertex_id}", "{vertex_type}");
            src = {{@@seeds}};
            neighbors = SELECT t FROM src:s -{edge_pattern}- {target_pattern}
                     {limit_clause};
            PRINT neighbors;
        }}
        """
        result = await conn.runInterpretedQuery(query)

        neighbors = []
        if result and len(result) > 0:
            neighbors = result[0].get("neighbors", [])

        return format_success(
            operation="get_neighbors",
            summary=f"Found {len(neighbors)} neighbor(s) of '{vertex_id}'",
            data={
                "neighbors": neighbors,
                "count": len(neighbors)
            },
            suggestions=[
                f"View source node: get_node(vertex_type='{vertex_type}', vertex_id='{vertex_id}')",
                f"View all edges: get_node_edges(vertex_type='{vertex_type}', vertex_id='{vertex_id}')",
                "Tip: Use edge_type and target_vertex_type to filter results"
            ],
            metadata={
                "graph_name": conn.graphname,
                "query_type": "GSQL_interpreted"
            }
        )
    except TigerGraphException as e:
        error_msg = e.message if hasattr(e, 'message') else str(e)
        error_code = f" (Code: {e.code})" if hasattr(e, 'code') and e.code else ""
        return format_error(
            operation="get_neighbors",
            error=Exception(f"{error_msg}{error_code}"),
            context={
                "vertex_type": vertex_type,
                "vertex_id": vertex_id,
                "edge_type": edge_type,
                "target_vertex_type": target_vertex_type,
                "graph_name": graph_name or "default"
            }
        )
    except Exception as e:
        return format_error(
            operation="get_neighbors",
            error=e,
            context={
                "vertex_type": vertex_type,
                "vertex_id": vertex_id,
                "graph_name": graph_name or "default"
            }
        )

