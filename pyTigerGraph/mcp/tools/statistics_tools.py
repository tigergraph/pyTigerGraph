# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Statistics tools for MCP."""

from typing import List, Optional
from pydantic import BaseModel, Field
from mcp.types import Tool, TextContent

from ..tool_names import TigerGraphToolName
from ..connection_manager import get_connection
from ..response_formatter import format_success, format_error
from pyTigerGraph.common.exception import TigerGraphException


class GetVertexCountToolInput(BaseModel):
    """Input schema for getting vertex count."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    vertex_type: Optional[str] = Field(None, description="Type of vertices to count. If not provided, counts all types.")


class GetEdgeCountToolInput(BaseModel):
    """Input schema for getting edge count."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    edge_type: Optional[str] = Field(None, description="Type of edges to count. If not provided, counts all types.")


class GetNodeDegreeToolInput(BaseModel):
    """Input schema for getting node degree."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    vertex_type: str = Field(..., description="Type of the vertex.")
    vertex_id: str = Field(..., description="ID of the vertex.")
    edge_type: Optional[str] = Field(None, description="Type of edges to count. If not provided, counts all edge types.")
    direction: Optional[str] = Field("both", description="Direction of edges: 'outgoing', 'incoming', or 'both'.")


get_vertex_count_tool = Tool(
    name=TigerGraphToolName.GET_VERTEX_COUNT,
    description="Get the count of vertices in a TigerGraph graph.",
    inputSchema=GetVertexCountToolInput.model_json_schema(),
)

get_edge_count_tool = Tool(
    name=TigerGraphToolName.GET_EDGE_COUNT,
    description="Get the count of edges in a TigerGraph graph.",
    inputSchema=GetEdgeCountToolInput.model_json_schema(),
)

get_node_degree_tool = Tool(
    name=TigerGraphToolName.GET_NODE_DEGREE,
    description="Get the degree (number of connected edges) of a node in a TigerGraph graph.",
    inputSchema=GetNodeDegreeToolInput.model_json_schema(),
)


async def get_vertex_count(
    vertex_type: Optional[str] = None,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Get vertex count."""
    try:
        conn = get_connection(graph_name=graph_name)

        if vertex_type:
            count = await conn.getVertexCount(vertex_type)
            
            return format_success(
                operation="get_vertex_count",
                summary=f"Success: Vertex count for type '{vertex_type}': {count:,}",
                data={
                    "vertex_type": vertex_type,
                    "count": count
                },
                suggestions=[
                    f"View vertices: get_nodes(vertex_type='{vertex_type}', limit=10)",
                    "Get all counts: get_vertex_count() (without vertex_type)"
                ],
                metadata={"graph_name": conn.graphname}
            )
        else:
            vertex_types = await conn.getVertexTypes()
            counts = {}
            for vtype in vertex_types:
                counts[vtype] = await conn.getVertexCount(vtype)
            total = sum(counts.values())
            
            return format_success(
                operation="get_vertex_count",
                summary=f"Success: Total vertices: {total:,} across {len(vertex_types)} types",
                data={
                    "counts_by_type": counts,
                    "total": total,
                    "type_count": len(vertex_types)
                },
                suggestions=[
                    "View specific type: get_vertex_count(vertex_type='<type>')",
                    "View schema: show_graph_details()"
                ],
                metadata={"graph_name": conn.graphname}
            )
    except TigerGraphException as e:
        error_msg = e.message if hasattr(e, 'message') else str(e)
        error_code = f" (Code: {e.code})" if hasattr(e, 'code') and e.code else ""
        return format_error(
            operation="get_vertex_count",
            error=Exception(f"{error_msg}{error_code}"),
            context={
                "vertex_type": vertex_type,
                "graph_name": graph_name or "default"
            }
        )
    except Exception as e:
        return format_error(
            operation="get_vertex_count",
            error=e,
            context={
                "vertex_type": vertex_type,
                "graph_name": graph_name or "default"
            }
        )


async def get_edge_count(
    edge_type: Optional[str] = None,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Get edge count."""
    try:
        conn = get_connection(graph_name=graph_name)

        if edge_type:
            count = await conn.getEdgeCount(edge_type)
            
            return format_success(
                operation="get_edge_count",
                summary=f"Edge count for type '{edge_type}': {count:,}",
                data={"edge_type": edge_type, "count": count},
                suggestions=[
                    f"View edges: get_edges(edge_type='{edge_type}', limit=10)",
                    "Get all counts: get_edge_count() (without edge_type)"
                ],
                metadata={"graph_name": conn.graphname}
            )
        else:
            edge_types = await conn.getEdgeTypes()
            counts = {}
            for etype in edge_types:
                counts[etype] = await conn.getEdgeCount(etype)
            total = sum(counts.values())
            
            return format_success(
                operation="get_edge_count",
                summary=f"Total edges: {total:,} across {len(edge_types)} types",
                data={
                    "counts_by_type": counts,
                    "total": total,
                    "type_count": len(edge_types)
                },
                suggestions=[
                    "View specific type: get_edge_count(edge_type='<type>')",
                    "View schema: show_graph_details()"
                ],
                metadata={"graph_name": conn.graphname}
            )
    except TigerGraphException as e:
        error_msg = e.message if hasattr(e, 'message') else str(e)
        error_code = f" (Code: {e.code})" if hasattr(e, 'code') and e.code else ""
        return format_error(
            operation="get_edge_count",
            error=Exception(f"{error_msg}{error_code}"),
            context={
                "edge_type": edge_type,
                "graph_name": graph_name or "default"
            }
        )
    except Exception as e:
        return format_error(
            operation="get_edge_count",
            error=e,
            context={
                "edge_type": edge_type,
                "graph_name": graph_name or "default"
            }
        )


async def get_node_degree(
    vertex_type: str,
    vertex_id: str,
    edge_type: Optional[str] = None,
    direction: Optional[str] = "both",
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Get the degree (number of connected edges) of a node."""
    try:
        conn = get_connection(graph_name=graph_name)

        # Build edge type parameter for v.outdegree()
        # If edge_type contains multiple types separated by |, convert to SET format
        if edge_type:
            if '|' in edge_type:
                # Multiple edge types: convert to SET<STRING> format
                edge_types_list = [f'"{et.strip()}"' for et in edge_type.split('|')]
                edge_param = f"[{', '.join(edge_types_list)}]"
            else:
                # Single edge type: use STRING format
                edge_param = f'"{edge_type}"'
        else:
            edge_param = ''
        
        # Build query based on direction
        if direction == "outgoing":
            query = f"""
            INTERPRET QUERY () FOR GRAPH {conn.graphname} {{
                MaxAccum<INT> @@outgoing;
                SetAccum<VERTEX> @@seeds;
                
                @@seeds += to_vertex("{vertex_id}", "{vertex_type}");
                seed = {{@@seeds}};
                
                result = SELECT s FROM seed:s
                         POST-ACCUM @@outgoing += s.outdegree({edge_param});
                
                PRINT @@outgoing AS outgoing;
            }}
            """
        elif direction == "incoming":
            # For incoming, traverse from all vertices to our target
            edge_filter = f"(({edge_type}):e)" if edge_type else "(ANY:e)"
            query = f"""
            INTERPRET QUERY () FOR GRAPH {conn.graphname} {{
                SumAccum<INT> @@incoming;
                SetAccum<VERTEX> @@seeds;
                
                @@seeds += to_vertex("{vertex_id}", "{vertex_type}");
                seed = {{@@seeds}};
                
                result = SELECT s FROM ANY:s -{edge_filter}- seed:t
                         ACCUM @@incoming += 1;
                PRINT @@incoming AS incoming;
            }}
            """
        else:  # direction == "both"
            edge_filter = f"(({edge_type}):e)" if edge_type else "(ANY:e)"
            query = f"""
            INTERPRET QUERY () FOR GRAPH {conn.graphname} {{
                MaxAccum<INT> @@outgoing;
                SumAccum<INT> @@incoming;
                SetAccum<VERTEX> @@seeds;
                
                @@seeds += to_vertex("{vertex_id}", "{vertex_type}");
                seed = {{@@seeds}};
                
                // Get outgoing degree using vertex function
                result1 = SELECT s FROM seed:s
                          POST-ACCUM @@outgoing += s.outdegree({edge_param});
                
                // Count incoming edges by traversing
                result2 = SELECT s FROM ANY:s -{edge_filter}- seed:t
                          ACCUM @@incoming += 1;
                
                PRINT @@outgoing AS outgoing, @@incoming AS incoming;
            }}
            """
        
        result = await conn.runInterpretedQuery(query)
        
        # Parse results based on direction
        outgoing_count = 0
        incoming_count = 0
        
        if result and len(result) > 0:
            if direction == "outgoing":
                outgoing_count = result[0].get("outgoing", 0)
            elif direction == "incoming":
                incoming_count = result[0].get("incoming", 0)
            else:  # both
                outgoing_count = result[0].get("outgoing", 0)
                incoming_count = result[0].get("incoming", 0)

        result_data = {
            "outgoing_degree": outgoing_count if direction in ["outgoing", "both"] else None,
            "incoming_degree": incoming_count if direction in ["incoming", "both"] else None,
            "total_degree": outgoing_count + incoming_count if direction == "both" else (outgoing_count if direction == "outgoing" else incoming_count),
        }
        
        return format_success(
            operation="get_node_degree",
            summary=f"Degree for node '{vertex_id}': {result_data['total_degree']} edges ({direction})",
            data=result_data,
            suggestions=[
                f"View edges: get_node_edges(vertex_type='{vertex_type}', vertex_id='{vertex_id}')",
                f"View neighbors: get_neighbors(vertex_type='{vertex_type}', vertex_id='{vertex_id}')",
                f"View node: get_node(vertex_type='{vertex_type}', vertex_id='{vertex_id}')"
            ],
            metadata={"graph_name": conn.graphname}
        )
    except TigerGraphException as e:
        error_msg = e.message if hasattr(e, 'message') else str(e)
        error_code = f" (Code: {e.code})" if hasattr(e, 'code') and e.code else ""
        return format_error(
            operation="get_node_degree",
            error=Exception(f"{error_msg}{error_code}"),
            context={
                "vertex_type": vertex_type,
                "vertex_id": vertex_id,
                "direction": direction,
                "graph_name": graph_name or "default"
            }
        )
    except Exception as e:
        return format_error(
            operation="get_node_degree",
            error=e,
            context={
                "vertex_type": vertex_type,
                "vertex_id": vertex_id,
                "direction": direction,
                "graph_name": graph_name or "default"
            }
        )

