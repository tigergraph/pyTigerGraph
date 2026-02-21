# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""MCP Server implementation for TigerGraph."""

import logging
from typing import Dict, List
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from .tool_names import TigerGraphToolName
from pyTigerGraph.common.exception import TigerGraphException
from .tools import (
    get_all_tools,
    # Global schema operations (database level)
    get_global_schema,
    # Graph operations (database level)
    list_graphs,
    create_graph,
    drop_graph,
    clear_graph_data,
    # Schema operations (graph level)
    get_graph_schema,
    show_graph_details,
    # Node tools
    add_node,
    add_nodes,
    get_node,
    get_nodes,
    delete_node,
    delete_nodes,
    has_node,
    get_node_edges,
    # Edge tools
    add_edge,
    add_edges,
    get_edge,
    get_edges,
    delete_edge,
    delete_edges,
    has_edge,
    # Query tools
    run_query,
    run_installed_query,
    install_query,
    drop_query,
    show_query,
    get_query_metadata,
    is_query_installed,
    get_neighbors,
    # Loading job tools
    create_loading_job,
    run_loading_job_with_file,
    run_loading_job_with_data,
    get_loading_jobs,
    get_loading_job_status,
    drop_loading_job,
    # Statistics tools
    get_vertex_count,
    get_edge_count,
    get_node_degree,
    # GSQL tools
    gsql,
    generate_gsql,
    generate_cypher,
    # Vector schema tools
    add_vector_attribute,
    drop_vector_attribute,
    list_vector_attributes,
    get_vector_index_status,
    # Vector data tools
    upsert_vectors,
    load_vectors_from_csv,
    load_vectors_from_json,
    search_top_k_similarity,
    fetch_vector,
    # Data Source tools
    create_data_source,
    update_data_source,
    get_data_source,
    drop_data_source,
    get_all_data_sources,
    drop_all_data_sources,
    preview_sample_data,
    # Discovery tools
    discover_tools,
    get_workflow,
    get_tool_info,
)

logger = logging.getLogger(__name__)


class MCPServer:
    """MCP Server for TigerGraph."""

    def __init__(self, name: str = "TigerGraph-MCP"):
        """Initialize the MCP server."""
        self.server = Server(name)
        self._setup_handlers()

    def _setup_handlers(self):
        """Setup MCP server handlers."""

        @self.server.list_tools()
        async def list_tools() -> List[Tool]:
            """List all available tools."""
            return get_all_tools()

        @self.server.call_tool()
        async def call_tool(name: str, arguments: Dict) -> List[TextContent]:
            """Handle tool calls."""
            try:
                match name:
                    # Global schema operations (database level)
                    case TigerGraphToolName.GET_GLOBAL_SCHEMA:
                        return await get_global_schema(**arguments)
                    # Graph operations (database level)
                    case TigerGraphToolName.LIST_GRAPHS:
                        return await list_graphs(**arguments)
                    case TigerGraphToolName.CREATE_GRAPH:
                        return await create_graph(**arguments)
                    case TigerGraphToolName.DROP_GRAPH:
                        return await drop_graph(**arguments)
                    case TigerGraphToolName.CLEAR_GRAPH_DATA:
                        return await clear_graph_data(**arguments)
                    # Schema operations (graph level)
                    case TigerGraphToolName.GET_GRAPH_SCHEMA:
                        return await get_graph_schema(**arguments)
                    case TigerGraphToolName.SHOW_GRAPH_DETAILS:
                        return await show_graph_details(**arguments)
                    # Node operations
                    case TigerGraphToolName.ADD_NODE:
                        return await add_node(**arguments)
                    case TigerGraphToolName.ADD_NODES:
                        return await add_nodes(**arguments)
                    case TigerGraphToolName.GET_NODE:
                        return await get_node(**arguments)
                    case TigerGraphToolName.GET_NODES:
                        return await get_nodes(**arguments)
                    case TigerGraphToolName.DELETE_NODE:
                        return await delete_node(**arguments)
                    case TigerGraphToolName.DELETE_NODES:
                        return await delete_nodes(**arguments)
                    case TigerGraphToolName.HAS_NODE:
                        return await has_node(**arguments)
                    case TigerGraphToolName.GET_NODE_EDGES:
                        return await get_node_edges(**arguments)
                    # Edge operations
                    case TigerGraphToolName.ADD_EDGE:
                        return await add_edge(**arguments)
                    case TigerGraphToolName.ADD_EDGES:
                        return await add_edges(**arguments)
                    case TigerGraphToolName.GET_EDGE:
                        return await get_edge(**arguments)
                    case TigerGraphToolName.GET_EDGES:
                        return await get_edges(**arguments)
                    case TigerGraphToolName.DELETE_EDGE:
                        return await delete_edge(**arguments)
                    case TigerGraphToolName.DELETE_EDGES:
                        return await delete_edges(**arguments)
                    case TigerGraphToolName.HAS_EDGE:
                        return await has_edge(**arguments)
                    # Query operations
                    case TigerGraphToolName.RUN_QUERY:
                        return await run_query(**arguments)
                    case TigerGraphToolName.RUN_INSTALLED_QUERY:
                        return await run_installed_query(**arguments)
                    case TigerGraphToolName.INSTALL_QUERY:
                        return await install_query(**arguments)
                    case TigerGraphToolName.DROP_QUERY:
                        return await drop_query(**arguments)
                    case TigerGraphToolName.SHOW_QUERY:
                        return await show_query(**arguments)
                    case TigerGraphToolName.GET_QUERY_METADATA:
                        return await get_query_metadata(**arguments)
                    case TigerGraphToolName.IS_QUERY_INSTALLED:
                        return await is_query_installed(**arguments)
                    case TigerGraphToolName.GET_NEIGHBORS:
                        return await get_neighbors(**arguments)
                    # Loading job operations
                    case TigerGraphToolName.CREATE_LOADING_JOB:
                        return await create_loading_job(**arguments)
                    case TigerGraphToolName.RUN_LOADING_JOB_WITH_FILE:
                        return await run_loading_job_with_file(**arguments)
                    case TigerGraphToolName.RUN_LOADING_JOB_WITH_DATA:
                        return await run_loading_job_with_data(**arguments)
                    case TigerGraphToolName.GET_LOADING_JOBS:
                        return await get_loading_jobs(**arguments)
                    case TigerGraphToolName.GET_LOADING_JOB_STATUS:
                        return await get_loading_job_status(**arguments)
                    case TigerGraphToolName.DROP_LOADING_JOB:
                        return await drop_loading_job(**arguments)
                    # Statistics operations
                    case TigerGraphToolName.GET_VERTEX_COUNT:
                        return await get_vertex_count(**arguments)
                    case TigerGraphToolName.GET_EDGE_COUNT:
                        return await get_edge_count(**arguments)
                    case TigerGraphToolName.GET_NODE_DEGREE:
                        return await get_node_degree(**arguments)
                    # GSQL operations
                    case TigerGraphToolName.GSQL:
                        return await gsql(**arguments)
                    case TigerGraphToolName.GENERATE_GSQL:
                        return await generate_gsql(**arguments)
                    case TigerGraphToolName.GENERATE_CYPHER:
                        return await generate_cypher(**arguments)
                    # Vector schema operations
                    case TigerGraphToolName.ADD_VECTOR_ATTRIBUTE:
                        return await add_vector_attribute(**arguments)
                    case TigerGraphToolName.DROP_VECTOR_ATTRIBUTE:
                        return await drop_vector_attribute(**arguments)
                    case TigerGraphToolName.LIST_VECTOR_ATTRIBUTES:
                        return await list_vector_attributes(**arguments)
                    case TigerGraphToolName.GET_VECTOR_INDEX_STATUS:
                        return await get_vector_index_status(**arguments)
                    # Vector data operations
                    case TigerGraphToolName.UPSERT_VECTORS:
                        return await upsert_vectors(**arguments)
                    case TigerGraphToolName.LOAD_VECTORS_FROM_CSV:
                        return await load_vectors_from_csv(**arguments)
                    case TigerGraphToolName.LOAD_VECTORS_FROM_JSON:
                        return await load_vectors_from_json(**arguments)
                    case TigerGraphToolName.SEARCH_TOP_K_SIMILARITY:
                        return await search_top_k_similarity(**arguments)
                    case TigerGraphToolName.FETCH_VECTOR:
                        return await fetch_vector(**arguments)
                    # Data Source operations
                    case TigerGraphToolName.CREATE_DATA_SOURCE:
                        return await create_data_source(**arguments)
                    case TigerGraphToolName.UPDATE_DATA_SOURCE:
                        return await update_data_source(**arguments)
                    case TigerGraphToolName.GET_DATA_SOURCE:
                        return await get_data_source(**arguments)
                    case TigerGraphToolName.DROP_DATA_SOURCE:
                        return await drop_data_source(**arguments)
                    case TigerGraphToolName.GET_ALL_DATA_SOURCES:
                        return await get_all_data_sources(**arguments)
                    case TigerGraphToolName.DROP_ALL_DATA_SOURCES:
                        return await drop_all_data_sources(**arguments)
                    case TigerGraphToolName.PREVIEW_SAMPLE_DATA:
                        return await preview_sample_data(**arguments)
                    # Discovery operations
                    case TigerGraphToolName.DISCOVER_TOOLS:
                        return await discover_tools(**arguments)
                    case TigerGraphToolName.GET_WORKFLOW:
                        return await get_workflow(**arguments)
                    case TigerGraphToolName.GET_TOOL_INFO:
                        return await get_tool_info(**arguments)
                    case _:
                        raise ValueError(f"Unknown tool: {name}")
            except TigerGraphException as e:
                logger.exception("Error in tool execution")
                error_msg = e.message if hasattr(e, 'message') else str(e)
                error_code = f" (Code: {e.code})" if hasattr(e, 'code') and e.code else ""
                return [TextContent(type="text", text=f"❌ TigerGraph Error{error_code} due to: {error_msg}")]
            except Exception as e:
                logger.exception("Error in tool execution")
                return [TextContent(type="text", text=f"❌ Error due to: {str(e)}")]


async def serve() -> None:
    """Serve the MCP server."""
    server = MCPServer()
    options = server.server.create_initialization_options()
    async with stdio_server() as (read_stream, write_stream):
        await server.server.run(read_stream, write_stream, options, raise_exceptions=True)

