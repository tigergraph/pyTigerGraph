# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Tool registry for MCP tools."""

from typing import List
from mcp.types import Tool

from .schema_tools import (
    # Global schema operations (database level)
    get_global_schema_tool,
    # Graph operations (database level)
    list_graphs_tool,
    create_graph_tool,
    drop_graph_tool,
    clear_graph_data_tool,
    # Schema operations (graph level)
    get_graph_schema_tool,
    describe_graph_tool,
    get_graph_metadata_tool,
)
from .node_tools import (
    add_node_tool,
    add_nodes_tool,
    get_node_tool,
    get_nodes_tool,
    delete_node_tool,
    delete_nodes_tool,
    has_node_tool,
    get_node_edges_tool,
)
from .edge_tools import (
    add_edge_tool,
    add_edges_tool,
    get_edge_tool,
    get_edges_tool,
    delete_edge_tool,
    delete_edges_tool,
    has_edge_tool,
)
from .query_tools import (
    run_query_tool,
    run_installed_query_tool,
    install_query_tool,
    show_query_tool,
    get_query_metadata_tool,
    drop_query_tool,
    is_query_installed_tool,
    get_neighbors_tool,
)
from .data_tools import (
    create_loading_job_tool,
    run_loading_job_with_file_tool,
    run_loading_job_with_data_tool,
    get_loading_jobs_tool,
    get_loading_job_status_tool,
    drop_loading_job_tool,
)
from .statistics_tools import get_vertex_count_tool, get_edge_count_tool, get_node_degree_tool
from .gsql_tools import gsql_tool, generate_gsql_query_tool, generate_cypher_query_tool
from .vector_tools import (
    # Vector schema tools
    add_vector_attribute_tool,
    drop_vector_attribute_tool,
    get_vector_index_status_tool,
    # Vector data tools
    upsert_vectors_tool,
    search_top_k_similarity_tool,
    fetch_vector_tool,
)
from .datasource_tools import (
    create_data_source_tool,
    update_data_source_tool,
    get_data_source_tool,
    drop_data_source_tool,
    get_all_data_sources_tool,
    drop_all_data_sources_tool,
    preview_sample_data_tool,
)
from .discovery_tools import (
    discover_tools_tool,
    get_workflow_tool,
    get_tool_info_tool,
)


def get_all_tools() -> List[Tool]:
    """Get all available MCP tools.

    Returns:
        List of all MCP tools.
    """
    return [
        # Global schema operations (database level)
        get_global_schema_tool,
        # Graph operations (database level)
        list_graphs_tool,
        create_graph_tool,
        drop_graph_tool,
        clear_graph_data_tool,
        # Schema operations (graph level)
        get_graph_schema_tool,
        describe_graph_tool,
        get_graph_metadata_tool,
        # Node tools
        add_node_tool,
        add_nodes_tool,
        get_node_tool,
        get_nodes_tool,
        delete_node_tool,
        delete_nodes_tool,
        has_node_tool,
        get_node_edges_tool,
        # Edge tools
        add_edge_tool,
        add_edges_tool,
        get_edge_tool,
        get_edges_tool,
        delete_edge_tool,
        delete_edges_tool,
        has_edge_tool,
        # Query tools
        run_query_tool,
        run_installed_query_tool,
        install_query_tool,
        drop_query_tool,
        show_query_tool,
        get_query_metadata_tool,
        is_query_installed_tool,
        get_neighbors_tool,
        # Loading job tools
        create_loading_job_tool,
        run_loading_job_with_file_tool,
        run_loading_job_with_data_tool,
        get_loading_jobs_tool,
        get_loading_job_status_tool,
        drop_loading_job_tool,
        # Statistics tools
        get_vertex_count_tool,
        get_edge_count_tool,
        get_node_degree_tool,
        # GSQL tools
        gsql_tool,
        generate_gsql_query_tool,
        generate_cypher_query_tool,
        # Vector schema tools
        add_vector_attribute_tool,
        drop_vector_attribute_tool,
        get_vector_index_status_tool,
        # Vector data tools
        upsert_vectors_tool,
        search_top_k_similarity_tool,
        fetch_vector_tool,
        # Data Source tools
        create_data_source_tool,
        update_data_source_tool,
        get_data_source_tool,
        drop_data_source_tool,
        get_all_data_sources_tool,
        drop_all_data_sources_tool,
        preview_sample_data_tool,
        # Discovery tools
        discover_tools_tool,
        get_workflow_tool,
        get_tool_info_tool,
    ]

