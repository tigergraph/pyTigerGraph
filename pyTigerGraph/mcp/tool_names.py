# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Tool names for TigerGraph MCP tools."""

from enum import Enum


class TigerGraphToolName(str, Enum):
    """Enumeration of all available TigerGraph MCP tool names."""

    # Global Schema Operations (Database level - operates on global schema)
    GET_GLOBAL_SCHEMA = "tigergraph__get_global_schema"

    # Graph Operations (Database level - operates on graphs within the database)
    LIST_GRAPHS = "tigergraph__list_graphs"
    CREATE_GRAPH = "tigergraph__create_graph"
    DROP_GRAPH = "tigergraph__drop_graph"
    CLEAR_GRAPH_DATA = "tigergraph__clear_graph_data"

    # Schema Operations (Graph level - operates on schema within a specific graph)
    GET_GRAPH_SCHEMA = "tigergraph__get_graph_schema"
    SHOW_GRAPH_DETAILS = "tigergraph__show_graph_details"

    # Node Operations
    ADD_NODE = "tigergraph__add_node"
    ADD_NODES = "tigergraph__add_nodes"
    GET_NODE = "tigergraph__get_node"
    GET_NODES = "tigergraph__get_nodes"
    DELETE_NODE = "tigergraph__delete_node"
    DELETE_NODES = "tigergraph__delete_nodes"
    HAS_NODE = "tigergraph__has_node"
    GET_NODE_EDGES = "tigergraph__get_node_edges"

    # Edge Operations
    ADD_EDGE = "tigergraph__add_edge"
    ADD_EDGES = "tigergraph__add_edges"
    GET_EDGE = "tigergraph__get_edge"
    GET_EDGES = "tigergraph__get_edges"
    DELETE_EDGE = "tigergraph__delete_edge"
    DELETE_EDGES = "tigergraph__delete_edges"
    HAS_EDGE = "tigergraph__has_edge"

    # Query Operations
    RUN_QUERY = "tigergraph__run_query"
    RUN_INSTALLED_QUERY = "tigergraph__run_installed_query"
    INSTALL_QUERY = "tigergraph__install_query"
    DROP_QUERY = "tigergraph__drop_query"
    SHOW_QUERY = "tigergraph__show_query"
    GET_QUERY_METADATA = "tigergraph__get_query_metadata"
    IS_QUERY_INSTALLED = "tigergraph__is_query_installed"
    GET_NEIGHBORS = "tigergraph__get_neighbors"

    # Loading Job Operations
    CREATE_LOADING_JOB = "tigergraph__create_loading_job"
    RUN_LOADING_JOB_WITH_FILE = "tigergraph__run_loading_job_with_file"
    RUN_LOADING_JOB_WITH_DATA = "tigergraph__run_loading_job_with_data"
    GET_LOADING_JOBS = "tigergraph__get_loading_jobs"
    GET_LOADING_JOB_STATUS = "tigergraph__get_loading_job_status"
    DROP_LOADING_JOB = "tigergraph__drop_loading_job"

    # Statistics
    GET_VERTEX_COUNT = "tigergraph__get_vertex_count"
    GET_EDGE_COUNT = "tigergraph__get_edge_count"
    GET_NODE_DEGREE = "tigergraph__get_node_degree"

    # GSQL Operations
    GSQL = "tigergraph__gsql"
    GENERATE_GSQL = "tigergraph__generate_gsql"
    GENERATE_CYPHER = "tigergraph__generate_cypher"

    # Vector Schema Operations
    ADD_VECTOR_ATTRIBUTE = "tigergraph__add_vector_attribute"
    DROP_VECTOR_ATTRIBUTE = "tigergraph__drop_vector_attribute"
    LIST_VECTOR_ATTRIBUTES = "tigergraph__list_vector_attributes"
    GET_VECTOR_INDEX_STATUS = "tigergraph__get_vector_index_status"

    # Vector Data Operations
    UPSERT_VECTORS = "tigergraph__upsert_vectors"
    LOAD_VECTORS_FROM_CSV = "tigergraph__load_vectors_from_csv"
    LOAD_VECTORS_FROM_JSON = "tigergraph__load_vectors_from_json"
    SEARCH_TOP_K_SIMILARITY = "tigergraph__search_top_k_similarity"
    FETCH_VECTOR = "tigergraph__fetch_vector"

    # Data Source Operations
    CREATE_DATA_SOURCE = "tigergraph__create_data_source"
    UPDATE_DATA_SOURCE = "tigergraph__update_data_source"
    GET_DATA_SOURCE = "tigergraph__get_data_source"
    DROP_DATA_SOURCE = "tigergraph__drop_data_source"
    GET_ALL_DATA_SOURCES = "tigergraph__get_all_data_sources"
    DROP_ALL_DATA_SOURCES = "tigergraph__drop_all_data_sources"
    PREVIEW_SAMPLE_DATA = "tigergraph__preview_sample_data"

    # Connection Profile Operations
    LIST_CONNECTIONS = "tigergraph__list_connections"
    SHOW_CONNECTION = "tigergraph__show_connection"

    # Discovery and Navigation Operations
    DISCOVER_TOOLS = "tigergraph__discover_tools"
    GET_WORKFLOW = "tigergraph__get_workflow"
    GET_TOOL_INFO = "tigergraph__get_tool_info"

    @classmethod
    def from_value(cls, value: str) -> "TigerGraphToolName":
        """Get enum from string value."""
        for tool in cls:
            if tool.value == value:
                return tool
        raise ValueError(f"Unknown tool name: {value}")

