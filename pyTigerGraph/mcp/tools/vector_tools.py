# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Vector operation tools for MCP.

TigerGraph Vector Database Operations:
- Vectors are stored as specialty attributes on vertices
- Vector search uses the vectorSearch() function in GSQL queries
- Vector attributes CANNOT be fetched via REST API - must use GSQL with "PRINT v WITH VECTOR"
- Supported metrics: COSINE, L2 (Euclidean), IP (inner product)
- Max dimensions: 4096 (Community Edition), 32768 (Enterprise Edition)
"""

import json
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from mcp.types import Tool, TextContent

from ..tool_names import TigerGraphToolName
from ..connection_manager import get_connection


# =============================================================================
# Vector Schema Input Models
# =============================================================================

class VectorAddAttributeToolInput(BaseModel):
    """Input schema for adding a vector attribute to a vertex type."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    vertex_type: str = Field(..., description="Name of the vertex type to add the vector attribute to.")
    vector_name: str = Field(..., description="Name of the vector attribute.")
    dimension: int = Field(..., description="Dimension (length) of the vector. Max 4096 for Community, 32768 for Enterprise.")
    metric: str = Field("COSINE", description="Similarity metric: 'COSINE', 'L2' (Euclidean), or 'IP' (inner product).")


class VectorDropAttributeToolInput(BaseModel):
    """Input schema for dropping a vector attribute from a vertex type."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    vertex_type: str = Field(..., description="Name of the vertex type.")
    vector_name: str = Field(..., description="Name of the vector attribute to drop.")


class VectorIndexStatusToolInput(BaseModel):
    """Input schema for checking vector index status."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    vertex_type: Optional[str] = Field(None, description="Vertex type to check. If not provided, checks all.")
    vector_name: Optional[str] = Field(None, description="Vector attribute name. If not provided, checks all.")


# =============================================================================
# Vector Data Input Models
# =============================================================================

class VectorData(BaseModel):
    """Schema for a single vector upsert operation."""
    vertex_id: Union[str, int] = Field(..., description="ID of the vertex.")
    vector: List[float] = Field(..., description="Vector data as a list of floats.")
    attributes: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional vertex attributes.")


class VectorUpsertToolInput(BaseModel):
    """Input schema for upserting multiple vectors via REST API."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    vertex_type: str = Field(..., description="Type of the vertices.")
    vector_attribute: str = Field(..., description="Name of the vector attribute.")
    vectors: List[VectorData] = Field(..., description="List of vectors to upsert, each with vertex_id, vector, and optional attributes.")


class VectorSearchToolInput(BaseModel):
    """Input schema for vector similarity search using vectorSearch() function."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    vertex_type: str = Field(..., description="Type of vertices to search.")
    vector_attribute: str = Field(..., description="Name of the vector attribute to search.")
    query_vector: List[float] = Field(..., description="Query vector for similarity search.")
    top_k: int = Field(10, description="Number of top similar results to return.")
    ef: Optional[int] = Field(None, description="Exploration factor for HNSW algorithm. Higher = more accurate but slower.")
    return_vectors: bool = Field(False, description="Whether to return the vector values (can be large).")


class VectorFetchToolInput(BaseModel):
    """Input schema for fetching vertices with their vector data using GSQL."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    vertex_type: str = Field(..., description="Type of the vertex.")
    vertex_ids: List[Union[str, int]] = Field(..., description="List of vertex IDs to fetch.")
    vector_attribute: Optional[str] = Field(None, description="Specific vector attribute to fetch. If not provided, fetches all vectors.")


# =============================================================================
# Vector Schema Tools
# =============================================================================

add_vector_attribute_tool = Tool(
    name=TigerGraphToolName.ADD_VECTOR_ATTRIBUTE,
    description="Add a vector attribute to an existing vertex type. Creates a schema change job to ALTER VERTEX with ADD VECTOR ATTRIBUTE.",
    inputSchema=VectorAddAttributeToolInput.model_json_schema(),
)

drop_vector_attribute_tool = Tool(
    name=TigerGraphToolName.DROP_VECTOR_ATTRIBUTE,
    description="Drop a vector attribute from a vertex type. Creates a schema change job to ALTER VERTEX with DROP VECTOR ATTRIBUTE.",
    inputSchema=VectorDropAttributeToolInput.model_json_schema(),
)

get_vector_index_status_tool = Tool(
    name=TigerGraphToolName.GET_VECTOR_INDEX_STATUS,
    description="Check the rebuild status of vector indexes. Returns 'Ready_for_query' when complete or 'Rebuild_processing' if still building.",
    inputSchema=VectorIndexStatusToolInput.model_json_schema(),
)


# =============================================================================
# Vector Data Tools
# =============================================================================

upsert_vectors_tool = Tool(
    name=TigerGraphToolName.UPSERT_VECTORS,
    description="Upsert multiple vertices with vector data using the REST Upsert API. Supports batch operations for efficiency.",
    inputSchema=VectorUpsertToolInput.model_json_schema(),
)

search_top_k_similarity_tool = Tool(
    name=TigerGraphToolName.SEARCH_TOP_K_SIMILARITY,
    description="Perform vector similarity search using TigerGraph's vectorSearch() function. Returns top-K most similar vertices with distance scores.",
    inputSchema=VectorSearchToolInput.model_json_schema(),
)

fetch_vector_tool = Tool(
    name=TigerGraphToolName.FETCH_VECTOR,
    description="Fetch vertices with their vector data using GSQL PRINT WITH VECTOR. Note: Vector attributes cannot be fetched via REST API.",
    inputSchema=VectorFetchToolInput.model_json_schema(),
)


# =============================================================================
# Vector Schema Implementations
# =============================================================================

async def add_vector_attribute(
    vertex_type: str,
    vector_name: str,
    dimension: int,
    metric: str = "COSINE",
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Add a vector attribute to a vertex type using a schema change job."""
    try:
        conn = get_connection(graph_name=graph_name)

        # Validate metric
        metric = metric.upper()
        if metric not in ["COSINE", "L2", "IP"]:
            return [TextContent(type="text", text=f"Error: Invalid metric '{metric}'. Must be 'COSINE', 'L2', or 'IP'.")]

        # Create and run schema change job
        job_name = f"add_vector_{vector_name}_{vertex_type}"
        gsql_cmd = f"""
CREATE GLOBAL SCHEMA_CHANGE JOB {job_name} {{
  ALTER VERTEX {vertex_type} ADD VECTOR ATTRIBUTE {vector_name}(DIMENSION={dimension}, METRIC="{metric}");
}}
RUN GLOBAL SCHEMA_CHANGE JOB {job_name} -N
DROP JOB {job_name}
"""
        result = await conn.gsql(gsql_cmd)
        message = f"Success: Vector attribute '{vector_name}' added to vertex type '{vertex_type}':\n  - Dimension: {dimension}\n  - Metric: {metric}\n\nResult:\n{result}"
    except Exception as e:
        message = f"Failed to add vector attribute due to: {str(e)}"
    return [TextContent(type="text", text=message)]


async def drop_vector_attribute(
    vertex_type: str,
    vector_name: str,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Drop a vector attribute from a vertex type."""
    try:
        conn = get_connection(graph_name=graph_name)

        job_name = f"drop_vector_{vector_name}_{vertex_type}"
        gsql_cmd = f"""
CREATE GLOBAL SCHEMA_CHANGE JOB {job_name} {{
  ALTER VERTEX {vertex_type} DROP VECTOR ATTRIBUTE {vector_name};
}}
RUN GLOBAL SCHEMA_CHANGE JOB {job_name} -N
DROP JOB {job_name}
"""
        result = await conn.gsql(gsql_cmd)
        message = f"Success: Vector attribute '{vector_name}' dropped from vertex type '{vertex_type}':\n{result}"
    except Exception as e:
        message = f"Failed to drop vector attribute due to: {str(e)}"
    return [TextContent(type="text", text=message)]


async def get_vector_index_status(
    graph_name: Optional[str] = None,
    vertex_type: Optional[str] = None,
    vector_name: Optional[str] = None,
) -> List[TextContent]:
    """Check the rebuild status of vector indexes."""
    try:
        conn = get_connection(graph_name=graph_name)

        # Build the endpoint path
        path = f"/vector/status/{conn.graphname}"
        if vertex_type:
            path += f"/{vertex_type}"
            if vector_name:
                path += f"/{vector_name}"

        # Use the connection's _req method to make the REST call
        result = await conn._req("GET", conn.restppUrl + path)

        # Parse status
        if result:
            need_rebuild = result.get("NeedRebuildServers", [])
            if len(need_rebuild) == 0:
                status = "Ready_for_query"
                status_msg = "Success: Vector index is ready for queries."
            else:
                status = "Rebuild_processing"
                status_msg = f"⏳ Vector index is still rebuilding on {len(need_rebuild)} server(s)."

            message = f"{status_msg}\n\nStatus: {status}\nDetails:\n{json.dumps(result, indent=2)}"
        else:
            message = "Success: No vector indexes found or status unavailable."
    except Exception as e:
        message = f"Failed to get vector index status due to: {str(e)}"
    return [TextContent(type="text", text=message)]


# =============================================================================
# Vector Data Implementations
# =============================================================================

async def upsert_vectors(
    vertex_type: str,
    vector_attribute: str,
    vectors: List[Dict[str, Any]],
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Upsert multiple vertices with vector data using REST Upsert API."""
    try:
        conn = get_connection(graph_name=graph_name)

        success_count = 0
        failed_ids = []
        dimensions = None

        for vec_data in vectors:
            try:
                vertex_id = vec_data["vertex_id"]
                vector = vec_data["vector"]
                attributes = vec_data.get("attributes", {})

                # Combine vector with other attributes
                all_attributes = attributes.copy() if attributes else {}
                all_attributes[vector_attribute] = vector

                await conn.upsertVertex(vertex_type, str(vertex_id), all_attributes)
                success_count += 1
                if dimensions is None:
                    dimensions = len(vector)
            except Exception as e:
                failed_ids.append((vec_data.get("vertex_id", "unknown"), str(e)))

        # Build result message
        if failed_ids:
            failed_msg = "\n".join([f"  - {vid}: {err}" for vid, err in failed_ids])
            message = f"Warning: Partial success: {success_count}/{len(vectors)} vectors upserted for vertex type '{vertex_type}':\n  - Vector attribute: {vector_attribute}\n  - Dimensions: {dimensions}\n\nFailed:\n{failed_msg}"
        else:
            message = f"Successfully upserted {success_count} vectors for vertex type '{vertex_type}':\n  - Vector attribute: {vector_attribute}\n  - Dimensions: {dimensions}"
    except Exception as e:
        message = f"Failed to upsert vectors due to: {str(e)}"
    return [TextContent(type="text", text=message)]


async def search_top_k_similarity(
    vertex_type: str,
    vector_attribute: str,
    query_vector: List[float],
    top_k: int = 10,
    ef: Optional[int] = None,
    return_vectors: bool = False,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Perform vector similarity search using vectorSearch() function."""
    try:
        conn = get_connection(graph_name=graph_name)

        # Format the query vector as a LIST<FLOAT>
        vector_str = ", ".join(str(v) for v in query_vector)

        # Build optional parameters
        optional_params = "distance_map: @@distances"
        if ef:
            optional_params += f", ef: {ef}"

        # Build the PRINT clause - with or without vectors
        print_clause = "PRINT v WITH VECTOR;" if return_vectors else "PRINT v;"

        # Use vectorSearch function as documented
        query = f"""
INTERPRET QUERY () FOR GRAPH {conn.graphname} SYNTAX v3 {{
    ListAccum<FLOAT> @@query_vec = [{vector_str}];
    MapAccum<VERTEX, FLOAT> @@distances;

    // Find top-{top_k} similar vectors using vectorSearch
    v = vectorSearch({{{vertex_type}.{vector_attribute}}}, @@query_vec, {top_k}, {{ {optional_params} }});

    {print_clause}
    PRINT @@distances AS distances;
}}
"""
        result = await conn.runInterpretedQuery(query)

        # Parse results
        vertices = []
        distances = {}
        if result:
            for item in result:
                if "v" in item:
                    vertices = item["v"]
                elif "distances" in item:
                    distances = item["distances"]

        # Format output
        output = {
            "query": f"Top {top_k} similar vertices to query vector ({len(query_vector)} dimensions)",
            "results_count": len(vertices),
            "vertices": vertices,
            "distances": distances
        }

        message = f"Success: Vector search completed:\n{json.dumps(output, indent=2, default=str)}"
    except Exception as e:
        message = f"Failed to perform vector search due to: {str(e)}\n\nNote: Ensure the vector attribute exists and has been indexed. Check status with vector_index_status."
    return [TextContent(type="text", text=message)]


async def fetch_vector(
    vertex_type: str,
    vertex_ids: List[Union[str, int]],
    vector_attribute: Optional[str] = None,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Fetch vertices with their vector data using GSQL PRINT WITH VECTOR.

    Note: Vector attributes CANNOT be fetched via REST API - must use GSQL.
    """
    try:
        conn = get_connection(graph_name=graph_name)

        # Build to_vertex() calls for each ID
        to_vertex_calls = "\n            ".join(
            f'@@seeds += to_vertex("{vid}", "{vertex_type}");' 
            for vid in vertex_ids
        )

        # Use GSQL to fetch vertices with vectors using to_vertex()
        query = f"""
INTERPRET QUERY () FOR GRAPH {conn.graphname} SYNTAX v3 {{
    SetAccum<VERTEX> @@seeds;
    
    {to_vertex_calls}
    src = {{@@seeds}};
    
    v = SELECT s FROM src:s;

    PRINT v WITH VECTOR;
}}
"""
        result = await conn.runInterpretedQuery(query)

        # Parse results
        vertices = []
        if result:
            for item in result:
                if "v" in item:
                    vertices = item["v"]

        if vertices:
            message = f"Success: Fetched {len(vertices)} vertex(ices) with vector data:\n{json.dumps(vertices, indent=2, default=str)}"
        else:
            message = f"Error: No vertices found with IDs: {vertex_ids}"
    except Exception as e:
        message = f"Failed to fetch vectors due to: {str(e)}\n\nNote: Vector attributes can only be retrieved via GSQL queries with 'PRINT WITH VECTOR'."
    return [TextContent(type="text", text=message)]
