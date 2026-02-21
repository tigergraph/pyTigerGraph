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


class VectorListAttributesToolInput(BaseModel):
    """Input schema for listing vector attributes in a graph."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    vertex_type: Optional[str] = Field(None, description="Filter by vertex type. If not provided, returns vector attributes for all vertex types.")


class VectorIndexStatusToolInput(BaseModel):
    """Input schema for checking vector index status."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    vertex_type: Optional[str] = Field(None, description="Vertex type to check. If not provided, checks all.")
    vector_name: Optional[str] = Field(None, description="Vector attribute name. If not provided, checks all.")


# =============================================================================
# Vector Loading Input Models
# =============================================================================

class VectorLoadFromCsvToolInput(BaseModel):
    """Input schema for bulk-loading vectors from a CSV/delimited file via a GSQL loading job."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    vertex_type: str = Field(..., description="Target vertex type that has the vector attribute.")
    vector_attribute: str = Field(..., description="Name of the vector attribute to load into.")
    file_path: str = Field(..., description="Absolute path to the CSV/delimited data file on the local machine (uploaded to TigerGraph via REST). Each row has a vertex ID and a vector column.")
    id_column: Union[str, int] = Field(0, description="Column for vertex ID: integer index (0-based) or header name. Default: 0 (first column).")
    vector_column: Union[str, int] = Field(1, description="Column containing the vector data: integer index (0-based) or header name. Default: 1 (second column).")
    element_separator: str = Field(",", description="Separator between vector elements within the vector column. Default: ','.")
    field_separator: str = Field("|", description="Separator between fields (columns) in the file. Default: '|'.")
    header: bool = Field(False, description="Whether the file has a header row. Default: false.")


class VectorLoadFromJsonToolInput(BaseModel):
    """Input schema for bulk-loading vectors from a JSON Lines file via a GSQL loading job."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    vertex_type: str = Field(..., description="Target vertex type that has the vector attribute.")
    vector_attribute: str = Field(..., description="Name of the vector attribute to load into.")
    file_path: str = Field(..., description="Absolute path to the JSON Lines (.jsonl) file on the local machine (uploaded to TigerGraph via REST). Each line is a JSON object with an ID field and a vector field.")
    id_key: str = Field("id", description="JSON key for the vertex ID. Default: 'id'.")
    vector_key: str = Field("vector", description="JSON key for the vector data (stored as a comma-separated string). Default: 'vector'.")
    element_separator: str = Field(",", description="Separator between vector elements within the vector string value. Default: ','.")


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

list_vector_attributes_tool = Tool(
    name=TigerGraphToolName.LIST_VECTOR_ATTRIBUTES,
    description=(
        "Get vector attribute information (name, dimension, metric) for vertex types in a graph. "
        "Parses the output of the GSQL 'LS' command. Optionally filter by vertex type.\n\n"
        "**Related Tools:** add_vector_attribute, drop_vector_attribute, get_vector_index_status"
    ),
    inputSchema=VectorListAttributesToolInput.model_json_schema(),
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
    description=(
        "Upsert multiple vertices with vector data using the REST Upsert API. "
        "Vectors must be provided inline as lists of floats (i.e., already in memory). "
        "To bulk-load vectors from a local file, use 'load_vectors_from_csv' or 'load_vectors_from_json' instead."
    ),
    inputSchema=VectorUpsertToolInput.model_json_schema(),
)

search_top_k_similarity_tool = Tool(
    name=TigerGraphToolName.SEARCH_TOP_K_SIMILARITY,
    description=(
        "Perform vector similarity search using TigerGraph's vectorSearch() function. "
        "Returns top-K most similar vertices with distance scores.\n\n"

        "**IMPORTANT:** The ``query_vector`` dimensions MUST match the dimension defined "
        "in the vector attribute (e.g., if the attribute was created with DIMENSION=1536, "
        "the query vector must have exactly 1536 elements). A dimension mismatch will cause "
        "the search to fail or return incorrect results.\n\n"

        "Use ``list_vector_attributes`` to check the expected dimension before searching.\n\n"

        "**Related Tools:** list_vector_attributes (check dimension), "
        "fetch_vector (retrieve vector values), get_vector_index_status (check index readiness)"
    ),
    inputSchema=VectorSearchToolInput.model_json_schema(),
)

fetch_vector_tool = Tool(
    name=TigerGraphToolName.FETCH_VECTOR,
    description="Fetch vertices with their vector data using GSQL PRINT WITH VECTOR. Note: Vector attributes cannot be fetched via REST API.",
    inputSchema=VectorFetchToolInput.model_json_schema(),
)

load_vectors_from_csv_tool = Tool(
    name=TigerGraphToolName.LOAD_VECTORS_FROM_CSV,
    description=(
        "Bulk-load vectors from a CSV/delimited file into a vertex type's vector attribute. "
        "Creates a GSQL loading job, runs it with the file, then drops the job.\n\n"

        "**File format:** Each row has a vertex ID and a vector. Fields are separated by "
        "``field_separator`` (default ``|``). Vector elements are separated by "
        "``element_separator`` (default ``,``).\n\n"

        "**Example file** (field_separator='|', element_separator=','):\n"
        "```\n"
        "vertex1|0.1,0.2,0.3\n"
        "vertex2|0.4,0.5,0.6\n"
        "```\n\n"

        "**Prerequisites:**\n"
        "  1. Vertex type must already exist\n"
        "  2. Vector attribute must already be added (use 'add_vector_attribute')\n"
        "  3. File must exist on the local machine (it is uploaded to TigerGraph via REST)\n\n"

        "**Related Tools:** add_vector_attribute, load_vectors_from_json (JSON Lines alternative), "
        "upsert_vectors (REST API for in-memory data), get_vector_index_status (check indexing after load)"
    ),
    inputSchema=VectorLoadFromCsvToolInput.model_json_schema(),
)

load_vectors_from_json_tool = Tool(
    name=TigerGraphToolName.LOAD_VECTORS_FROM_JSON,
    description=(
        "Bulk-load vectors from a JSON Lines (.jsonl) file into a vertex type's vector attribute. "
        "Creates a GSQL loading job with JSON_FILE=\"true\", runs it with the file, then drops the job.\n\n"

        "**File format:** Each line is a JSON object with an ID field and a vector field. "
        "The vector is stored as a comma-separated string (not a JSON array).\n\n"

        "**Example file** (id_key='id', vector_key='embedding'):\n"
        "```\n"
        '{"id": "vertex1", "embedding": "0.1,0.2,0.3"}\n'
        '{"id": "vertex2", "embedding": "0.4,0.5,0.6"}\n'
        "```\n\n"

        "**Prerequisites:**\n"
        "  1. Vertex type must already exist\n"
        "  2. Vector attribute must already be added (use 'add_vector_attribute')\n"
        "  3. File must exist on the local machine (it is uploaded to TigerGraph via REST)\n\n"

        "**Related Tools:** add_vector_attribute, load_vectors_from_csv (CSV alternative), "
        "upsert_vectors (REST API for in-memory data), get_vector_index_status (check indexing after load)"
    ),
    inputSchema=VectorLoadFromJsonToolInput.model_json_schema(),
)


# =============================================================================
# Vector Schema Implementations
# =============================================================================

async def _is_global_vertex_type(conn, vertex_type: str) -> bool:
    """Check whether a vertex type is global by running SHOW VERTEX at global scope."""
    import re
    try:
        result = await conn.gsql(f"USE GLOBAL\nSHOW VERTEX {vertex_type}")
        result_str = str(result) if result else ""
        return bool(re.search(r'VERTEX\s+' + re.escape(vertex_type) + r'\b', result_str))
    except Exception:
        return False


def _build_schema_change_gsql(
    job_name: str,
    graph_name: str,
    alter_stmt: str,
    is_global: bool,
) -> str:
    """Build a global or local schema change job GSQL block."""
    if is_global:
        return (
            f"CREATE GLOBAL SCHEMA_CHANGE JOB {job_name} {{\n"
            f"  {alter_stmt}\n"
            f"}}\n"
            f"RUN GLOBAL SCHEMA_CHANGE JOB {job_name}\n"
            f"DROP JOB {job_name}"
        )
    return (
        f"USE GRAPH {graph_name}\n"
        f"CREATE SCHEMA_CHANGE JOB {job_name} FOR GRAPH {graph_name} {{\n"
        f"  {alter_stmt}\n"
        f"}}\n"
        f"RUN SCHEMA_CHANGE JOB {job_name}\n"
        f"DROP JOB {job_name}"
    )


async def add_vector_attribute(
    vertex_type: str,
    vector_name: str,
    dimension: int,
    metric: str = "COSINE",
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Add a vector attribute to a vertex type.

    Automatically detects whether the vertex type is global or local and uses
    the corresponding schema change job type.
    """
    from ..response_formatter import format_success, format_error, gsql_has_error

    try:
        conn = get_connection(graph_name=graph_name)
        gname = conn.graphname

        metric = metric.upper()
        if metric not in ["COSINE", "L2", "IP"]:
            return format_error(
                operation="add_vector_attribute",
                error=ValueError(f"Invalid metric '{metric}'. Must be 'COSINE', 'L2', or 'IP'."),
                context={"vertex_type": vertex_type, "vector_name": vector_name},
            )

        is_global = await _is_global_vertex_type(conn, vertex_type)
        job_name = f"add_vector_{vector_name}_{vertex_type}"
        alter_stmt = f'ALTER VERTEX {vertex_type} ADD VECTOR ATTRIBUTE {vector_name}(DIMENSION={dimension}, METRIC="{metric}");'
        gsql_cmd = _build_schema_change_gsql(job_name, gname, alter_stmt, is_global)

        result = await conn.gsql(gsql_cmd)
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="add_vector_attribute",
                error=Exception(f"Failed to add vector attribute:\n{result_str}"),
                context={"graph_name": gname, "vertex_type": vertex_type, "vector_name": vector_name},
            )

        scope = "global" if is_global else "local"
        return format_success(
            operation="add_vector_attribute",
            summary=f"Vector attribute '{vector_name}' added to {scope} vertex type '{vertex_type}'",
            data={
                "graph_name": gname,
                "vertex_type": vertex_type,
                "vector_name": vector_name,
                "dimension": dimension,
                "metric": metric,
                "scope": scope,
                "gsql_result": result_str,
            },
            suggestions=[
                f"Check index status: get_vector_index_status(vertex_type='{vertex_type}', vector_name='{vector_name}')",
                f"List vector attributes: list_vector_attributes(graph_name='{gname}')",
                f"Load vectors: load_vectors_from_csv(...) or load_vectors_from_json(...)",
            ],
        )
    except Exception as e:
        return format_error(
            operation="add_vector_attribute",
            error=e,
            context={"vertex_type": vertex_type, "vector_name": vector_name},
        )


async def drop_vector_attribute(
    vertex_type: str,
    vector_name: str,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Drop a vector attribute from a vertex type.

    Automatically detects whether the vertex type is global or local and uses
    the corresponding schema change job type.
    """
    from ..response_formatter import format_success, format_error, gsql_has_error

    try:
        conn = get_connection(graph_name=graph_name)
        gname = conn.graphname

        is_global = await _is_global_vertex_type(conn, vertex_type)
        job_name = f"drop_vector_{vector_name}_{vertex_type}"
        alter_stmt = f"ALTER VERTEX {vertex_type} DROP VECTOR ATTRIBUTE {vector_name};"
        gsql_cmd = _build_schema_change_gsql(job_name, gname, alter_stmt, is_global)

        result = await conn.gsql(gsql_cmd)
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="drop_vector_attribute",
                error=Exception(f"Failed to drop vector attribute:\n{result_str}"),
                context={"graph_name": gname, "vertex_type": vertex_type, "vector_name": vector_name},
            )

        scope = "global" if is_global else "local"
        return format_success(
            operation="drop_vector_attribute",
            summary=f"Vector attribute '{vector_name}' dropped from {scope} vertex type '{vertex_type}'",
            data={
                "graph_name": gname,
                "vertex_type": vertex_type,
                "vector_name": vector_name,
                "scope": scope,
                "gsql_result": result_str,
            },
            suggestions=[
                f"List remaining vector attributes: list_vector_attributes(graph_name='{gname}')",
                f"View schema: get_graph_schema(graph_name='{gname}')",
            ],
        )
    except Exception as e:
        return format_error(
            operation="drop_vector_attribute",
            error=e,
            context={"vertex_type": vertex_type, "vector_name": vector_name},
        )


async def list_vector_attributes(
    graph_name: Optional[str] = None,
    vertex_type: Optional[str] = None,
) -> List[TextContent]:
    """Get vector attribute details by parsing the GSQL LS output.

    The LS output has a ``Vector Embeddings:`` section structured as::

        Vector Embeddings:
          - Person:
            - embedding(Dimension=1536, IndexType="HNSW", DataType="FLOAT", Metric="COSINE")

    Returns structured data with vertex_type, vector_name, dimension, index_type,
    data_type, and metric for each vector attribute.
    """
    import re
    from ..response_formatter import format_success, format_error, gsql_has_error

    try:
        conn = get_connection(graph_name=graph_name)
        gname = conn.graphname

        result = await conn.gsql(f"USE GRAPH {gname}\nLS")
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="list_vector_attributes",
                error=Exception(f"LS command failed:\n{result_str}"),
                context={"graph_name": gname},
            )

        # State machine: detect "Vector Embeddings:" section, then parse
        #   - <VertexType>:
        #     - <vec_name>(Key=Value, ...)
        in_vector_section = False
        current_vertex: Optional[str] = None
        vector_attrs: List[Dict[str, Any]] = []

        vertex_header_re = re.compile(r'^\s*-\s+(\w+)\s*:\s*$')
        vec_attr_re = re.compile(r'^\s*-\s+(\w+)\((.+)\)\s*$')
        kv_re = re.compile(r'(\w+)\s*=\s*"?([^",]+)"?')

        for line in result_str.splitlines():
            stripped = line.strip()

            if stripped.startswith("Vector Embeddings"):
                in_vector_section = True
                current_vertex = None
                continue

            if in_vector_section:
                # A non-indented section header ends the vector block
                if stripped and not line[0].isspace() and not stripped.startswith("-"):
                    in_vector_section = False
                    current_vertex = None
                    continue

                vm = vertex_header_re.match(line)
                if vm:
                    current_vertex = vm.group(1)
                    continue

                vecm = vec_attr_re.match(line)
                if vecm and current_vertex:
                    vec_name = vecm.group(1)
                    params_str = vecm.group(2)
                    params = {k: v for k, v in kv_re.findall(params_str)}
                    entry: Dict[str, Any] = {
                        "vertex_type": current_vertex,
                        "vector_name": vec_name,
                    }
                    if "Dimension" in params:
                        entry["dimension"] = int(params["Dimension"])
                    if "IndexType" in params:
                        entry["index_type"] = params["IndexType"]
                    if "DataType" in params:
                        entry["data_type"] = params["DataType"]
                    if "Metric" in params:
                        entry["metric"] = params["Metric"].upper()
                    vector_attrs.append(entry)

        if vertex_type:
            vector_attrs = [v for v in vector_attrs if v["vertex_type"] == vertex_type]

        if vector_attrs:
            summary = f"Found {len(vector_attrs)} vector attribute(s)"
            if vertex_type:
                summary += f" on vertex type '{vertex_type}'"
        else:
            summary = "No vector attributes found"
            if vertex_type:
                summary += f" on vertex type '{vertex_type}'"

        return format_success(
            operation="list_vector_attributes",
            summary=summary,
            data={
                "graph_name": gname,
                "vector_attributes": vector_attrs,
                "count": len(vector_attrs),
            },
            suggestions=[
                "Add a vector attribute: add_vector_attribute(vertex_type='...', vector_name='...', dimension=...)",
                "Check index status: get_vector_index_status()",
            ] if not vector_attrs else [
                f"Check index status: get_vector_index_status(vertex_type='{vector_attrs[0]['vertex_type']}', vector_name='{vector_attrs[0]['vector_name']}')",
                f"Search vectors: search_top_k_similarity(vertex_type='{vector_attrs[0]['vertex_type']}', vector_attribute='{vector_attrs[0]['vector_name']}', ...)",
                "Load vectors: load_vectors_from_csv(...) or load_vectors_from_json(...)",
            ],
        )
    except Exception as e:
        return format_error(
            operation="list_vector_attributes",
            error=e,
            context={"graph_name": graph_name},
        )


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
                status_msg = f"Vector index is still rebuilding on {len(need_rebuild)} server(s)."

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
    """Perform vector similarity search using vectorSearch() function.

    ``vectorSearch()`` is not supported in interpreted mode, so this function
    creates a temporary installed query with a ``LIST<FLOAT>`` parameter,
    passes the query vector via REST, and drops the query afterward.
    """
    import re
    import uuid
    from ..response_formatter import format_success, format_error, gsql_has_error

    query_name = None
    gname = None

    try:
        conn = get_connection(graph_name=graph_name)
        gname = conn.graphname

        # Pre-flight: check query_vector dimension against the attribute definition
        ls_result = await conn.gsql(f"USE GRAPH {gname}\nLS")
        ls_str = str(ls_result) if ls_result else ""
        dim_match = re.search(
            re.escape(vector_attribute) + r'\(.*?Dimension\s*=\s*(\d+)',
            ls_str, re.IGNORECASE,
        )
        if dim_match:
            expected_dim = int(dim_match.group(1))
            actual_dim = len(query_vector)
            if actual_dim != expected_dim:
                return format_error(
                    operation="search_top_k_similarity",
                    error=ValueError(
                        f"Query vector dimension mismatch: expected {expected_dim} "
                        f"(defined in {vertex_type}.{vector_attribute}), got {actual_dim}."
                    ),
                    context={
                        "graph_name": gname,
                        "vertex_type": vertex_type,
                        "vector_attribute": vector_attribute,
                        "expected_dimension": expected_dim,
                        "actual_dimension": actual_dim,
                    },
                    suggestions=[
                        f"Provide a query vector with exactly {expected_dim} elements",
                        f"Verify dimension: list_vector_attributes(graph_name='{gname}', vertex_type='{vertex_type}')",
                        "If using an embedding model, ensure it produces the correct dimension",
                    ],
                )

        query_name = f"_vec_search_{uuid.uuid4().hex[:8]}"

        optional_params = "distance_map: @@distances"
        if ef:
            optional_params += f", ef: {ef}"

        print_clause = "PRINT v WITH VECTOR;" if return_vectors else "PRINT v;"

        create_gsql = (
            f"USE GRAPH {gname}\n"
            f"CREATE QUERY {query_name}(LIST<FLOAT> query_vec, INT k) FOR GRAPH {gname} SYNTAX v3 {{\n"
            f"  MapAccum<VERTEX, FLOAT> @@distances;\n"
            f"  v = vectorSearch({{{vertex_type}.{vector_attribute}}}, query_vec, k, {{ {optional_params} }});\n"
            f"  {print_clause}\n"
            f"  PRINT @@distances AS distances;\n"
            f"}}\n"
            f"INSTALL QUERY {query_name}"
        )

        create_result = await conn.gsql(create_gsql)
        create_str = str(create_result) if create_result else ""

        if gsql_has_error(create_str):
            return format_error(
                operation="search_top_k_similarity",
                error=Exception(f"Failed to create/install vector search query:\n{create_str}"),
                context={
                    "graph_name": gname,
                    "vertex_type": vertex_type,
                    "vector_attribute": vector_attribute,
                },
                suggestions=[
                    f"Check index status: get_vector_index_status(vertex_type='{vertex_type}', vector_name='{vector_attribute}')",
                    f"List vector attributes: list_vector_attributes(graph_name='{gname}')",
                ],
            )

        try:
            run_result = await conn.runInstalledQuery(
                query_name,
                params={"query_vec": query_vector, "k": top_k},
            )
        finally:
            try:
                await conn.gsql(f"USE GRAPH {gname}\nDROP QUERY {query_name}")
            except Exception:
                pass

        return format_success(
            operation="search_top_k_similarity",
            summary=f"Top-{top_k} vector search on {vertex_type}.{vector_attribute} ({len(query_vector)} dimensions)",
            data={
                "graph_name": gname,
                "vertex_type": vertex_type,
                "vector_attribute": vector_attribute,
                "top_k": top_k,
                "ef": ef,
                "return_vectors": return_vectors,
                "result": run_result,
            },
            suggestions=[
                f"Check index status: get_vector_index_status(vertex_type='{vertex_type}', vector_name='{vector_attribute}')",
                f"Fetch specific vectors: fetch_vector(vertex_type='{vertex_type}', vertex_ids=[...])",
            ],
        )
    except Exception as e:
        if query_name and gname:
            try:
                await conn.gsql(f"USE GRAPH {gname}\nDROP QUERY {query_name}")
            except Exception:
                pass
        return format_error(
            operation="search_top_k_similarity",
            error=e,
            context={
                "vertex_type": vertex_type,
                "vector_attribute": vector_attribute,
                "top_k": top_k,
                "note": "Ensure the vector attribute exists and has been indexed. Check with get_vector_index_status.",
            },
        )


async def fetch_vector(
    vertex_type: str,
    vertex_ids: List[Union[str, int]],
    vector_attribute: Optional[str] = None,
    graph_name: Optional[str] = None,
    **kwargs,
) -> List[TextContent]:
    """Fetch vertices with their vector data using GSQL PRINT WITH VECTOR.

    ``PRINT WITH VECTOR`` does not work in interpreted mode, so this function
    creates a temporary installed query, runs it, and then drops it.

    Workflow:
        1. CREATE QUERY (temp) with ``to_vertex()`` + ``PRINT v WITH VECTOR``
        2. INSTALL QUERY
        3. RUN QUERY via ``runInstalledQuery``
        4. DROP QUERY
    """
    from ..response_formatter import format_success, format_error, gsql_has_error
    import uuid

    try:
        conn = get_connection(graph_name=graph_name)
        gname = conn.graphname

        query_name = f"temp_fetch_vec_{uuid.uuid4().hex[:8]}"

        to_vertex_calls = "\n  ".join(
            f'@@seeds += to_vertex("{vid}", "{vertex_type}");'
            for vid in vertex_ids
        )

        create_gsql = (
            f"USE GRAPH {gname}\n"
            f"CREATE QUERY {query_name}() FOR GRAPH {gname} {{\n"
            f"  SetAccum<VERTEX> @@seeds;\n"
            f"  {to_vertex_calls}\n"
            f"  src = {{@@seeds}};\n"
            f"  v = SELECT s FROM src:s;\n"
            f"  PRINT v WITH VECTOR;\n"
            f"}}\n"
            f"INSTALL QUERY {query_name}"
        )

        create_result = await conn.gsql(create_gsql)
        create_str = str(create_result) if create_result else ""

        if gsql_has_error(create_str):
            return format_error(
                operation="fetch_vector",
                error=Exception(f"Failed to create/install temp query:\n{create_str}"),
                context={
                    "graph_name": gname,
                    "vertex_type": vertex_type,
                    "vertex_ids": vertex_ids,
                },
            )

        try:
            run_result = await conn.runInstalledQuery(query_name)
        finally:
            try:
                await conn.gsql(f"USE GRAPH {gname}\nDROP QUERY {query_name}")
            except Exception:
                pass

        return format_success(
            operation="fetch_vector",
            summary=f"Fetched vector data for {len(vertex_ids)} vertex ID(s) of type '{vertex_type}'",
            data={
                "graph_name": gname,
                "vertex_type": vertex_type,
                "vertex_ids": vertex_ids,
                "vector_attribute": vector_attribute,
                "result": run_result,
            },
            suggestions=[
                "Note: PRINT WITH VECTOR returns all vector attributes on the vertex type",
                f"Search similar vectors: search_top_k_similarity(vertex_type='{vertex_type}', ...)",
            ],
        )
    except Exception as e:
        try:
            await conn.gsql(f"USE GRAPH {gname}\nDROP QUERY {query_name}")
        except Exception:
            pass
        return format_error(
            operation="fetch_vector",
            error=e,
            context={
                "vertex_type": vertex_type,
                "vertex_ids": vertex_ids,
                "note": "Vector attributes require an installed query with PRINT WITH VECTOR",
            },
        )


# =============================================================================
# Vector File Loading Implementation
# =============================================================================

async def load_vectors_from_csv(
    vertex_type: str,
    vector_attribute: str,
    file_path: str,
    id_column: Union[str, int] = 0,
    vector_column: Union[str, int] = 1,
    element_separator: str = ",",
    field_separator: str = "|",
    header: bool = False,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Bulk-load vectors from a local CSV/delimited file using a GSQL loading job.

    The file is uploaded from the local machine to TigerGraph via the REST API.

    Workflow:
        1. CREATE LOADING JOB with LOAD ... TO VECTOR ATTRIBUTE
        2. Upload and run the job with the local file via ``runLoadingJobWithFile``
        3. DROP the job

    See: https://docs.tigergraph.com/gsql-ref/4.2/vector/#loading_vectors
    """
    from ..response_formatter import format_success, format_error, gsql_has_error

    try:
        conn = get_connection(graph_name=graph_name)
        gname = conn.graphname

        job_name = f"load_vec_csv_{vector_attribute}_{vertex_type}"
        file_tag = "vec_file"

        id_col = f'$"{id_column}"' if isinstance(id_column, str) else f"${id_column}"
        vec_col = f'$"{vector_column}"' if isinstance(vector_column, str) else f"${vector_column}"

        header_clause = f', HEADER="true"' if header else ""

        gsql_cmd = (
            f"USE GRAPH {gname}\n"
            f"DROP JOB {job_name}\n"
            f"CREATE LOADING JOB {job_name} FOR GRAPH {gname} {{\n"
            f'  DEFINE FILENAME {file_tag};\n'
            f"  LOAD {file_tag} TO VECTOR ATTRIBUTE {vector_attribute} ON VERTEX {vertex_type}\n"
            f'    VALUES ({id_col}, SPLIT({vec_col}, "{element_separator}"))\n'
            f'    USING SEPARATOR="{field_separator}"{header_clause};\n'
            f"}}"
        )

        result = await conn.gsql(gsql_cmd)
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="load_vectors_from_csv",
                error=Exception(f"Failed to create loading job:\n{result_str}"),
                context={
                    "vertex_type": vertex_type,
                    "vector_attribute": vector_attribute,
                    "file_path": file_path,
                },
            )

        run_result = await conn.runLoadingJobWithFile(
            filePath=file_path,
            fileTag=file_tag,
            jobName=job_name,
            sep=field_separator,
        )

        try:
            await conn.gsql(f"USE GRAPH {gname}\nDROP JOB {job_name}")
        except Exception:
            pass

        return format_success(
            operation="load_vectors_from_csv",
            summary=f"Vectors loaded from CSV '{file_path}' into {vertex_type}.{vector_attribute}",
            data={
                "vertex_type": vertex_type,
                "vector_attribute": vector_attribute,
                "file_path": file_path,
                "loading_result": run_result,
            },
            suggestions=[
                f"Check index status: get_vector_index_status(vertex_type='{vertex_type}', vector_name='{vector_attribute}')",
                f"Search vectors: search_top_k_similarity(vertex_type='{vertex_type}', vector_attribute='{vector_attribute}', ...)",
                "Note: Vectors not yet indexed will not appear in search results",
            ],
        )
    except Exception as e:
        try:
            await conn.gsql(f"USE GRAPH {gname}\nDROP JOB {job_name}")
        except Exception:
            pass
        return format_error(
            operation="load_vectors_from_csv",
            error=e,
            context={
                "vertex_type": vertex_type,
                "vector_attribute": vector_attribute,
                "file_path": file_path,
            },
        )


async def load_vectors_from_json(
    vertex_type: str,
    vector_attribute: str,
    file_path: str,
    id_key: str = "id",
    vector_key: str = "vector",
    element_separator: str = ",",
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Bulk-load vectors from a JSON Lines file using a GSQL loading job with JSON_FILE="true".

    The file is uploaded from the local machine to TigerGraph via the REST API.
    Each line must be a JSON object with an ID field and a vector field (comma-separated string).

    Example file::

        {"id": "vertex1", "embedding": "0.1,0.2,0.3"}
        {"id": "vertex2", "embedding": "0.4,0.5,0.6"}

    Workflow:
        1. CREATE LOADING JOB with LOAD ... TO VECTOR ATTRIBUTE ... USING JSON_FILE="true"
        2. Upload and run the job with the local file via ``runLoadingJobWithFile``
        3. DROP the job

    See: https://docs.tigergraph.com/gsql-ref/4.2/ddl-and-loading/creating-a-loading-job#_loading_json_data
    """
    from ..response_formatter import format_success, format_error, gsql_has_error

    try:
        conn = get_connection(graph_name=graph_name)
        gname = conn.graphname

        job_name = f"load_vec_json_{vector_attribute}_{vertex_type}"
        file_tag = "vec_file"

        gsql_cmd = (
            f"USE GRAPH {gname}\n"
            f"DROP JOB {job_name}\n"
            f"CREATE LOADING JOB {job_name} FOR GRAPH {gname} {{\n"
            f'  DEFINE FILENAME {file_tag};\n'
            f"  LOAD {file_tag} TO VECTOR ATTRIBUTE {vector_attribute} ON VERTEX {vertex_type}\n"
            f'    VALUES ($"{id_key}", SPLIT($"{vector_key}", "{element_separator}"))\n'
            f'    USING JSON_FILE="true";\n'
            f"}}"
        )

        result = await conn.gsql(gsql_cmd)
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="load_vectors_from_json",
                error=Exception(f"Failed to create loading job:\n{result_str}"),
                context={
                    "vertex_type": vertex_type,
                    "vector_attribute": vector_attribute,
                    "file_path": file_path,
                },
            )

        run_result = await conn.runLoadingJobWithFile(
            filePath=file_path,
            fileTag=file_tag,
            jobName=job_name,
        )

        try:
            await conn.gsql(f"USE GRAPH {gname}\nDROP JOB {job_name}")
        except Exception:
            pass

        return format_success(
            operation="load_vectors_from_json",
            summary=f"Vectors loaded from JSON '{file_path}' into {vertex_type}.{vector_attribute}",
            data={
                "vertex_type": vertex_type,
                "vector_attribute": vector_attribute,
                "file_path": file_path,
                "loading_result": run_result,
            },
            suggestions=[
                f"Check index status: get_vector_index_status(vertex_type='{vertex_type}', vector_name='{vector_attribute}')",
                f"Search vectors: search_top_k_similarity(vertex_type='{vertex_type}', vector_attribute='{vector_attribute}', ...)",
                "Note: Vectors not yet indexed will not appear in search results",
            ],
        )
    except Exception as e:
        try:
            await conn.gsql(f"USE GRAPH {gname}\nDROP JOB {job_name}")
        except Exception:
            pass
        return format_error(
            operation="load_vectors_from_json",
            error=e,
            context={
                "vertex_type": vertex_type,
                "vector_attribute": vector_attribute,
                "file_path": file_path,
            },
        )
