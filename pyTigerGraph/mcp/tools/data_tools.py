# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Data loading tools for MCP.

These tools use the non-deprecated loading job APIs:
- createLoadingJob - Create a loading job from structured config or GSQL
- runLoadingJobWithFile - Execute loading job with a file
- runLoadingJobWithData - Execute loading job with data string
- getLoadingJobs - List all loading jobs
- getLoadingJobStatus - Get status of a loading job
- dropLoadingJob - Drop a loading job
"""

import json
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from mcp.types import Tool, TextContent

from ..tool_names import TigerGraphToolName
from ..connection_manager import get_connection
from ..response_formatter import format_success, format_error, gsql_has_error
from pyTigerGraph.common.exception import TigerGraphException


# =============================================================================
# Input Models for Loading Job Configuration
# =============================================================================

class NodeMapping(BaseModel):
    """Mapping configuration for loading vertices."""
    vertex_type: str = Field(..., description="Target vertex type name.")
    attribute_mappings: Dict[str, Union[str, int]] = Field(
        ..., 
        description="Map of attribute name to column index (int) or header name (string). Must include the primary key. Example: {'id': 0, 'name': 1} or {'id': 'user_id', 'name': 'user_name'}"
    )


class EdgeMapping(BaseModel):
    """Mapping configuration for loading edges."""
    edge_type: str = Field(..., description="Target edge type name.")
    source_column: Union[str, int] = Field(..., description="Column for source vertex ID (string for header name, int for column index).")
    target_column: Union[str, int] = Field(..., description="Column for target vertex ID (string for header name, int for column index).")
    attribute_mappings: Optional[Dict[str, Union[str, int]]] = Field(
        default_factory=dict,
        description="Map of attribute name to column. Optional for edges without attributes."
    )


class FileConfig(BaseModel):
    """Configuration for a single data file in a loading job."""
    file_alias: str = Field(..., description="Alias for the file (used in DEFINE FILENAME).")
    file_path: Optional[str] = Field(None, description="Path to the file. If not provided, data will be passed at runtime.")
    separator: str = Field(",", description="Field separator character.")
    header: str = Field("true", description="Whether the file has a header row ('true' or 'false').")
    eol: str = Field("\\n", description="End-of-line character.")
    quote: Optional[str] = Field(None, description="Quote character for CSV (e.g., 'DOUBLE' for double quotes).")
    node_mappings: List[NodeMapping] = Field(
        default_factory=list, 
        description="List of vertex loading mappings. Example: [{'vertex_type': 'Person', 'attribute_mappings': {'id': 0, 'name': 1}}]"
    )
    edge_mappings: List[EdgeMapping] = Field(
        default_factory=list, 
        description="List of edge loading mappings."
    )


class CreateLoadingJobToolInput(BaseModel):
    """Input schema for creating a loading job."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    job_name: str = Field(..., description="Name for the loading job.")
    files: List[FileConfig] = Field(
        ..., 
        description="List of file configurations. Each file must have a 'file_alias' and 'node_mappings' and/or 'edge_mappings'. Example: [{'file_alias': 'f1', 'node_mappings': [...]}]"
    )
    run_job: bool = Field(False, description="If True, run the loading job immediately after creation.")
    drop_after_run: bool = Field(False, description="If True, drop the job after running (only applies if run_job=True).")


# =============================================================================
# Input Models for Other Operations
# =============================================================================

class RunLoadingJobWithFileToolInput(BaseModel):
    """Input schema for running a loading job with a file."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    file_path: str = Field(..., description="Absolute path to the data file to load. Example: '/home/user/data/persons.csv'")
    file_tag: str = Field(..., description="The name of file variable in the loading job (DEFINE FILENAME <fileTag>).")
    job_name: str = Field(..., description="The name of the loading job to run.")
    separator: Optional[str] = Field(None, description="Data value separator. Default is comma. For JSON data, don't specify.")
    eol: Optional[str] = Field(None, description="End-of-line character. Default is '\\n'. Supports '\\r\\n'.")
    timeout: int = Field(16000, description="Timeout in milliseconds. Set to 0 for system-wide timeout.")
    size_limit: int = Field(128000000, description="Maximum size for input file in bytes (default 128MB).")


class RunLoadingJobWithDataToolInput(BaseModel):
    """Input schema for running a loading job with inline data."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    data: str = Field(..., description="The data string to load (CSV, JSON, etc.). Example: 'user1,Alice\\nuser2,Bob'")
    file_tag: str = Field(..., description="The name of file variable in the loading job (DEFINE FILENAME <fileTag>).")
    job_name: str = Field(..., description="The name of the loading job to run.")
    separator: Optional[str] = Field(None, description="Data value separator. Default is comma. For JSON data, don't specify.")
    eol: Optional[str] = Field(None, description="End-of-line character. Default is '\\n'. Supports '\\r\\n'.")
    timeout: int = Field(16000, description="Timeout in milliseconds. Set to 0 for system-wide timeout.")
    size_limit: int = Field(128000000, description="Maximum size for input data in bytes (default 128MB).")


class GetLoadingJobsToolInput(BaseModel):
    """Input schema for listing loading jobs."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")


class GetLoadingJobStatusToolInput(BaseModel):
    """Input schema for getting loading job status."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    job_id: str = Field(..., description="The ID of the loading job to check status.")


class DropLoadingJobToolInput(BaseModel):
    """Input schema for dropping a loading job."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    job_name: str = Field(..., description="The name of the loading job to drop.")


# =============================================================================
# Tool Definitions
# =============================================================================

create_loading_job_tool = Tool(
    name=TigerGraphToolName.CREATE_LOADING_JOB,
    description="""Create a loading job from structured configuration. 
The job defines how to load data from files into vertices and edges.
Each file config specifies: file alias, separator, header, EOL, and mappings.
Node mappings define which columns map to vertex attributes.
Edge mappings define source/target columns and edge attributes.
Optionally run the job immediately and drop it after execution.""",
    inputSchema=CreateLoadingJobToolInput.model_json_schema(),
)

run_loading_job_with_file_tool = Tool(
    name=TigerGraphToolName.RUN_LOADING_JOB_WITH_FILE,
    description="Execute a loading job with a data file. The file is uploaded to TigerGraph and loaded according to the specified loading job definition.",
    inputSchema=RunLoadingJobWithFileToolInput.model_json_schema(),
)

run_loading_job_with_data_tool = Tool(
    name=TigerGraphToolName.RUN_LOADING_JOB_WITH_DATA,
    description="Execute a loading job with inline data string. The data is posted to TigerGraph and loaded according to the specified loading job definition.",
    inputSchema=RunLoadingJobWithDataToolInput.model_json_schema(),
)

get_loading_jobs_tool = Tool(
    name=TigerGraphToolName.GET_LOADING_JOBS,
    description="Get a list of all loading jobs defined for the current graph.",
    inputSchema=GetLoadingJobsToolInput.model_json_schema(),
)

get_loading_job_status_tool = Tool(
    name=TigerGraphToolName.GET_LOADING_JOB_STATUS,
    description="Get the status of a specific loading job by its job ID.",
    inputSchema=GetLoadingJobStatusToolInput.model_json_schema(),
)

drop_loading_job_tool = Tool(
    name=TigerGraphToolName.DROP_LOADING_JOB,
    description="Drop (delete) a loading job from the graph.",
    inputSchema=DropLoadingJobToolInput.model_json_schema(),
)


# =============================================================================
# Helper Functions
# =============================================================================

def _format_column(column: Union[str, int]) -> str:
    """Format column reference for GSQL loading job."""
    if isinstance(column, int):
        return f"${column}"
    return f'$"{column}"'


def _generate_loading_job_gsql(
    graph_name: str,
    job_name: str,
    files: List[Dict[str, Any]],
) -> str:
    """Generate GSQL script for creating a loading job."""

    # Build DEFINE FILENAME statements
    define_files = []
    for file_config in files:
        alias = file_config["file_alias"]
        path = file_config.get("file_path")
        if path:
            define_files.append(f'DEFINE FILENAME {alias} = "{path}";')
        else:
            define_files.append(f"DEFINE FILENAME {alias};")

    # Build LOAD statements for each file
    load_statements = []
    for file_config in files:
        alias = file_config["file_alias"]
        separator = file_config.get("separator", ",")
        header = file_config.get("header", "true")
        eol = file_config.get("eol", "\\n")
        quote = file_config.get("quote")

        # Build USING clause
        using_parts = [
            f'SEPARATOR="{separator}"',
            f'HEADER="{header}"',
            f'EOL="{eol}"'
        ]
        if quote:
            using_parts.append(f'QUOTE="{quote}"')
        using_clause = "USING " + ", ".join(using_parts) + ";"

        # Build mapping statements
        mapping_statements = []

        # Node mappings
        for node_mapping in file_config.get("node_mappings", []):
            vertex_type = node_mapping["vertex_type"]
            attr_mappings = node_mapping["attribute_mappings"]

            # Format attribute values
            attr_values = ", ".join(
                _format_column(col) for col in attr_mappings.values()
            )
            mapping_statements.append(
                f"TO VERTEX {vertex_type} VALUES({attr_values})"
            )

        # Edge mappings
        for edge_mapping in file_config.get("edge_mappings", []):
            edge_type = edge_mapping["edge_type"]
            source_col = _format_column(edge_mapping["source_column"])
            target_col = _format_column(edge_mapping["target_column"])
            attr_mappings = edge_mapping.get("attribute_mappings", {})

            # Format attribute values
            if attr_mappings:
                attr_values = ", ".join(
                    _format_column(col) for col in attr_mappings.values()
                )
                all_values = f"{source_col}, {target_col}, {attr_values}"
            else:
                all_values = f"{source_col}, {target_col}"

            mapping_statements.append(
                f"TO EDGE {edge_type} VALUES({all_values})"
            )

        # Combine into LOAD statement
        if mapping_statements:
            load_stmt = f"LOAD {alias}\n    " + ",\n    ".join(mapping_statements) + f"\n    {using_clause}"
            load_statements.append(load_stmt)

    # Build the complete GSQL script
    define_section = "  # Define files\n  " + "\n  ".join(define_files)
    load_section = "  # Load data\n  " + "\n  ".join(load_statements)

    gsql_script = f"""USE GRAPH {graph_name}

CREATE LOADING JOB {job_name} FOR GRAPH {graph_name} {{
{define_section}

{load_section}
}}"""

    return gsql_script


# =============================================================================
# Tool Implementations
# =============================================================================

async def create_loading_job(
    job_name: str,
    files: List[Dict[str, Any]],
    run_job: bool = False,
    drop_after_run: bool = False,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Create a loading job from structured configuration."""
    try:
        conn = get_connection(graph_name=graph_name)

        # Generate the GSQL script
        gsql_script = _generate_loading_job_gsql(
            graph_name=conn.graphname,
            job_name=job_name,
            files=files
        )

        # Add RUN and DROP commands if requested
        if run_job:
            gsql_script += f"\n\nRUN LOADING JOB {job_name}"
            if drop_after_run:
                gsql_script += f"\n\nDROP JOB {job_name}"

        result = await conn.gsql(gsql_script)
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="create_loading_job",
                error=TigerGraphException(result_str),
                context={
                    "job_name": job_name,
                    "graph_name": conn.graphname,
                    "gsql_script": gsql_script,
                },
                suggestions=[
                    "Check that vertex/edge types referenced in the job exist in the schema",
                    "Use show_graph_details() to verify the current schema",
                    "Ensure file paths and column mappings are correct",
                ],
            )

        status_parts = []
        if run_job:
            if drop_after_run:
                status_parts.append("Job created, executed, and dropped (one-time load)")
            else:
                status_parts.append("Job created and executed")
        else:
            status_parts.append("Job created successfully")

        return format_success(
            operation="create_loading_job",
            summary=f"Success: Loading job '{job_name}' " + ", ".join(status_parts),
            data={
                "job_name": job_name,
                "file_count": len(files),
                "executed": run_job,
                "dropped": drop_after_run,
                "gsql_script": gsql_script,
                "result": result_str,
            },
            suggestions=[s for s in [
                f"Run the job: run_loading_job_with_file(job_name='{job_name}', ...)" if not run_job else "Job already executed",
                "List all jobs: get_loading_jobs()",
                f"Get status: get_loading_job_status(job_name='{job_name}')" if not drop_after_run else None,
                "Tip: Loading jobs are the recommended way to bulk-load data"
            ] if s is not None],
            metadata={
                "graph_name": conn.graphname,
                "operation_type": "DDL"
            }
        )

    except Exception as e:
        return format_error(
            operation="create_loading_job",
            error=e,
            context={
                "job_name": job_name,
                "file_count": len(files),
                "graph_name": graph_name or "default"
            }
        )


async def run_loading_job_with_file(
    file_path: str,
    file_tag: str,
    job_name: str,
    separator: Optional[str] = None,
    eol: Optional[str] = None,
    timeout: int = 16000,
    size_limit: int = 128000000,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Execute a loading job with a data file."""
    try:
        conn = get_connection(graph_name=graph_name)
        result = await conn.runLoadingJobWithFile(
            filePath=file_path,
            fileTag=file_tag,
            jobName=job_name,
            sep=separator,
            eol=eol,
            timeout=timeout,
            sizeLimit=size_limit
        )
        if result:
            return format_success(
                operation="run_loading_job_with_file",
                summary=f"Success: Loading job '{job_name}' executed successfully with file '{file_path}'",
                data={
                    "job_name": job_name,
                    "file_path": file_path,
                    "file_tag": file_tag,
                    "result": result
                },
                suggestions=[
                    f"Check status: get_loading_job_status(job_id='<job_id>')",
                    "Verify loaded data with: get_vertex_count() or get_edge_count()",
                    "List all jobs: get_loading_jobs()"
                ],
                metadata={"graph_name": conn.graphname}
            )
        else:
            return format_error(
                operation="run_loading_job_with_file",
                error=ValueError("Loading job returned no result"),
                context={
                    "job_name": job_name,
                    "file_path": file_path,
                    "file_tag": file_tag,
                    "graph_name": graph_name or "default"
                },
                suggestions=[
                    "Check if the job name is correct",
                    "Verify the file_tag matches the loading job definition",
                    "Ensure the loading job exists: get_loading_jobs()"
                ]
            )
    except Exception as e:
        return format_error(
            operation="run_loading_job_with_file",
            error=e,
            context={
                "job_name": job_name,
                "file_path": file_path,
                "graph_name": graph_name or "default"
            }
        )


async def run_loading_job_with_data(
    data: str,
    file_tag: str,
    job_name: str,
    separator: Optional[str] = None,
    eol: Optional[str] = None,
    timeout: int = 16000,
    size_limit: int = 128000000,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Execute a loading job with inline data string."""
    try:
        conn = get_connection(graph_name=graph_name)
        result = await conn.runLoadingJobWithData(
            data=data,
            fileTag=file_tag,
            jobName=job_name,
            sep=separator,
            eol=eol,
            timeout=timeout,
            sizeLimit=size_limit
        )
        if result:
            data_preview = data[:100] + "..." if len(data) > 100 else data
            return format_success(
                operation="run_loading_job_with_data",
                summary=f"Success: Loading job '{job_name}' executed successfully with inline data",
                data={
                    "job_name": job_name,
                    "file_tag": file_tag,
                    "data_preview": data_preview,
                    "data_size": len(data),
                    "result": result
                },
                suggestions=[
                    "Verify loaded data: get_vertex_count() or get_edge_count()",
                    "Tip: For large datasets, use 'run_loading_job_with_file' instead",
                    "List all jobs: get_loading_jobs()"
                ],
                metadata={"graph_name": conn.graphname}
            )
        else:
            return format_error(
                operation="run_loading_job_with_data",
                error=ValueError("Loading job returned no result"),
                context={
                    "job_name": job_name,
                    "file_tag": file_tag,
                    "data_size": len(data),
                    "graph_name": graph_name or "default"
                },
                suggestions=[
                    "Check if the job name is correct",
                    "Verify the file_tag matches the loading job definition",
                    "Ensure the loading job exists: get_loading_jobs()"
                ]
            )
    except Exception as e:
        return format_error(
            operation="run_loading_job_with_data",
            error=e,
            context={
                "job_name": job_name,
                "data_size": len(data),
                "graph_name": graph_name or "default"
            }
        )


async def get_loading_jobs(
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Get a list of all loading jobs for the current graph."""
    try:
        conn = get_connection(graph_name=graph_name)
        result = await conn.getLoadingJobs()
        if result:
            job_count = len(result) if isinstance(result, list) else 1
            return format_success(
                operation="get_loading_jobs",
                summary=f"Found {job_count} loading job(s) for graph '{conn.graphname}'",
                data={
                    "jobs": result,
                    "count": job_count
                },
                suggestions=[
                    "Run a job: run_loading_job_with_file(...) or run_loading_job_with_data(...)",
                    "Create new job: create_loading_job(...)",
                    "Check job status: get_loading_job_status(job_id='<job_id>')"
                ],
                metadata={"graph_name": conn.graphname}
            )
        else:
            return format_success(
                operation="get_loading_jobs",
                summary=f"Success: No loading jobs found for graph '{conn.graphname}'",
                suggestions=[
                    "Create a loading job: create_loading_job(...)",
                    "Tip: Loading jobs are used for bulk data ingestion"
                ],
                metadata={"graph_name": conn.graphname}
            )
    except Exception as e:
        return format_error(
            operation="get_loading_jobs",
            error=e,
            context={"graph_name": graph_name or "default"}
        )


async def get_loading_job_status(
    job_id: str,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Get the status of a specific loading job."""
    try:
        conn = get_connection(graph_name=graph_name)
        result = await conn.getLoadingJobStatus(jobId=job_id)
        if result:
            return format_success(
                operation="get_loading_job_status",
                summary=f"Success: Loading job status for '{job_id}'",
                data={
                    "job_id": job_id,
                    "status": result
                },
                suggestions=[
                    "List all jobs: get_loading_jobs()",
                    "Tip: Use this to monitor long-running loading jobs"
                ],
                metadata={"graph_name": conn.graphname}
            )
        else:
            return format_error(
                operation="get_loading_job_status",
                error=ValueError("No status found for loading job"),
                context={
                    "job_id": job_id,
                    "graph_name": graph_name or "default"
                },
                suggestions=[
                    "Verify the job_id is correct",
                    "List all jobs: get_loading_jobs()"
                ]
            )
    except Exception as e:
        return format_error(
            operation="get_loading_job_status",
            error=e,
            context={
                "job_id": job_id,
                "graph_name": graph_name or "default"
            }
        )


async def drop_loading_job(
    job_name: str,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Drop a loading job from the graph."""
    try:
        conn = get_connection(graph_name=graph_name)
        result = await conn.dropLoadingJob(jobName=job_name)
        
        return format_success(
            operation="drop_loading_job",
            summary=f"Success: Loading job '{job_name}' dropped successfully",
            data={
                "job_name": job_name,
                "result": result
            },
            suggestions=[
                "Warning: This operation is permanent and cannot be undone",
                "Verify deletion: get_loading_jobs()",
                "Create a new job: create_loading_job(...)"
            ],
            metadata={
                "graph_name": conn.graphname,
                "destructive": True
            }
        )
    except Exception as e:
        return format_error(
            operation="drop_loading_job",
            error=e,
            context={
                "job_name": job_name,
                "graph_name": graph_name or "default"
            }
        )
