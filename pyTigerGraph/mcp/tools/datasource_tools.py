# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Data source operation tools for MCP."""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from mcp.types import Tool, TextContent

from ..tool_names import TigerGraphToolName
from ..connection_manager import get_connection


class CreateDataSourceToolInput(BaseModel):
    """Input schema for creating a data source."""
    data_source_name: str = Field(..., description="Name of the data source.")
    data_source_type: str = Field(..., description="Type of data source: 's3', 'gcs', 'azure_blob', or 'local'.")
    config: Dict[str, Any] = Field(..., description="Configuration for the data source (e.g., bucket, credentials).")


class UpdateDataSourceToolInput(BaseModel):
    """Input schema for updating a data source."""
    data_source_name: str = Field(..., description="Name of the data source to update.")
    config: Dict[str, Any] = Field(..., description="Updated configuration for the data source.")


class GetDataSourceToolInput(BaseModel):
    """Input schema for getting a data source."""
    data_source_name: str = Field(..., description="Name of the data source.")


class DropDataSourceToolInput(BaseModel):
    """Input schema for dropping a data source."""
    data_source_name: str = Field(..., description="Name of the data source to drop.")


class GetAllDataSourcesToolInput(BaseModel):
    """Input schema for getting all data sources."""
    # No parameters needed - returns all data sources


class DropAllDataSourcesToolInput(BaseModel):
    """Input schema for dropping all data sources."""
    confirm: bool = Field(False, description="Must be True to confirm dropping all data sources.")


class PreviewSampleDataToolInput(BaseModel):
    """Input schema for previewing sample data."""
    data_source_name: str = Field(..., description="Name of the data source.")
    file_path: str = Field(..., description="Path to the file within the data source.")
    num_rows: int = Field(10, description="Number of sample rows to preview.")
    graph_name: Optional[str] = Field(None, description="Name of the graph context. If not provided, uses default connection.")


create_data_source_tool = Tool(
    name=TigerGraphToolName.CREATE_DATA_SOURCE,
    description="Create a new data source for loading data (S3, GCS, Azure Blob, or local).",
    inputSchema=CreateDataSourceToolInput.model_json_schema(),
)

update_data_source_tool = Tool(
    name=TigerGraphToolName.UPDATE_DATA_SOURCE,
    description="Update an existing data source configuration.",
    inputSchema=UpdateDataSourceToolInput.model_json_schema(),
)

get_data_source_tool = Tool(
    name=TigerGraphToolName.GET_DATA_SOURCE,
    description="Get information about a specific data source.",
    inputSchema=GetDataSourceToolInput.model_json_schema(),
)

drop_data_source_tool = Tool(
    name=TigerGraphToolName.DROP_DATA_SOURCE,
    description="Drop (delete) a data source.",
    inputSchema=DropDataSourceToolInput.model_json_schema(),
)

get_all_data_sources_tool = Tool(
    name=TigerGraphToolName.GET_ALL_DATA_SOURCES,
    description="Get information about all data sources.",
    inputSchema=GetAllDataSourcesToolInput.model_json_schema(),
)

drop_all_data_sources_tool = Tool(
    name=TigerGraphToolName.DROP_ALL_DATA_SOURCES,
    description="Drop all data sources. WARNING: This is a destructive operation.",
    inputSchema=DropAllDataSourcesToolInput.model_json_schema(),
)

preview_sample_data_tool = Tool(
    name=TigerGraphToolName.PREVIEW_SAMPLE_DATA,
    description="Preview sample data from a file in a data source.",
    inputSchema=PreviewSampleDataToolInput.model_json_schema(),
)


async def create_data_source(
    data_source_name: str,
    data_source_type: str,
    config: Dict[str, Any],
) -> List[TextContent]:
    """Create a new data source."""
    try:
        conn = get_connection()

        # Build the CREATE DATA_SOURCE command based on type
        config_str = ", ".join([f'{k}="{v}"' for k, v in config.items()])

        gsql_cmd = f"CREATE DATA_SOURCE {data_source_type.upper()} {data_source_name}"
        if config_str:
            gsql_cmd += f" = ({config_str})"

        result = await conn.gsql(gsql_cmd)
        message = f"Success: Data source '{data_source_name}' of type '{data_source_type}' created successfully:\n{result}"
    except Exception as e:
        message = f"Failed to create data source due to: {str(e)}"
    return [TextContent(type="text", text=message)]


async def update_data_source(
    data_source_name: str,
    config: Dict[str, Any],
) -> List[TextContent]:
    """Update an existing data source."""
    try:
        conn = get_connection()

        config_str = ", ".join([f'{k}="{v}"' for k, v in config.items()])
        gsql_cmd = f"ALTER DATA_SOURCE {data_source_name} = ({config_str})"

        result = await conn.gsql(gsql_cmd)
        message = f"Success: Data source '{data_source_name}' updated successfully:\n{result}"
    except Exception as e:
        message = f"Failed to update data source due to: {str(e)}"
    return [TextContent(type="text", text=message)]


async def get_data_source(
    data_source_name: str,
) -> List[TextContent]:
    """Get information about a data source."""
    try:
        conn = get_connection()

        result = await conn.gsql(f"SHOW DATA_SOURCE {data_source_name}")
        message = f"Success: Data source '{data_source_name}':\n{result}"
    except Exception as e:
        message = f"Failed to get data source due to: {str(e)}"
    return [TextContent(type="text", text=message)]


async def drop_data_source(
    data_source_name: str,
) -> List[TextContent]:
    """Drop a data source."""
    try:
        conn = get_connection()

        result = await conn.gsql(f"DROP DATA_SOURCE {data_source_name}")
        message = f"Success: Data source '{data_source_name}' dropped successfully:\n{result}"
    except Exception as e:
        message = f"Failed to drop data source due to: {str(e)}"
    return [TextContent(type="text", text=message)]


async def get_all_data_sources(**kwargs) -> List[TextContent]:
    """Get all data sources."""
    try:
        conn = get_connection()

        result = await conn.gsql("SHOW DATA_SOURCE *")
        message = f"Success: All data sources:\n{result}"
    except Exception as e:
        message = f"Failed to get data sources due to: {str(e)}"
    return [TextContent(type="text", text=message)]


async def drop_all_data_sources(
    confirm: bool = False,
) -> List[TextContent]:
    """Drop all data sources."""
    if not confirm:
        return [TextContent(type="text", text="Error: Drop all data sources requires confirm=True. This is a destructive operation.")]

    try:
        conn = get_connection()

        result = await conn.gsql("DROP DATA_SOURCE *")
        message = f"Success: All data sources dropped successfully:\n{result}"
    except Exception as e:
        message = f"Failed to drop all data sources due to: {str(e)}"
    return [TextContent(type="text", text=message)]


async def preview_sample_data(
    data_source_name: str,
    file_path: str,
    num_rows: int = 10,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Preview sample data from a file."""
    try:
        conn = get_connection(graph_name=graph_name)

        # Use GSQL to preview the file
        # Note: The actual command may vary based on TigerGraph version
        gsql_cmd = f"""
        USE GRAPH {conn.graphname}
        SHOW DATA_SOURCE {data_source_name} FILE "{file_path}" LIMIT {num_rows}
        """

        result = await conn.gsql(gsql_cmd)
        message = f"Success: Sample data preview from '{file_path}' (first {num_rows} rows):\n{result}"
    except Exception as e:
        message = f"Failed to preview sample data due to: {str(e)}"
    return [TextContent(type="text", text=message)]

