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
    from ..response_formatter import format_success, format_error, gsql_has_error

    try:
        conn = get_connection()

        config_str = ", ".join([f'{k}="{v}"' for k, v in config.items()])

        gsql_cmd = f"CREATE DATA_SOURCE {data_source_type.upper()} {data_source_name}"
        if config_str:
            gsql_cmd += f" = ({config_str})"

        result = await conn.gsql(gsql_cmd)
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="create_data_source",
                error=Exception(f"Could not create data source:\n{result_str}"),
                context={"data_source_name": data_source_name, "data_source_type": data_source_type},
            )

        return format_success(
            operation="create_data_source",
            summary=f"Data source '{data_source_name}' of type '{data_source_type}' created successfully",
            data={"data_source_name": data_source_name, "result": result_str},
            suggestions=[
                f"View data source: get_data_source(data_source_name='{data_source_name}')",
                "List all data sources: get_all_data_sources()",
            ],
        )
    except Exception as e:
        return format_error(
            operation="create_data_source",
            error=e,
            context={"data_source_name": data_source_name},
        )


async def update_data_source(
    data_source_name: str,
    config: Dict[str, Any],
) -> List[TextContent]:
    """Update an existing data source."""
    from ..response_formatter import format_success, format_error, gsql_has_error

    try:
        conn = get_connection()

        config_str = ", ".join([f'{k}="{v}"' for k, v in config.items()])
        gsql_cmd = f"ALTER DATA_SOURCE {data_source_name} = ({config_str})"

        result = await conn.gsql(gsql_cmd)
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="update_data_source",
                error=Exception(f"Could not update data source:\n{result_str}"),
                context={"data_source_name": data_source_name},
            )

        return format_success(
            operation="update_data_source",
            summary=f"Data source '{data_source_name}' updated successfully",
            data={"data_source_name": data_source_name, "result": result_str},
        )
    except Exception as e:
        return format_error(
            operation="update_data_source",
            error=e,
            context={"data_source_name": data_source_name},
        )


async def get_data_source(
    data_source_name: str,
) -> List[TextContent]:
    """Get information about a data source."""
    from ..response_formatter import format_success, format_error, gsql_has_error

    try:
        conn = get_connection()

        result = await conn.gsql(f"SHOW DATA_SOURCE {data_source_name}")
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="get_data_source",
                error=Exception(f"Could not retrieve data source:\n{result_str}"),
                context={"data_source_name": data_source_name},
            )

        return format_success(
            operation="get_data_source",
            summary=f"Data source '{data_source_name}' details",
            data={"data_source_name": data_source_name, "details": result_str},
        )
    except Exception as e:
        return format_error(
            operation="get_data_source",
            error=e,
            context={"data_source_name": data_source_name},
        )


async def drop_data_source(
    data_source_name: str,
) -> List[TextContent]:
    """Drop a data source."""
    from ..response_formatter import format_success, format_error, gsql_has_error

    try:
        conn = get_connection()

        result = await conn.gsql(f"DROP DATA_SOURCE {data_source_name}")
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="drop_data_source",
                error=Exception(f"Could not drop data source:\n{result_str}"),
                context={"data_source_name": data_source_name},
            )

        return format_success(
            operation="drop_data_source",
            summary=f"Data source '{data_source_name}' dropped successfully",
            data={"data_source_name": data_source_name, "result": result_str},
            suggestions=["List remaining: get_all_data_sources()"],
            metadata={"destructive": True},
        )
    except Exception as e:
        return format_error(
            operation="drop_data_source",
            error=e,
            context={"data_source_name": data_source_name},
        )


async def get_all_data_sources(**kwargs) -> List[TextContent]:
    """Get all data sources."""
    from ..response_formatter import format_success, format_error, gsql_has_error

    try:
        conn = get_connection()

        result = await conn.gsql("SHOW DATA_SOURCE *")
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="get_all_data_sources",
                error=Exception(f"Could not retrieve data sources:\n{result_str}"),
                context={},
            )

        return format_success(
            operation="get_all_data_sources",
            summary="All data sources retrieved",
            data={"details": result_str},
            suggestions=["Create a data source: create_data_source(...)"],
        )
    except Exception as e:
        return format_error(
            operation="get_all_data_sources",
            error=e,
            context={},
        )


async def drop_all_data_sources(
    confirm: bool = False,
) -> List[TextContent]:
    """Drop all data sources."""
    from ..response_formatter import format_success, format_error, gsql_has_error

    if not confirm:
        return format_error(
            operation="drop_all_data_sources",
            error=ValueError("Confirmation required"),
            context={},
            suggestions=[
                "Set confirm=True to proceed with this destructive operation",
                "This will drop ALL data sources",
            ],
        )

    try:
        conn = get_connection()

        result = await conn.gsql("DROP DATA_SOURCE *")
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="drop_all_data_sources",
                error=Exception(f"Could not drop all data sources:\n{result_str}"),
                context={},
            )

        return format_success(
            operation="drop_all_data_sources",
            summary="All data sources dropped successfully",
            data={"result": result_str},
            metadata={"destructive": True},
        )
    except Exception as e:
        return format_error(
            operation="drop_all_data_sources",
            error=e,
            context={},
        )


async def preview_sample_data(
    data_source_name: str,
    file_path: str,
    num_rows: int = 10,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Preview sample data from a file."""
    from ..response_formatter import format_success, format_error, gsql_has_error

    try:
        conn = get_connection(graph_name=graph_name)

        gsql_cmd = (
            f"USE GRAPH {conn.graphname}\n"
            f'SHOW DATA_SOURCE {data_source_name} FILE "{file_path}" LIMIT {num_rows}'
        )

        result = await conn.gsql(gsql_cmd)
        result_str = str(result) if result else ""

        if gsql_has_error(result_str):
            return format_error(
                operation="preview_sample_data",
                error=Exception(f"Could not preview data:\n{result_str}"),
                context={"data_source_name": data_source_name, "file_path": file_path},
            )

        return format_success(
            operation="preview_sample_data",
            summary=f"Sample data from '{file_path}' (first {num_rows} rows)",
            data={"data_source_name": data_source_name, "file_path": file_path, "preview": result_str},
            metadata={"graph_name": conn.graphname},
        )
    except Exception as e:
        return format_error(
            operation="preview_sample_data",
            error=e,
            context={"data_source_name": data_source_name, "file_path": file_path},
        )

