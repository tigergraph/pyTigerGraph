# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Connection profile tools for MCP.

Allows agents to list available connection profiles and inspect
non-sensitive connection details for a given profile.
"""

from typing import List, Optional
from pydantic import BaseModel, Field
from mcp.types import Tool, TextContent

from ..tool_names import TigerGraphToolName
from ..connection_manager import ConnectionManager
from ..response_formatter import format_success, format_error


class ListConnectionsToolInput(BaseModel):
    """Input schema for listing available connection profiles."""


class ShowConnectionToolInput(BaseModel):
    """Input schema for showing connection details."""
    profile: Optional[str] = Field(
        None,
        description=(
            "Connection profile name to inspect. "
            "If not provided, shows the active profile (from TG_PROFILE env var or 'default')."
        ),
    )


list_connections_tool = Tool(
    name=TigerGraphToolName.LIST_CONNECTIONS,
    description=(
        "List all available TigerGraph connection profiles. "
        "Profiles are configured via environment variables: "
        "the default profile uses TG_HOST, TG_USERNAME, etc., "
        "while named profiles use <PROFILE>_TG_HOST, <PROFILE>_TG_USERNAME, etc."
    ),
    inputSchema=ListConnectionsToolInput.model_json_schema(),
)

show_connection_tool = Tool(
    name=TigerGraphToolName.SHOW_CONNECTION,
    description=(
        "Show non-sensitive connection details for a specific profile "
        "(host, username, graph name, ports). Never reveals passwords or tokens."
    ),
    inputSchema=ShowConnectionToolInput.model_json_schema(),
)


async def list_connections() -> List[TextContent]:
    """List all available connection profiles."""
    try:
        profiles = ConnectionManager.list_profiles()
        profile_details = []
        for p in profiles:
            info = ConnectionManager.get_profile_info(p)
            profile_details.append(info)

        return format_success(
            operation="list_connections",
            summary=f"Found {len(profiles)} connection profile(s): {', '.join(profiles)}",
            data={"profiles": profile_details, "count": len(profiles)},
            suggestions=[
                "Show details: show_connection(profile='<name>')",
                "Use a profile: pass profile='<name>' to any tool",
            ],
        )
    except Exception as e:
        return format_error(
            operation="list_connections",
            error=str(e),
        )


async def show_connection(profile: Optional[str] = None) -> List[TextContent]:
    """Show non-sensitive connection details for a profile."""
    try:
        import os
        effective = profile or os.getenv("TG_PROFILE", "default")
        info = ConnectionManager.get_profile_info(effective)

        return format_success(
            operation="show_connection",
            summary=f"Connection profile '{effective}': {info['host']}",
            data=info,
            suggestions=[
                "List all profiles: list_connections()",
                f"Use this profile: pass profile='{effective}' to any tool",
            ],
        )
    except Exception as e:
        return format_error(
            operation="show_connection",
            error=str(e),
        )
