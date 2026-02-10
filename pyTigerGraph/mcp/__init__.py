# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Model Context Protocol (MCP) support for TigerGraph.

This module provides MCP server capabilities for TigerGraph, allowing
AI agents to interact with TigerGraph through the Model Context Protocol.
"""

from .server import serve, MCPServer
from .connection_manager import get_connection, ConnectionManager

__all__ = [
    "serve",
    "MCPServer",
    "get_connection",
    "ConnectionManager",
]

