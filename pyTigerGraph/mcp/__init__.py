# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0

"""Deprecated MCP shim — the MCP server has moved to the `tigergraph-mcp` package.

Install the standalone package::

    pip install tigergraph-mcp

Or continue using the convenience alias (which installs `tigergraph-mcp` automatically)::

    pip install pyTigerGraph[mcp]

Update your imports::

    # Old
    from pyTigerGraph.mcp import serve, MCPServer, ConnectionManager

    # New
    from tigergraph_mcp import serve, MCPServer, ConnectionManager
"""

import warnings

warnings.warn(
    "pyTigerGraph.mcp is deprecated and will be removed in a future release. "
    "The MCP server now lives in the 'tigergraph-mcp' package. "
    "Install it with: pip install tigergraph-mcp  "
    "Update imports from 'pyTigerGraph.mcp' to 'tigergraph_mcp'.",
    DeprecationWarning,
    stacklevel=2,
)

try:
    from tigergraph_mcp import serve, MCPServer, get_connection, ConnectionManager  # noqa: F401
except ImportError as e:
    raise ImportError(
        "Could not import 'tigergraph_mcp'. "
        "Install it with: pip install tigergraph-mcp"
    ) from e

__all__ = [
    "serve",
    "MCPServer",
    "get_connection",
    "ConnectionManager",
]
