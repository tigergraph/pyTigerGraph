from pyTigerGraph.pyTigerGraph import TigerGraphConnection
from pyTigerGraph.pytgasync.pyTigerGraph import AsyncTigerGraphConnection
from pyTigerGraph.common.exception import TigerGraphException

__version__ = "2.0.0"

__license__ = "Apache 2"

# Optional MCP support
try:
    from pyTigerGraph.mcp import serve, MCPServer, get_connection, ConnectionManager
    __all__ = [
        "TigerGraphConnection",
        "AsyncTigerGraphConnection",
        "TigerGraphException",
        "serve",
        "MCPServer",
        "get_connection",
        "ConnectionManager",
    ]
except ImportError:
    # MCP dependencies not installed
    __all__ = [
        "TigerGraphConnection",
        "AsyncTigerGraphConnection",
        "TigerGraphException",
    ]
