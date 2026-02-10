# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Connection manager for MCP server.

Manages AsyncTigerGraphConnection instances for MCP tools.
"""

import os
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from pyTigerGraph import AsyncTigerGraphConnection
from pyTigerGraph.common.exception import TigerGraphException

logger = logging.getLogger(__name__)

# Try to load dotenv if available
try:
    from dotenv import load_dotenv
    _dotenv_available = True
except ImportError:
    _dotenv_available = False


def _load_env_file(env_path: Optional[str] = None) -> None:
    """Load environment variables from .env file if available.

    Args:
        env_path: Optional path to .env file. If not provided, looks for .env in current directory.
    """
    if not _dotenv_available:
        return

    if env_path:
        env_file = Path(env_path).expanduser().resolve()
    else:
        # Look for .env in current directory and parent directories
        current_dir = Path.cwd()
        env_file = None
        for directory in [current_dir] + list(current_dir.parents):
            potential_env = directory / ".env"
            if potential_env.exists():
                env_file = potential_env
                break

        if env_file is None:
            # Also check in the directory where the script is running
            env_file = Path(".env")

    if env_file and env_file.exists():
        load_dotenv(env_file, override=False)  # Don't override existing env vars
        logger.debug(f"Loaded environment variables from {env_file}")
    elif env_path:
        logger.warning(f"Specified .env file not found: {env_path}")


class ConnectionManager:
    """Manages TigerGraph connections for MCP tools."""

    _default_connection: Optional[AsyncTigerGraphConnection] = None

    @classmethod
    def get_default_connection(cls) -> Optional[AsyncTigerGraphConnection]:
        """Get the default connection instance."""
        return cls._default_connection

    @classmethod
    def set_default_connection(cls, conn: AsyncTigerGraphConnection) -> None:
        """Set the default connection instance."""
        cls._default_connection = conn

    @classmethod
    def create_connection_from_env(cls, env_path: Optional[str] = None) -> AsyncTigerGraphConnection:
        """Create a connection from environment variables.

        Automatically loads variables from a .env file if it exists (requires python-dotenv).
        Environment variables take precedence over .env file values.

        Reads the following environment variables:
        - TG_HOST: TigerGraph host (default: http://127.0.0.1)
        - TG_GRAPHNAME: Graph name (optional - can be set later or use list_graphs tool)
        - TG_USERNAME: Username (default: tigergraph)
        - TG_PASSWORD: Password (default: tigergraph)
        - TG_SECRET: GSQL secret (optional)
        - TG_API_TOKEN: API token (optional)
        - TG_JWT_TOKEN: JWT token (optional)
        - TG_RESTPP_PORT: REST++ port (default: 9000)
        - TG_GS_PORT: GSQL port (default: 14240)
        - TG_SSL_PORT: SSL port (default: 443)
        - TG_TGCLOUD: Whether using TigerGraph Cloud (default: False)
        - TG_CERT_PATH: Path to certificate (optional)

        Args:
            env_path: Optional path to .env file. If not provided, searches for .env in current and parent directories.
        """
        # Load .env file if available
        _load_env_file(env_path)

        host = os.getenv("TG_HOST", "http://127.0.0.1")
        graphname = os.getenv("TG_GRAPHNAME", "")  # Optional - can be empty
        username = os.getenv("TG_USERNAME", "tigergraph")
        password = os.getenv("TG_PASSWORD", "tigergraph")
        gsql_secret = os.getenv("TG_SECRET", "")
        api_token = os.getenv("TG_API_TOKEN", "")
        jwt_token = os.getenv("TG_JWT_TOKEN", "")
        restpp_port = os.getenv("TG_RESTPP_PORT", "9000")
        gs_port = os.getenv("TG_GS_PORT", "14240")
        ssl_port = os.getenv("TG_SSL_PORT", "443")
        tg_cloud = os.getenv("TG_TGCLOUD", "false").lower() == "true"
        cert_path = os.getenv("TG_CERT_PATH", None)

        # TG_GRAPHNAME is now optional - can be set later or use list_graphs tool

        conn = AsyncTigerGraphConnection(
            host=host,
            graphname=graphname,
            username=username,
            password=password,
            gsqlSecret=gsql_secret if gsql_secret else "",
            apiToken=api_token if api_token else "",
            jwtToken=jwt_token if jwt_token else "",
            restppPort=restpp_port,
            gsPort=gs_port,
            sslPort=ssl_port,
            tgCloud=tg_cloud,
            certPath=cert_path,
        )

        cls._default_connection = conn
        return conn


def get_connection(
    graph_name: Optional[str] = None,
    connection_config: Optional[Dict[str, Any]] = None,
) -> AsyncTigerGraphConnection:
    """Get or create an async TigerGraph connection.

    Args:
        graph_name: Name of the graph. If provided, will create a new connection.
        connection_config: Connection configuration dict. If provided, will create a new connection.

    Returns:
        AsyncTigerGraphConnection instance.
    """
    # If connection config is provided, create a new connection
    if connection_config:
        return AsyncTigerGraphConnection(
            host=connection_config.get("host", "http://127.0.0.1"),
            graphname=connection_config.get("graphname", graph_name or ""),
            username=connection_config.get("username", "tigergraph"),
            password=connection_config.get("password", "tigergraph"),
            gsqlSecret=connection_config.get("gsqlSecret", ""),
            apiToken=connection_config.get("apiToken", ""),
            jwtToken=connection_config.get("jwtToken", ""),
            restppPort=connection_config.get("restppPort", "9000"),
            gsPort=connection_config.get("gsPort", "14240"),
            sslPort=connection_config.get("sslPort", "443"),
            tgCloud=connection_config.get("tgCloud", False),
            certPath=connection_config.get("certPath", None),
        )

    # If graph_name is provided, try to get/create connection for that graph
    if graph_name:
        # For now, use default connection but set graphname
        conn = ConnectionManager.get_default_connection()
        if conn is None:
            conn = ConnectionManager.create_connection_from_env()
        # Update graphname if different
        if conn.graphname != graph_name:
            conn.graphname = graph_name
        return conn

    # Return default connection or create from env
    conn = ConnectionManager.get_default_connection()
    if conn is None:
        conn = ConnectionManager.create_connection_from_env()
    return conn

