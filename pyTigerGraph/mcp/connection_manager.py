# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Connection manager for MCP server.

Manages AsyncTigerGraphConnection instances for MCP tools.
Supports named connection profiles via environment variables:

  - Default profile uses unprefixed ``TG_*`` vars (backward compatible).
  - Named profiles use ``<PROFILE>_TG_*`` vars (e.g. ``STAGING_TG_HOST``).
  - ``TG_PROFILE`` selects the active profile (default: ``"default"``).
"""

import os
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
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


def _get_env_for_profile(profile: str, key: str, default: str = "") -> str:
    """Resolve a config value for a profile.

    Default profile uses unprefixed ``TG_*`` vars.
    Named profiles use ``<PROFILE>_TG_*`` vars, falling back to
    the unprefixed ``TG_*`` var, then the built-in *default*.
    """
    if profile == "default":
        return os.getenv(f"TG_{key}", default)
    return os.getenv(
        f"{profile.upper()}_TG_{key}",
        os.getenv(f"TG_{key}", default),
    )


class ConnectionManager:
    """Manages TigerGraph connections for MCP tools.

    Connections are pooled by ``profile:graph_name`` key so that
    repeated calls with the same profile reuse the same connection.

    Call ``await ConnectionManager.close_all()`` at server shutdown to release
    the persistent HTTP connection pools held by each ``AsyncTigerGraphConnection``.
    """

    _connection_pool: Dict[str, AsyncTigerGraphConnection] = {}
    _profiles: set = set()

    # Keep legacy single-connection reference for backward compat
    _default_connection: Optional[AsyncTigerGraphConnection] = None

    @classmethod
    def load_profiles(cls, env_path: Optional[str] = None) -> None:
        """Discover available profiles from environment variables.

        Profiles are detected by scanning for ``<PROFILE>_TG_HOST`` env vars.
        The ``"default"`` profile always exists and uses unprefixed ``TG_*``
        vars.  Called once at server startup.
        """
        _load_env_file(env_path)

        for key in os.environ:
            if key.endswith("_TG_HOST") and not key.startswith("TG_"):
                profile = key.rsplit("_TG_HOST", 1)[0].lower()
                cls._profiles.add(profile)

        cls._profiles.add("default")
        logger.info(f"Discovered connection profiles: {sorted(cls._profiles)}")

    @classmethod
    def list_profiles(cls) -> List[str]:
        """Return sorted list of discovered profile names."""
        if not cls._profiles:
            cls._profiles.add("default")
        return sorted(cls._profiles)

    @classmethod
    def get_default_connection(cls) -> Optional[AsyncTigerGraphConnection]:
        """Get the default connection instance (backward compat)."""
        return cls._default_connection

    @classmethod
    def set_default_connection(cls, conn: AsyncTigerGraphConnection) -> None:
        """Set the default connection instance (backward compat)."""
        cls._default_connection = conn

    @classmethod
    async def close_all(cls) -> None:
        """Close all pooled connections and release their HTTP sockets.

        Call this at server/application shutdown to drain keep-alive connections
        gracefully. Connections are removed from the pool after closing so that
        subsequent calls to get_connection_for_profile() create fresh sessions.

        Example:
            ```python
            # In an MCP server lifespan or FastAPI shutdown event:
            await ConnectionManager.close_all()
            ```
        """
        for conn in list(cls._connection_pool.values()):
            await conn.aclose()
        cls._connection_pool.clear()
        cls._profiles.clear()
        cls._default_connection = None

    @classmethod
    def get_connection_for_profile(
        cls,
        profile: str = "default",
        graph_name: Optional[str] = None,
    ) -> AsyncTigerGraphConnection:
        """Get or create a connection for the given profile and optional graph.

        Connections are cached by ``profile`` (or ``profile:graph_name`` when
        a graph_name override is given).  If a cached connection exists but the
        caller passes a different ``graph_name``, the graphname attribute on
        the cached connection is updated in place.
        """
        cache_key = profile

        if cache_key in cls._connection_pool:
            conn = cls._connection_pool[cache_key]
            if graph_name and conn.graphname != graph_name:
                conn.graphname = graph_name
            return conn

        host = _get_env_for_profile(profile, "HOST", "http://127.0.0.1")
        graphname = graph_name or _get_env_for_profile(profile, "GRAPHNAME", "")
        username = _get_env_for_profile(profile, "USERNAME", "tigergraph")
        password = _get_env_for_profile(profile, "PASSWORD", "tigergraph")
        gsql_secret = _get_env_for_profile(profile, "SECRET", "")
        api_token = _get_env_for_profile(profile, "API_TOKEN", "")
        jwt_token = _get_env_for_profile(profile, "JWT_TOKEN", "")
        restpp_port = _get_env_for_profile(profile, "RESTPP_PORT", "9000")
        gs_port = _get_env_for_profile(profile, "GS_PORT", "14240")
        ssl_port = _get_env_for_profile(profile, "SSL_PORT", "443")
        tg_cloud = _get_env_for_profile(profile, "TGCLOUD", "false").lower() == "true"
        cert_path = _get_env_for_profile(profile, "CERT_PATH", "") or None
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

        cls._connection_pool[cache_key] = conn

        if profile == "default":
            cls._default_connection = conn

        logger.info(f"Created connection for profile '{profile}' -> {host}")
        return conn

    @classmethod
    def get_profile_info(cls, profile: str = "default") -> Dict[str, str]:
        """Return non-sensitive connection info for a profile.

        Never includes password, secret, or tokens.
        """
        return {
            "profile": profile,
            "host": _get_env_for_profile(profile, "HOST", "http://127.0.0.1"),
            "graphname": _get_env_for_profile(profile, "GRAPHNAME", ""),
            "username": _get_env_for_profile(profile, "USERNAME", "tigergraph"),
            "restpp_port": _get_env_for_profile(profile, "RESTPP_PORT", "9000"),
            "gs_port": _get_env_for_profile(profile, "GS_PORT", "14240"),
            "tgcloud": _get_env_for_profile(profile, "TGCLOUD", "false"),
        }

    @classmethod
    def create_connection_from_env(cls, env_path: Optional[str] = None) -> AsyncTigerGraphConnection:
        """Create a connection from environment variables (backward compat).

        Equivalent to ``get_connection_for_profile("default")``.
        """
        _load_env_file(env_path)
        return cls.get_connection_for_profile("default")

    @classmethod
    async def close_all(cls) -> None:
        """Close all pooled connections and release their HTTP connection pools.

        Call at server shutdown to cleanly drain open sockets held by the
        persistent ``aiohttp.ClientSession`` inside each connection.
        """
        for key, conn in list(cls._connection_pool.items()):
            try:
                await conn.aclose()
                logger.debug(f"Closed connection for profile '{key}'")
            except Exception as e:
                logger.warning(f"Error closing connection '{key}': {e}")
        cls._connection_pool.clear()
        cls._default_connection = None


def get_connection(
    profile: Optional[str] = None,
    graph_name: Optional[str] = None,
    connection_config: Optional[Dict[str, Any]] = None,
) -> AsyncTigerGraphConnection:
    """Get or create an async TigerGraph connection.

    Args:
        profile: Connection profile name. Falls back to ``TG_PROFILE`` env var,
            then ``"default"``.
        graph_name: Graph name override. If provided, updates the connection's
            active graph.
        connection_config: Explicit connection config dict. If provided, creates
            a one-off connection (not pooled).

    Returns:
        AsyncTigerGraphConnection instance.
    """
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

    effective_profile = profile or os.getenv("TG_PROFILE", "default")
    return ConnectionManager.get_connection_for_profile(effective_profile, graph_name)
