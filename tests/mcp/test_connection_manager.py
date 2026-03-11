"""Tests for pyTigerGraph.mcp.connection_manager multi-profile support."""

import os
import unittest
from unittest.mock import patch, MagicMock

from pyTigerGraph.mcp.connection_manager import (
    ConnectionManager,
    _get_env_for_profile,
    get_connection,
)


class TestGetEnvForProfile(unittest.TestCase):
    """Verify env-var resolution for default and named profiles."""

    @patch.dict(os.environ, {"TG_HOST": "http://default-host"}, clear=False)
    def test_default_profile_reads_unprefixed(self):
        self.assertEqual(
            _get_env_for_profile("default", "HOST"),
            "http://default-host",
        )

    @patch.dict(os.environ, {"TG_HOST": "http://default-host"}, clear=False)
    def test_default_profile_returns_builtin_default(self):
        val = _get_env_for_profile("default", "NONEXISTENT_KEY", "fallback")
        self.assertEqual(val, "fallback")

    @patch.dict(
        os.environ,
        {"STAGING_TG_HOST": "http://staging-host", "TG_HOST": "http://default-host"},
        clear=False,
    )
    def test_named_profile_reads_prefixed(self):
        self.assertEqual(
            _get_env_for_profile("staging", "HOST"),
            "http://staging-host",
        )

    @patch.dict(
        os.environ,
        {"TG_USERNAME": "shared_user"},
        clear=False,
    )
    def test_named_profile_falls_back_to_unprefixed(self):
        val = _get_env_for_profile("staging", "USERNAME")
        self.assertEqual(val, "shared_user")

    @patch.dict(os.environ, {}, clear=True)
    def test_named_profile_falls_back_to_builtin(self):
        val = _get_env_for_profile("staging", "HOST", "http://127.0.0.1")
        self.assertEqual(val, "http://127.0.0.1")

    @patch.dict(
        os.environ,
        {
            "PROD_US_TG_HOST": "http://prod-us",
            "TG_HOST": "http://default",
        },
        clear=False,
    )
    def test_multi_word_profile(self):
        self.assertEqual(
            _get_env_for_profile("prod_us", "HOST"),
            "http://prod-us",
        )


class TestConnectionManagerLoadProfiles(unittest.TestCase):

    def setUp(self):
        ConnectionManager._profiles = set()
        ConnectionManager._connection_pool = {}
        ConnectionManager._default_connection = None

    @patch.dict(
        os.environ,
        {
            "TG_HOST": "http://default",
            "STAGING_TG_HOST": "http://staging",
            "ANALYTICS_TG_HOST": "http://analytics",
        },
        clear=False,
    )
    @patch("pyTigerGraph.mcp.connection_manager._load_env_file")
    def test_discovers_named_profiles(self, mock_load_env):
        ConnectionManager.load_profiles()
        profiles = ConnectionManager.list_profiles()
        self.assertIn("default", profiles)
        self.assertIn("staging", profiles)
        self.assertIn("analytics", profiles)
        self.assertEqual(len(profiles), 3)

    @patch.dict(os.environ, {"TG_HOST": "http://default"}, clear=True)
    @patch("pyTigerGraph.mcp.connection_manager._load_env_file")
    def test_always_includes_default(self, mock_load_env):
        ConnectionManager.load_profiles()
        self.assertIn("default", ConnectionManager.list_profiles())

    @patch.dict(os.environ, {}, clear=True)
    def test_list_profiles_without_load(self):
        """list_profiles should return at least 'default' even without load."""
        profiles = ConnectionManager.list_profiles()
        self.assertIn("default", profiles)


class TestConnectionManagerPool(unittest.TestCase):

    def setUp(self):
        ConnectionManager._profiles = set()
        ConnectionManager._connection_pool = {}
        ConnectionManager._default_connection = None

    @patch.dict(
        os.environ,
        {"TG_HOST": "http://default-host", "TG_USERNAME": "admin"},
        clear=True,
    )
    def test_creates_connection_for_default(self):
        conn = ConnectionManager.get_connection_for_profile("default")
        self.assertEqual(conn.host, "http://default-host")
        self.assertEqual(conn.username, "admin")

    @patch.dict(
        os.environ,
        {
            "TG_HOST": "http://default-host",
            "STAGING_TG_HOST": "http://staging-host",
            "STAGING_TG_USERNAME": "stg_user",
        },
        clear=True,
    )
    def test_creates_separate_connections_per_profile(self):
        conn_default = ConnectionManager.get_connection_for_profile("default")
        conn_staging = ConnectionManager.get_connection_for_profile("staging")
        self.assertIsNot(conn_default, conn_staging)
        self.assertEqual(conn_default.host, "http://default-host")
        self.assertEqual(conn_staging.host, "http://staging-host")
        self.assertEqual(conn_staging.username, "stg_user")

    @patch.dict(os.environ, {"TG_HOST": "http://default-host"}, clear=True)
    def test_pool_caches_by_profile(self):
        conn1 = ConnectionManager.get_connection_for_profile("default")
        conn2 = ConnectionManager.get_connection_for_profile("default")
        self.assertIs(conn1, conn2)

    @patch.dict(os.environ, {"TG_HOST": "http://default-host"}, clear=True)
    def test_graph_name_override(self):
        conn = ConnectionManager.get_connection_for_profile("default", graph_name="MyGraph")
        self.assertEqual(conn.graphname, "MyGraph")

    @patch.dict(os.environ, {"TG_HOST": "http://default-host"}, clear=True)
    def test_graph_name_updated_on_cached_conn(self):
        conn1 = ConnectionManager.get_connection_for_profile("default", graph_name="Graph1")
        conn2 = ConnectionManager.get_connection_for_profile("default", graph_name="Graph2")
        self.assertIs(conn1, conn2)
        self.assertEqual(conn2.graphname, "Graph2")

    @patch.dict(os.environ, {"TG_HOST": "http://default-host"}, clear=True)
    def test_default_sets_legacy_ref(self):
        conn = ConnectionManager.get_connection_for_profile("default")
        self.assertIs(ConnectionManager.get_default_connection(), conn)

    @patch.dict(
        os.environ,
        {"STAGING_TG_HOST": "http://staging-host"},
        clear=True,
    )
    def test_named_profile_does_not_set_legacy_ref(self):
        ConnectionManager.get_connection_for_profile("staging")
        self.assertIsNone(ConnectionManager.get_default_connection())


class TestConnectionManagerProfileInfo(unittest.TestCase):

    @patch.dict(
        os.environ,
        {
            "TG_HOST": "http://my-host",
            "TG_USERNAME": "admin",
            "TG_PASSWORD": "supersecret",
            "TG_SECRET": "mysecret",
            "TG_GRAPHNAME": "ProdGraph",
        },
        clear=True,
    )
    def test_info_excludes_secrets(self):
        info = ConnectionManager.get_profile_info("default")
        self.assertEqual(info["host"], "http://my-host")
        self.assertEqual(info["username"], "admin")
        self.assertEqual(info["graphname"], "ProdGraph")
        self.assertNotIn("password", info)
        self.assertNotIn("secret", info)
        self.assertNotIn("api_token", info)
        self.assertNotIn("jwt_token", info)


class TestGetConnectionFunction(unittest.TestCase):

    def setUp(self):
        ConnectionManager._profiles = set()
        ConnectionManager._connection_pool = {}
        ConnectionManager._default_connection = None

    @patch.dict(
        os.environ,
        {"TG_HOST": "http://default", "TG_PROFILE": "default"},
        clear=True,
    )
    def test_no_args_uses_tg_profile_env(self):
        conn = get_connection()
        self.assertEqual(conn.host, "http://default")

    @patch.dict(
        os.environ,
        {
            "TG_HOST": "http://default",
            "STAGING_TG_HOST": "http://staging",
            "TG_PROFILE": "staging",
        },
        clear=True,
    )
    def test_tg_profile_env_selects_staging(self):
        conn = get_connection()
        self.assertEqual(conn.host, "http://staging")

    @patch.dict(
        os.environ,
        {
            "TG_HOST": "http://default",
            "STAGING_TG_HOST": "http://staging",
            "TG_PROFILE": "default",
        },
        clear=True,
    )
    def test_explicit_profile_overrides_env(self):
        conn = get_connection(profile="staging")
        self.assertEqual(conn.host, "http://staging")

    @patch.dict(os.environ, {"TG_HOST": "http://default"}, clear=True)
    def test_graph_name_passthrough(self):
        conn = get_connection(graph_name="TestG")
        self.assertEqual(conn.graphname, "TestG")

    def test_connection_config_creates_oneoff(self):
        conn = get_connection(connection_config={
            "host": "http://adhoc",
            "graphname": "AdHocGraph",
            "username": "user1",
            "password": "pass1",
        })
        self.assertEqual(conn.host, "http://adhoc")
        self.assertEqual(conn.graphname, "AdHocGraph")
        self.assertNotIn("adhoc", ConnectionManager._connection_pool)


class TestBackwardCompatibility(unittest.TestCase):
    """Existing single-connection usage (no profile param) should keep working."""

    def setUp(self):
        ConnectionManager._profiles = set()
        ConnectionManager._connection_pool = {}
        ConnectionManager._default_connection = None

    @patch.dict(
        os.environ,
        {
            "TG_HOST": "http://legacy-host",
            "TG_GRAPHNAME": "LegacyGraph",
            "TG_USERNAME": "tigergraph",
            "TG_PASSWORD": "tigergraph",
        },
        clear=True,
    )
    def test_get_connection_without_profile(self):
        conn = get_connection()
        self.assertEqual(conn.host, "http://legacy-host")
        self.assertEqual(conn.graphname, "LegacyGraph")

    @patch.dict(
        os.environ,
        {"TG_HOST": "http://legacy-host"},
        clear=True,
    )
    def test_create_connection_from_env_backward_compat(self):
        conn = ConnectionManager.create_connection_from_env()
        self.assertEqual(conn.host, "http://legacy-host")
        self.assertIs(ConnectionManager.get_default_connection(), conn)


if __name__ == "__main__":
    unittest.main()
