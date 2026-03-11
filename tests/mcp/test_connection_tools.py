"""Tests for pyTigerGraph.mcp.tools.connection_tools."""

import os
import unittest
from unittest.mock import patch

from tests.mcp import MCPToolTestBase
from pyTigerGraph.mcp.connection_manager import ConnectionManager
from pyTigerGraph.mcp.tools.connection_tools import (
    list_connections,
    show_connection,
)


class TestListConnections(MCPToolTestBase):

    def setUp(self):
        super().setUp()
        ConnectionManager._profiles = set()
        ConnectionManager._connection_pool = {}
        ConnectionManager._default_connection = None

    @patch.dict(
        os.environ,
        {
            "TG_HOST": "http://default",
            "STAGING_TG_HOST": "http://staging",
        },
        clear=True,
    )
    async def test_lists_discovered_profiles(self):
        ConnectionManager.load_profiles()

        result = await list_connections()
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["count"], 2)
        profile_names = [p["profile"] for p in resp["data"]["profiles"]]
        self.assertIn("default", profile_names)
        self.assertIn("staging", profile_names)

    @patch.dict(os.environ, {}, clear=True)
    async def test_lists_default_only(self):
        ConnectionManager._profiles = set()
        result = await list_connections()
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["count"], 1)
        self.assertEqual(resp["data"]["profiles"][0]["profile"], "default")

    @patch.dict(
        os.environ,
        {
            "TG_HOST": "http://default",
            "STAGING_TG_HOST": "http://staging",
            "ANALYTICS_TG_HOST": "http://analytics",
        },
        clear=True,
    )
    async def test_three_profiles(self):
        ConnectionManager.load_profiles()
        result = await list_connections()
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["count"], 3)


class TestShowConnection(MCPToolTestBase):

    def setUp(self):
        super().setUp()
        ConnectionManager._profiles = set()
        ConnectionManager._connection_pool = {}
        ConnectionManager._default_connection = None

    @patch.dict(
        os.environ,
        {
            "TG_HOST": "http://my-host",
            "TG_USERNAME": "admin",
            "TG_GRAPHNAME": "MyGraph",
            "TG_PASSWORD": "secret123",
        },
        clear=True,
    )
    async def test_shows_default_profile(self):
        result = await show_connection()
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["host"], "http://my-host")
        self.assertEqual(resp["data"]["username"], "admin")
        self.assertEqual(resp["data"]["graphname"], "MyGraph")
        self.assertNotIn("password", resp["data"])

    @patch.dict(
        os.environ,
        {
            "STAGING_TG_HOST": "http://staging-host",
            "STAGING_TG_USERNAME": "stg_admin",
        },
        clear=True,
    )
    async def test_shows_named_profile(self):
        result = await show_connection(profile="staging")
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["host"], "http://staging-host")
        self.assertEqual(resp["data"]["username"], "stg_admin")
        self.assertEqual(resp["data"]["profile"], "staging")

    @patch.dict(
        os.environ,
        {"TG_HOST": "http://default", "TG_PROFILE": "default"},
        clear=True,
    )
    async def test_falls_back_to_tg_profile_env(self):
        result = await show_connection(profile=None)
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["profile"], "default")
        self.assertEqual(resp["data"]["host"], "http://default")


if __name__ == "__main__":
    unittest.main()
