"""Tests for pyTigerGraph.mcp.tools.gsql_tools."""

import os
import unittest
from unittest.mock import patch

from tests.mcp import MCPToolTestBase
from pyTigerGraph.mcp.tools.gsql_tools import get_llm_config, gsql

PATCH_TARGET = "pyTigerGraph.mcp.tools.gsql_tools.get_connection"


class TestGsql(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Successfully created vertex Person"

        result = await gsql(command="CREATE VERTEX Person (PRIMARY_ID id STRING)")
        resp = self.assert_success(result)
        self.assertIn("Successfully created", resp["data"]["result"])

    @patch(PATCH_TARGET)
    async def test_gsql_error_detected(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = 'Encountered "BAD" — Syntax Error'

        result = await gsql(command="BAD COMMAND")
        self.assert_error(result)

    @patch(PATCH_TARGET)
    async def test_exception(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.side_effect = Exception("connection refused")

        result = await gsql(command="LS")
        self.assert_error(result)

    @patch(PATCH_TARGET)
    async def test_with_graph_name(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "OK"

        result = await gsql(command="LS", graph_name="MyGraph")
        self.assert_success(result)
        mock_gc.assert_called_with(profile=None, graph_name="MyGraph")

    @patch(PATCH_TARGET)
    async def test_with_profile(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "OK"

        result = await gsql(command="LS", profile="staging")
        self.assert_success(result)
        mock_gc.assert_called_with(profile="staging", graph_name=None)

    @patch(PATCH_TARGET)
    async def test_with_profile_and_graph(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "OK"

        result = await gsql(command="LS", profile="analytics", graph_name="FinGraph")
        self.assert_success(result)
        mock_gc.assert_called_with(profile="analytics", graph_name="FinGraph")


class TestGetLlmConfig(unittest.TestCase):
    """Tests for the LLM config env-var parsing logic."""

    def _with_env(self, **env_vars):
        """Context manager to set env vars and restore originals."""
        return patch.dict(os.environ, env_vars, clear=False)

    def test_defaults(self):
        with patch.dict(os.environ, {}, clear=True):
            # Remove any existing LLM_* vars
            os.environ.pop("LLM_MODEL", None)
            os.environ.pop("LLM_PROVIDER", None)
            provider, model = get_llm_config()
            self.assertEqual(provider, "openai")
            self.assertEqual(model, "gpt-4o")

    def test_provider_colon_model(self):
        with self._with_env(LLM_MODEL="anthropic:claude-3"):
            provider, model = get_llm_config()
            self.assertEqual(provider, "anthropic")
            self.assertEqual(model, "claude-3")

    def test_model_with_separate_provider(self):
        with self._with_env(LLM_MODEL="claude-3", LLM_PROVIDER="anthropic"):
            provider, model = get_llm_config()
            self.assertEqual(provider, "anthropic")
            self.assertEqual(model, "claude-3")

    def test_model_without_provider_uses_default(self):
        with self._with_env(LLM_MODEL="gpt-4-turbo"):
            os.environ.pop("LLM_PROVIDER", None)
            provider, model = get_llm_config()
            self.assertEqual(provider, "openai")
            self.assertEqual(model, "gpt-4-turbo")

    def test_invalid_colon_format_raises(self):
        with self._with_env(LLM_MODEL=":model_only"):
            with self.assertRaises(ValueError):
                get_llm_config()

    def test_provider_only_uses_default_model(self):
        with self._with_env(LLM_PROVIDER="bedrock_converse"):
            os.environ.pop("LLM_MODEL", None)
            provider, model = get_llm_config()
            self.assertEqual(provider, "bedrock_converse")
            self.assertEqual(model, "gpt-4o")


if __name__ == "__main__":
    unittest.main()
