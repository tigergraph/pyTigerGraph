"""Shared test infrastructure for MCP tool tests.

Provides MCPToolTestBase with mock connection setup and response parsing
helpers so individual test modules stay concise.
"""

import json
import re
import unittest
from unittest.mock import AsyncMock


class MCPToolTestBase(unittest.IsolatedAsyncioTestCase):
    """Base class for all MCP tool tests.

    Sets up a mock ``AsyncTigerGraphConnection`` that every tool function
    receives when ``get_connection()`` is patched.
    """

    def setUp(self):
        self.mock_conn = AsyncMock()
        self.mock_conn.graphname = "TestGraph"
        self.mock_conn.restppUrl = "http://localhost:9000"
        self.mock_conn.host = "http://localhost"
        self.mock_conn.apiToken = ""
        self.mock_conn.jwtToken = ""

    # ------------------------------------------------------------------
    # Response helpers
    # ------------------------------------------------------------------

    _JSON_BLOCK_RE = re.compile(r"```json\s*\n(.*?)\n```", re.DOTALL)

    def parse_response(self, result):
        """Extract the first JSON code-block from a ``List[TextContent]``."""
        self.assertIsInstance(result, list)
        self.assertTrue(len(result) > 0)
        text = result[0].text
        m = self._JSON_BLOCK_RE.search(text)
        self.assertIsNotNone(m, f"No JSON block found in response:\n{text[:300]}")
        return json.loads(m.group(1))

    def assert_success(self, result):
        resp = self.parse_response(result)
        self.assertTrue(resp["success"], f"Expected success but got error: {resp.get('error')}")
        return resp

    def assert_error(self, result):
        resp = self.parse_response(result)
        self.assertFalse(resp["success"], f"Expected error but got success: {resp.get('summary')}")
        return resp
