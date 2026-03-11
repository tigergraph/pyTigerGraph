"""Tests for pyTigerGraph.mcp.tools.statistics_tools."""

import unittest
from unittest.mock import patch

from tests.mcp import MCPToolTestBase
from pyTigerGraph.mcp.tools.statistics_tools import (
    get_edge_count,
    get_node_degree,
    get_vertex_count,
)

PATCH_TARGET = "pyTigerGraph.mcp.tools.statistics_tools.get_connection"


class TestGetVertexCount(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_single_type(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getVertexCount.return_value = 42

        result = await get_vertex_count(vertex_type="Person")
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["count"], 42)
        self.assertEqual(resp["data"]["vertex_type"], "Person")

    @patch(PATCH_TARGET)
    async def test_all_types(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getVertexTypes.return_value = ["Person", "Product"]
        self.mock_conn.getVertexCount.side_effect = [100, 50]

        result = await get_vertex_count()
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["total"], 150)
        self.assertEqual(resp["data"]["counts_by_type"]["Person"], 100)
        self.assertEqual(resp["data"]["counts_by_type"]["Product"], 50)

    @patch(PATCH_TARGET)
    async def test_exception(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getVertexCount.side_effect = Exception("not found")

        result = await get_vertex_count(vertex_type="Bad")
        self.assert_error(result)


class TestGetEdgeCount(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_single_type(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getEdgeCount.return_value = 200

        result = await get_edge_count(edge_type="FOLLOWS")
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["count"], 200)

    @patch(PATCH_TARGET)
    async def test_all_types(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getEdgeTypes.return_value = ["FOLLOWS", "PURCHASED"]
        self.mock_conn.getEdgeCount.side_effect = [200, 80]

        result = await get_edge_count()
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["total"], 280)


class TestGetNodeDegree(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_outgoing(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runInterpretedQuery.return_value = [{"outgoing": 5}]

        result = await get_node_degree(
            vertex_type="Person", vertex_id="u1", direction="outgoing"
        )
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["outgoing_degree"], 5)
        self.assertEqual(resp["data"]["total_degree"], 5)

    @patch(PATCH_TARGET)
    async def test_incoming(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runInterpretedQuery.return_value = [{"incoming": 3}]

        result = await get_node_degree(
            vertex_type="Person", vertex_id="u1", direction="incoming"
        )
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["incoming_degree"], 3)

    @patch(PATCH_TARGET)
    async def test_both(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runInterpretedQuery.return_value = [
            {"outgoing": 5, "incoming": 3}
        ]

        result = await get_node_degree(
            vertex_type="Person", vertex_id="u1", direction="both"
        )
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["total_degree"], 8)

    @patch(PATCH_TARGET)
    async def test_with_edge_type(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runInterpretedQuery.return_value = [{"outgoing": 2}]

        result = await get_node_degree(
            vertex_type="Person",
            vertex_id="u1",
            edge_type="FOLLOWS",
            direction="outgoing",
        )
        query_arg = self.mock_conn.runInterpretedQuery.call_args[0][0]
        self.assertIn("FOLLOWS", query_arg)


class TestProfilePropagation(MCPToolTestBase):
    """Verify profile is forwarded to get_connection for statistics tools."""

    @patch(PATCH_TARGET)
    async def test_get_vertex_count_with_profile(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getVertexCount.return_value = 10

        result = await get_vertex_count(vertex_type="Person", profile="staging")
        self.assert_success(result)
        mock_gc.assert_called_with(profile="staging", graph_name=None)

    @patch(PATCH_TARGET)
    async def test_get_edge_count_with_profile(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getEdgeCount.return_value = 50

        result = await get_edge_count(edge_type="FOLLOWS", profile="analytics")
        self.assert_success(result)
        mock_gc.assert_called_with(profile="analytics", graph_name=None)


if __name__ == "__main__":
    unittest.main()
