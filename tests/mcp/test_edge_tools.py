"""Tests for pyTigerGraph.mcp.tools.edge_tools."""

import unittest
from unittest.mock import patch

from tests.mcp import MCPToolTestBase
from pyTigerGraph.mcp.tools.edge_tools import (
    add_edge,
    add_edges,
    delete_edge,
    delete_edges,
    get_edge,
    get_edges,
    has_edge,
)

PATCH_TARGET = "pyTigerGraph.mcp.tools.edge_tools.get_connection"


class TestAddEdge(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.upsertEdge.return_value = None

        result = await add_edge(
            source_vertex_type="Person",
            source_vertex_id="u1",
            edge_type="FOLLOWS",
            target_vertex_type="Person",
            target_vertex_id="u2",
            attributes={"since": "2024-01-01"},
        )
        resp = self.assert_success(result)
        self.assertIn("u1", resp["summary"])
        self.assertIn("u2", resp["summary"])
        self.mock_conn.upsertEdge.assert_called_once()

    @patch(PATCH_TARGET)
    async def test_exception(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.upsertEdge.side_effect = Exception("edge type not found")

        result = await add_edge(
            source_vertex_type="Person",
            source_vertex_id="u1",
            edge_type="BAD",
            target_vertex_type="Person",
            target_vertex_id="u2",
        )
        self.assert_error(result)


class TestAddEdges(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.upsertEdges.return_value = None

        edges = [
            {"source_type": "Person", "source_id": "u1", "target_type": "Person", "target_id": "u2"},
            {"source_type": "Person", "source_id": "u2", "target_type": "Person", "target_id": "u3"},
        ]
        result = await add_edges(edge_type="FOLLOWS", edges=edges)
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["edge_count"], 2)

    @patch(PATCH_TARGET)
    async def test_empty_list_raises(self, mock_gc):
        mock_gc.return_value = self.mock_conn

        result = await add_edges(edge_type="FOLLOWS", edges=[])
        self.assert_error(result)

    @patch(PATCH_TARGET)
    async def test_missing_types_raises(self, mock_gc):
        mock_gc.return_value = self.mock_conn

        edges = [{"source_id": "u1", "target_id": "u2"}]
        result = await add_edges(edge_type="FOLLOWS", edges=edges)
        self.assert_error(result)


class TestGetEdge(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_found(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getEdges.return_value = [
            {"e_type": "FOLLOWS", "from_id": "u1", "to_id": "u2", "attributes": {}}
        ]

        result = await get_edge(
            source_vertex_type="Person",
            source_vertex_id="u1",
            edge_type="FOLLOWS",
            target_vertex_type="Person",
            target_vertex_id="u2",
        )
        resp = self.assert_success(result)
        self.assertIn("Found", resp["summary"])

    @patch(PATCH_TARGET)
    async def test_not_found(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getEdges.return_value = []

        result = await get_edge(
            source_vertex_type="Person",
            source_vertex_id="u1",
            edge_type="FOLLOWS",
            target_vertex_type="Person",
            target_vertex_id="missing",
        )
        resp = self.assert_success(result)
        self.assertIn("not found", resp["summary"])


class TestGetEdges(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_by_source(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getEdges.return_value = [
            {"e_type": "FOLLOWS", "from_id": "u1", "to_id": "u2"},
        ]

        result = await get_edges(
            source_vertex_type="Person",
            source_vertex_id="u1",
            edge_type="FOLLOWS",
        )
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["count"], 1)

    @patch(PATCH_TARGET)
    async def test_by_type(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getEdgesByType.return_value = [
            {"e_type": "FOLLOWS", "from_id": "u1", "to_id": "u2"},
        ]

        result = await get_edges(edge_type="FOLLOWS")
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["count"], 1)


class TestDeleteEdge(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.delEdges.return_value = 1

        result = await delete_edge(
            source_vertex_type="Person",
            source_vertex_id="u1",
            edge_type="FOLLOWS",
            target_vertex_type="Person",
            target_vertex_id="u2",
        )
        self.assert_success(result)


class TestDeleteEdges(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.delEdges.return_value = 1

        edges = [
            {"source_type": "Person", "source_id": "u1", "target_type": "Person", "target_id": "u2"},
            {"source_type": "Person", "source_id": "u2", "target_type": "Person", "target_id": "u3"},
        ]
        result = await delete_edges(edge_type="FOLLOWS", edges=edges)
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["deleted_count"], 2)


class TestHasEdge(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_exists(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getEdges.return_value = [{"e_type": "FOLLOWS"}]

        result = await has_edge(
            source_vertex_type="Person",
            source_vertex_id="u1",
            edge_type="FOLLOWS",
            target_vertex_type="Person",
            target_vertex_id="u2",
        )
        resp = self.assert_success(result)
        self.assertTrue(resp["data"]["exists"])

    @patch(PATCH_TARGET)
    async def test_not_exists(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getEdges.return_value = []

        result = await has_edge(
            source_vertex_type="Person",
            source_vertex_id="u1",
            edge_type="FOLLOWS",
            target_vertex_type="Person",
            target_vertex_id="nope",
        )
        resp = self.assert_success(result)
        self.assertFalse(resp["data"]["exists"])

    @patch(PATCH_TARGET)
    async def test_source_missing_returns_false(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getEdges.side_effect = Exception("source not found")

        result = await has_edge(
            source_vertex_type="Person",
            source_vertex_id="ghost",
            edge_type="FOLLOWS",
            target_vertex_type="Person",
            target_vertex_id="u2",
        )
        resp = self.assert_success(result)
        self.assertFalse(resp["data"]["exists"])


class TestProfilePropagation(MCPToolTestBase):
    """Verify profile is forwarded to get_connection for edge tools."""

    @patch(PATCH_TARGET)
    async def test_add_edge_with_profile(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.upsertEdge.return_value = None

        result = await add_edge(
            source_vertex_type="Person",
            source_vertex_id="u1",
            edge_type="FOLLOWS",
            target_vertex_type="Person",
            target_vertex_id="u2",
            profile="staging",
        )
        self.assert_success(result)
        mock_gc.assert_called_with(profile="staging", graph_name=None)

    @patch(PATCH_TARGET)
    async def test_get_edge_with_profile_and_graph(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getEdges.return_value = [{"e_type": "FOLLOWS", "from_id": "u1", "to_id": "u2"}]

        result = await get_edge(
            source_vertex_type="Person",
            source_vertex_id="u1",
            edge_type="FOLLOWS",
            target_vertex_type="Person",
            target_vertex_id="u2",
            profile="analytics",
            graph_name="FinGraph",
        )
        self.assert_success(result)
        mock_gc.assert_called_with(profile="analytics", graph_name="FinGraph")


if __name__ == "__main__":
    unittest.main()
