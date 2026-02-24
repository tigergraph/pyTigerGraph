"""Tests for pyTigerGraph.mcp.tools.node_tools."""

import unittest
from unittest.mock import patch

from tests.mcp import MCPToolTestBase
from pyTigerGraph.mcp.tools.node_tools import (
    add_node,
    add_nodes,
    delete_node,
    delete_nodes,
    get_node,
    get_nodes,
    get_node_edges,
    has_node,
)

PATCH_TARGET = "pyTigerGraph.mcp.tools.node_tools.get_connection"


class TestAddNode(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.upsertVertex.return_value = None

        result = await add_node(
            vertex_type="Person",
            vertex_id="user1",
            attributes={"name": "Alice", "age": 30},
        )
        resp = self.assert_success(result)
        self.assertIn("user1", resp["summary"])
        self.mock_conn.upsertVertex.assert_called_once_with(
            "Person", "user1", {"name": "Alice", "age": 30}
        )

    @patch(PATCH_TARGET)
    async def test_exception(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.upsertVertex.side_effect = Exception("vertex type not found")

        result = await add_node(vertex_type="Bad", vertex_id="x")
        self.assert_error(result)

    @patch(PATCH_TARGET)
    async def test_no_attributes(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.upsertVertex.return_value = None

        result = await add_node(vertex_type="Person", vertex_id="user2")
        self.assert_success(result)
        self.mock_conn.upsertVertex.assert_called_once_with("Person", "user2", {})


class TestAddNodes(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.upsertVertices.return_value = None

        vertices = [
            {"id": "u1", "name": "Alice"},
            {"id": "u2", "name": "Bob"},
        ]
        result = await add_nodes(vertex_type="Person", vertices=vertices)
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["success_count"], 2)
        self.assertEqual(resp["data"]["failed_count"], 0)

    @patch(PATCH_TARGET)
    async def test_missing_primary_key(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.upsertVertices.return_value = None

        vertices = [
            {"id": "u1", "name": "Alice"},
            {"name": "Bob"},  # missing "id"
        ]
        result = await add_nodes(vertex_type="Person", vertices=vertices)
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["success_count"], 1)
        self.assertEqual(resp["data"]["failed_count"], 1)

    @patch(PATCH_TARGET)
    async def test_custom_vertex_id_field(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.upsertVertices.return_value = None

        vertices = [{"ACCT_ID": 1001, "balance": 100.0}]
        result = await add_nodes(
            vertex_type="Account", vertices=vertices, vertex_id="ACCT_ID"
        )
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["success_count"], 1)


class TestGetNode(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_found(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getVerticesById.return_value = [
            {"v_id": "user1", "v_type": "Person", "attributes": {"name": "Alice"}}
        ]

        result = await get_node(vertex_type="Person", vertex_id="user1")
        resp = self.assert_success(result)
        self.assertIn("user1", resp["summary"])

    @patch(PATCH_TARGET)
    async def test_not_found(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getVerticesById.return_value = []

        result = await get_node(vertex_type="Person", vertex_id="missing")
        resp = self.assert_success(result)
        self.assertIn("not found", resp["summary"])


class TestGetNodes(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_with_filter(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getVertices.return_value = [
            {"v_id": "u1", "attributes": {"age": 30}},
        ]

        result = await get_nodes(vertex_type="Person", where="age > 25", limit=10)
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["count"], 1)

    @patch(PATCH_TARGET)
    async def test_no_results(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getVertices.return_value = []

        result = await get_nodes(vertex_type="Person")
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["count"], 0)


class TestDeleteNode(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_deleted(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.delVerticesById.return_value = 1

        result = await delete_node(vertex_type="Person", vertex_id="u1")
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["deleted_count"], 1)

    @patch(PATCH_TARGET)
    async def test_not_found(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.delVerticesById.return_value = 0

        result = await delete_node(vertex_type="Person", vertex_id="nope")
        resp = self.assert_success(result)
        self.assertIn("No vertex", resp["summary"])


class TestDeleteNodes(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_by_ids(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.delVerticesById.return_value = 3

        result = await delete_nodes(
            vertex_type="Person", vertex_ids=["u1", "u2", "u3"]
        )
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["deleted_count"], 3)

    @patch(PATCH_TARGET)
    async def test_by_where(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.delVertices.return_value = 5

        result = await delete_nodes(vertex_type="Person", where="age > 70")
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["deleted_count"], 5)


class TestHasNode(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_exists(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getVerticesById.return_value = [{"v_id": "u1"}]

        result = await has_node(vertex_type="Person", vertex_id="u1")
        resp = self.assert_success(result)
        self.assertTrue(resp["data"]["exists"])

    @patch(PATCH_TARGET)
    async def test_not_exists(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getVerticesById.return_value = []

        result = await has_node(vertex_type="Person", vertex_id="nope")
        resp = self.assert_success(result)
        self.assertFalse(resp["data"]["exists"])


class TestGetNodeEdges(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getEdges.return_value = [
            {"e_type": "FOLLOWS", "from_id": "u1", "to_id": "u2"},
            {"e_type": "FOLLOWS", "from_id": "u1", "to_id": "u3"},
        ]

        result = await get_node_edges(vertex_type="Person", vertex_id="u1")
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["count"], 2)

    @patch(PATCH_TARGET)
    async def test_no_edges(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getEdges.return_value = []

        result = await get_node_edges(vertex_type="Person", vertex_id="u1")
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["count"], 0)

    @patch(PATCH_TARGET)
    async def test_with_edge_type_filter(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getEdges.return_value = [
            {"e_type": "FOLLOWS", "from_id": "u1", "to_id": "u2"},
        ]

        result = await get_node_edges(
            vertex_type="Person", vertex_id="u1", edge_type="FOLLOWS"
        )
        resp = self.assert_success(result)
        self.mock_conn.getEdges.assert_called_once_with(
            sourceVertexType="Person",
            sourceVertexId="u1",
            edgeType="FOLLOWS",
        )


if __name__ == "__main__":
    unittest.main()
