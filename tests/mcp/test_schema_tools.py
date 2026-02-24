"""Tests for pyTigerGraph.mcp.tools.schema_tools."""

import unittest
from unittest.mock import AsyncMock, patch

from tests.mcp import MCPToolTestBase
from pyTigerGraph.mcp.tools.schema_tools import (
    _build_edge_stmt,
    _build_vertex_stmt,
    create_graph,
    drop_graph,
    get_global_schema,
    get_graph_schema,
    list_graphs,
    clear_graph_data,
    show_graph_details,
)

PATCH_TARGET = "pyTigerGraph.mcp.tools.schema_tools.get_connection"


class TestBuildVertexStmt(unittest.TestCase):
    """Pure-function tests — no mocking needed."""

    def test_primary_id_default(self):
        vtype = {
            "name": "Person",
            "attributes": [{"name": "name", "type": "STRING"}],
        }
        name, stmt = _build_vertex_stmt(vtype)
        self.assertEqual(name, "Person")
        self.assertIn("PRIMARY_ID id STRING", stmt)
        self.assertIn("primary_id_as_attribute", stmt)
        self.assertIn("name STRING", stmt)

    def test_primary_id_explicit(self):
        vtype = {
            "name": "Account",
            "primary_id": "acct_id",
            "primary_id_type": "INT",
            "attributes": [{"name": "balance", "type": "FLOAT"}],
        }
        _, stmt = _build_vertex_stmt(vtype)
        self.assertIn("PRIMARY_ID acct_id INT", stmt)

    def test_primary_key_mode(self):
        vtype = {
            "name": "Doc",
            "attributes": [
                {"name": "doc_id", "type": "STRING", "primary_key": True},
                {"name": "title", "type": "STRING"},
            ],
        }
        _, stmt = _build_vertex_stmt(vtype)
        self.assertIn("doc_id STRING PRIMARY KEY", stmt)
        self.assertNotIn("PRIMARY_ID", stmt)

    def test_composite_key(self):
        vtype = {
            "name": "Event",
            "primary_id": ["date", "venue"],
            "attributes": [
                {"name": "date", "type": "STRING"},
                {"name": "venue", "type": "STRING"},
            ],
        }
        _, stmt = _build_vertex_stmt(vtype)
        self.assertIn("PRIMARY KEY (date, venue)", stmt)

    def test_composite_key_missing_attr_raises(self):
        vtype = {
            "name": "Bad",
            "primary_id": ["missing_col"],
            "attributes": [{"name": "x", "type": "INT"}],
        }
        with self.assertRaises(ValueError):
            _build_vertex_stmt(vtype)

    def test_no_name_returns_none(self):
        name, stmt = _build_vertex_stmt({"attributes": []})
        self.assertIsNone(name)
        self.assertIsNone(stmt)

    def test_default_value(self):
        vtype = {
            "name": "V",
            "attributes": [{"name": "active", "type": "BOOL", "default": True}],
        }
        _, stmt = _build_vertex_stmt(vtype)
        self.assertIn("DEFAULT True", stmt)


class TestBuildEdgeStmt(unittest.TestCase):

    def test_directed_edge(self):
        etype = {
            "name": "FOLLOWS",
            "from_vertex": "Person",
            "to_vertex": "Person",
            "directed": True,
        }
        name, stmt = _build_edge_stmt(etype)
        self.assertEqual(name, "FOLLOWS")
        self.assertIn("DIRECTED EDGE FOLLOWS", stmt)
        self.assertIn("FROM Person", stmt)
        self.assertIn("TO Person", stmt)

    def test_undirected_edge(self):
        etype = {
            "name": "KNOWS",
            "from_vertex": "Person",
            "to_vertex": "Person",
            "directed": False,
        }
        _, stmt = _build_edge_stmt(etype)
        self.assertIn("UNDIRECTED EDGE KNOWS", stmt)

    def test_edge_with_attributes(self):
        etype = {
            "name": "PURCHASED",
            "from_vertex": "User",
            "to_vertex": "Product",
            "attributes": [{"name": "quantity", "type": "INT"}],
        }
        _, stmt = _build_edge_stmt(etype)
        self.assertIn("quantity INT", stmt)

    def test_no_name_returns_none(self):
        name, stmt = _build_edge_stmt({})
        self.assertIsNone(name)


class TestCreateGraph(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Successfully created graph"

        result = await create_graph(
            graph_name="NewGraph",
            vertex_types=[{"name": "Person", "attributes": [{"name": "name", "type": "STRING"}]}],
            edge_types=[{"name": "KNOWS", "from_vertex": "Person", "to_vertex": "Person", "directed": False}],
        )
        resp = self.assert_success(result)
        self.assertIn("NewGraph", resp["summary"])
        self.assertEqual(self.mock_conn.gsql.call_count, 2)

    @patch(PATCH_TARGET)
    async def test_gsql_error_on_create(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = 'Encountered "CREATE" — already exists'

        result = await create_graph(graph_name="Dup", vertex_types=[{"name": "V", "attributes": []}])
        self.assert_error(result)

    @patch(PATCH_TARGET)
    async def test_empty_graph(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Successfully created graph"

        result = await create_graph(graph_name="EmptyG", vertex_types=[])
        resp = self.assert_success(result)
        self.assertIn("Empty", resp["summary"])


class TestDropGraph(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Successfully dropped graph"

        result = await drop_graph(graph_name="OldGraph")
        self.assert_success(result)

    @patch(PATCH_TARGET)
    async def test_gsql_error(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Graph 'OldGraph' does not exist"

        result = await drop_graph(graph_name="OldGraph")
        self.assert_error(result)


class TestListGraphs(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_parse_graph_names(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = (
            "- Graph SocialNetwork(Person:v, KNOWS:e)\n"
            "- Graph FinanceGraph(Account:v, Transfer:e)\n"
        )
        result = await list_graphs()
        resp = self.assert_success(result)
        self.assertIn("SocialNetwork", resp["data"]["graphs"])
        self.assertIn("FinanceGraph", resp["data"]["graphs"])
        self.assertEqual(resp["data"]["count"], 2)

    @patch(PATCH_TARGET)
    async def test_no_graphs(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = ""

        result = await list_graphs()
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["count"], 0)


class TestGetGraphSchema(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getSchema.return_value = {
            "VertexTypes": [{"Name": "Person"}],
            "EdgeTypes": [{"Name": "KNOWS"}],
        }
        result = await get_graph_schema()
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["vertex_type_count"], 1)
        self.assertEqual(resp["data"]["edge_type_count"], 1)

    @patch(PATCH_TARGET)
    async def test_exception(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getSchema.side_effect = Exception("timeout")

        result = await get_graph_schema()
        self.assert_error(result)


class TestGetGlobalSchema(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Global vertex types: ..."

        result = await get_global_schema()
        self.assert_success(result)


class TestClearGraphData(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_requires_confirm(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        result = await clear_graph_data(confirm=False)
        self.assert_error(result)

    @patch(PATCH_TARGET)
    async def test_clear_all(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getVertexTypes.return_value = ["Person", "Product"]
        self.mock_conn.delVertices.return_value = 10

        result = await clear_graph_data(confirm=True)
        resp = self.assert_success(result)
        self.assertEqual(self.mock_conn.delVertices.call_count, 2)

    @patch(PATCH_TARGET)
    async def test_clear_specific_type(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.delVertices.return_value = 5

        result = await clear_graph_data(vertex_type="Person", confirm=True)
        resp = self.assert_success(result)
        self.mock_conn.delVertices.assert_called_once_with("Person")


class TestShowGraphDetails(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_default_ls(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Vertex types: Person\nEdge types: KNOWS"

        result = await show_graph_details()
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["detail_type"], "all")

    @patch(PATCH_TARGET)
    async def test_query_detail(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Queries: myQuery"

        result = await show_graph_details(detail_type="query")
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["detail_type"], "query")


if __name__ == "__main__":
    unittest.main()
