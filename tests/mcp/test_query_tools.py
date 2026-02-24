"""Tests for pyTigerGraph.mcp.tools.query_tools."""

import unittest
from unittest.mock import patch

from tests.mcp import MCPToolTestBase
from pyTigerGraph.mcp.tools.query_tools import (
    drop_query,
    get_neighbors,
    install_query,
    is_query_installed,
    run_installed_query,
    run_query,
    show_query,
    get_query_metadata,
)

PATCH_TARGET = "pyTigerGraph.mcp.tools.query_tools.get_connection"


class TestRunQuery(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_gsql_query(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runInterpretedQuery.return_value = [{"v": []}]

        query = "INTERPRET QUERY () FOR GRAPH G { SELECT v FROM Person:v; PRINT v; }"
        result = await run_query(query_text=query)
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["query_type"], "GSQL")

    @patch(PATCH_TARGET)
    async def test_cypher_query_detected(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runInterpretedQuery.return_value = [{"n": []}]

        query = "INTERPRET OPENCYPHER QUERY () FOR GRAPH G { MATCH (n) RETURN n LIMIT 5 }"
        result = await run_query(query_text=query)
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["query_type"], "openCypher")

    @patch(PATCH_TARGET)
    async def test_exception(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runInterpretedQuery.side_effect = Exception("syntax error")

        result = await run_query(query_text="bad query")
        self.assert_error(result)


class TestRunInstalledQuery(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runInstalledQuery.return_value = [{"result": 42}]

        result = await run_installed_query(
            query_name="myQuery", params={"p": "value"}
        )
        resp = self.assert_success(result)
        self.assertIn("myQuery", resp["summary"])
        self.mock_conn.runInstalledQuery.assert_called_once_with(
            "myQuery", {"p": "value"}
        )

    @patch(PATCH_TARGET)
    async def test_no_params(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runInstalledQuery.return_value = [{}]

        result = await run_installed_query(query_name="simple")
        self.assert_success(result)
        self.mock_conn.runInstalledQuery.assert_called_once_with("simple", {})


class TestInstallQuery(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Successfully created query myQuery"

        query_text = "CREATE QUERY myQuery() FOR GRAPH G { PRINT 1; }"
        result = await install_query(query_text=query_text)
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["query_name"], "myQuery")

    @patch(PATCH_TARGET)
    async def test_gsql_error(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = 'Encountered "SELECT" — Syntax Error'

        result = await install_query(query_text="CREATE QUERY bad() { bad }")
        self.assert_error(result)

    @patch(PATCH_TARGET)
    async def test_query_name_extraction(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "OK"

        result = await install_query(
            query_text="CREATE QUERY getFriends(VERTEX<Person> p) FOR GRAPH G { }"
        )
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["query_name"], "getFriends")


class TestDropQuery(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Successfully dropped query q1"

        result = await drop_query(query_name="q1")
        self.assert_success(result)

    @patch(PATCH_TARGET)
    async def test_gsql_error(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Query 'q1' does not exist"

        result = await drop_query(query_name="q1")
        self.assert_error(result)


class TestShowQuery(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.showQuery.return_value = "CREATE QUERY myQ() { PRINT 1; }"

        result = await show_query(query_name="myQ")
        resp = self.assert_success(result)
        self.assertIn("myQ", resp["data"]["query_name"])


class TestGetQueryMetadata(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getQueryMetadata.return_value = {"params": [], "return": "JSON"}

        result = await get_query_metadata(query_name="myQ")
        self.assert_success(result)


class TestIsQueryInstalled(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_installed(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getQueryMetadata.return_value = {"params": []}

        result = await is_query_installed(query_name="myQ")
        resp = self.assert_success(result)
        self.assertTrue(resp["data"]["installed"])

    @patch(PATCH_TARGET)
    async def test_not_installed(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.getQueryMetadata.side_effect = Exception("not found")

        result = await is_query_installed(query_name="nope")
        resp = self.assert_success(result)
        self.assertFalse(resp["data"]["installed"])


class TestGetNeighbors(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runInterpretedQuery.return_value = [
            {"neighbors": [{"v_id": "u2"}, {"v_id": "u3"}]}
        ]

        result = await get_neighbors(vertex_type="Person", vertex_id="u1")
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["count"], 2)

    @patch(PATCH_TARGET)
    async def test_with_edge_type_filter(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runInterpretedQuery.return_value = [
            {"neighbors": [{"v_id": "u2"}]}
        ]

        result = await get_neighbors(
            vertex_type="Person", vertex_id="u1", edge_type="FOLLOWS"
        )
        resp = self.assert_success(result)
        query_arg = self.mock_conn.runInterpretedQuery.call_args[0][0]
        self.assertIn("FOLLOWS", query_arg)

    @patch(PATCH_TARGET)
    async def test_with_target_type(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runInterpretedQuery.return_value = [{"neighbors": []}]

        result = await get_neighbors(
            vertex_type="Person",
            vertex_id="u1",
            target_vertex_type="Product",
        )
        query_arg = self.mock_conn.runInterpretedQuery.call_args[0][0]
        self.assertIn("Product", query_arg)

    @patch(PATCH_TARGET)
    async def test_empty_result(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runInterpretedQuery.return_value = [{"neighbors": []}]

        result = await get_neighbors(vertex_type="Person", vertex_id="u1")
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["count"], 0)


if __name__ == "__main__":
    unittest.main()
