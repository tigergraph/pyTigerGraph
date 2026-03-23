"""Unit tests for pyTigerGraph.common.gsql helper functions."""

import unittest

from pyTigerGraph.common.gsql import (
    _wrap_gsql_result,
    _parse_graph_list,
    _GSQL_ERROR_PATTERNS,
)
from pyTigerGraph.common.exception import TigerGraphException


class TestWrapGsqlResult(unittest.TestCase):

    def test_success_message(self):
        result = _wrap_gsql_result("Successfully created graph G1")
        self.assertEqual(result, {"error": False, "message": "Successfully created graph G1"})

    def test_empty_result(self):
        result = _wrap_gsql_result("")
        self.assertEqual(result, {"error": False, "message": ""})

    def test_none_result(self):
        result = _wrap_gsql_result(None)
        self.assertEqual(result, {"error": False, "message": ""})

    def test_error_raises_by_default(self):
        for pattern in _GSQL_ERROR_PATTERNS:
            with self.subTest(pattern=pattern):
                with self.assertRaises(TigerGraphException):
                    _wrap_gsql_result(f"Some output with {pattern} in it")

    def test_error_skipCheck_returns_dict(self):
        for pattern in _GSQL_ERROR_PATTERNS:
            with self.subTest(pattern=pattern):
                msg = f"Some output with {pattern} in it"
                result = _wrap_gsql_result(msg, skipCheck=True)
                self.assertTrue(result["error"])
                self.assertEqual(result["message"], msg)

    def test_no_false_positive(self):
        clean_messages = [
            "Successfully created query myQuery",
            "The query has been installed",
            "Schema changed successfully",
            "Graph dropped",
            "",
        ]
        for msg in clean_messages:
            with self.subTest(msg=msg):
                result = _wrap_gsql_result(msg)
                self.assertFalse(result["error"])

    def test_specific_error_patterns(self):
        cases = [
            ('Encountered "bad token" at line 1', True),
            ("SEMANTIC ERROR in query foo", True),
            ("Syntax Error: unexpected token", True),
            ("Failed to create schema", True),
            ("Vertex type 'Foo' does not exist", True),
            ("Edge type is not a valid identifier", True),
            ("Graph already exists in cluster", True),
            ("Invalid syntax near ';'", True),
            ("Query installed successfully", False),
        ]
        for msg, expect_error in cases:
            with self.subTest(msg=msg):
                if expect_error:
                    with self.assertRaises(TigerGraphException):
                        _wrap_gsql_result(msg)
                    result = _wrap_gsql_result(msg, skipCheck=True)
                    self.assertTrue(result["error"])
                else:
                    result = _wrap_gsql_result(msg)
                    self.assertFalse(result["error"])


class TestParseGraphList(unittest.TestCase):

    def test_single_graph(self):
        output = "- Graph SocialNet(Person:v, KNOWS:e)"
        result = _parse_graph_list(output)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["GraphName"], "SocialNet")
        self.assertEqual(result[0]["VertexTypes"], ["Person"])
        self.assertEqual(result[0]["EdgeTypes"], ["KNOWS"])

    def test_multiple_graphs(self):
        output = (
            "- Graph G1(V1:v, V2:v, E1:e)\n"
            "- Graph G2(Account:v, Transfer:e)\n"
        )
        result = _parse_graph_list(output)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["GraphName"], "G1")
        self.assertEqual(result[0]["VertexTypes"], ["V1", "V2"])
        self.assertEqual(result[0]["EdgeTypes"], ["E1"])
        self.assertEqual(result[1]["GraphName"], "G2")
        self.assertEqual(result[1]["VertexTypes"], ["Account"])
        self.assertEqual(result[1]["EdgeTypes"], ["Transfer"])

    def test_empty_output(self):
        self.assertEqual(_parse_graph_list(""), [])
        self.assertEqual(_parse_graph_list(None), [])

    def test_graph_without_types(self):
        output = "- Graph EmptyGraph()"
        result = _parse_graph_list(output)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["GraphName"], "EmptyGraph")
        self.assertEqual(result[0]["VertexTypes"], [])
        self.assertEqual(result[0]["EdgeTypes"], [])

    def test_non_graph_lines_ignored(self):
        output = (
            "Some header text\n"
            "- Graph G1(V:v)\n"
            "Other info\n"
        )
        result = _parse_graph_list(output)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["GraphName"], "G1")

    def test_graph_star_ignored(self):
        output = "- Graph *\n- Graph RealGraph(V:v)"
        result = _parse_graph_list(output)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["GraphName"], "RealGraph")

    def test_multiple_vertex_edge_types(self):
        output = "- Graph Complex(Person:v, Product:v, Company:v, BOUGHT:e, WORKS_AT:e)"
        result = _parse_graph_list(output)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["VertexTypes"], ["Person", "Product", "Company"])
        self.assertEqual(result[0]["EdgeTypes"], ["BOUGHT", "WORKS_AT"])


if __name__ == '__main__':
    unittest.main()
