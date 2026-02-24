"""Tests for pyTigerGraph.mcp.response_formatter."""

import json
import unittest

from pyTigerGraph.mcp.response_formatter import (
    format_error,
    format_list_response,
    format_success,
    gsql_has_error,
)


class TestGsqlHasError(unittest.TestCase):
    """Verify all known error patterns are detected."""

    ERROR_STRINGS = [
        'Encountered "bad token" at line 1',
        "SEMANTIC ERROR in query foo",
        "Syntax Error: unexpected token",
        "Failed to create schema",
        "Vertex type 'Foo' does not exist",
        "Edge type is not a valid identifier",
        "already exists in graph",
        "Invalid syntax near ';'",
    ]

    def test_each_error_pattern_detected(self):
        for s in self.ERROR_STRINGS:
            with self.subTest(s=s):
                self.assertTrue(gsql_has_error(s), f"Should detect error in: {s}")

    def test_clean_output_not_flagged(self):
        clean = [
            "Successfully created query myQuery",
            "The query has been installed",
            "Schema changed successfully",
            "",
        ]
        for s in clean:
            with self.subTest(s=s):
                self.assertFalse(gsql_has_error(s), f"Should NOT flag: {s}")


class TestFormatSuccess(unittest.TestCase):

    def test_basic_success(self):
        result = format_success(
            operation="test_op",
            summary="It worked",
            data={"key": "value"},
        )
        self.assertEqual(len(result), 1)
        text = result[0].text
        self.assertIn('"success": true', text)
        self.assertIn('"operation": "test_op"', text)
        self.assertIn("It worked", text)

    def test_with_suggestions_and_metadata(self):
        result = format_success(
            operation="op",
            summary="ok",
            suggestions=["Try this", "Or that"],
            metadata={"graph_name": "G"},
        )
        text = result[0].text
        self.assertIn("Try this", text)
        self.assertIn("Or that", text)
        self.assertIn("graph_name", text)


class TestFormatError(unittest.TestCase):

    def test_basic_error(self):
        result = format_error(
            operation="fail_op",
            error=ValueError("bad value"),
        )
        text = result[0].text
        self.assertIn('"success": false', text)
        self.assertIn("bad value", text)

    def test_schema_error_suggestions(self):
        result = format_error(
            operation="op",
            error=Exception("vertex type not found"),
        )
        text = result[0].text
        self.assertIn("show_graph_details", text)

    def test_connection_error_suggestions(self):
        result = format_error(
            operation="op",
            error=Exception("connection refused"),
        )
        text = result[0].text
        self.assertIn("TG_HOST", text)

    def test_auth_error_suggestions(self):
        result = format_error(
            operation="op",
            error=Exception("authentication failed"),
        )
        text = result[0].text
        self.assertIn("TG_USERNAME", text)

    def test_syntax_error_suggestions(self):
        result = format_error(
            operation="op",
            error=Exception("syntax error near SELECT"),
        )
        text = result[0].text
        self.assertIn("INTERPRET QUERY", text)

    def test_custom_suggestions_override(self):
        result = format_error(
            operation="op",
            error=Exception("something"),
            suggestions=["Custom hint"],
        )
        text = result[0].text
        self.assertIn("Custom hint", text)

    def test_context_in_metadata(self):
        result = format_error(
            operation="op",
            error=Exception("err"),
            context={"graph_name": "G1"},
        )
        text = result[0].text
        self.assertIn("G1", text)


class TestFormatListResponse(unittest.TestCase):

    def test_list_response(self):
        result = format_list_response(
            operation="list_op",
            items=["a", "b", "c"],
            item_type="things",
        )
        text = result[0].text
        self.assertIn("3", text)
        self.assertIn("things", text)


if __name__ == "__main__":
    unittest.main()
