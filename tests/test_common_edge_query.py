"""Unit tests for pyTigerGraph.common.edge and pyTigerGraph.common.query helpers.

These tests exercise the helper functions in isolation — no live TigerGraph
server is required.
"""
import json
import sys
import os
import unittest
from datetime import datetime

# Make sure the project source is on the path when running from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyTigerGraph.common.edge import _dumps, _prep_upsert_edge_dataframe
from pyTigerGraph.common.query import _parse_query_parameters


# ---------------------------------------------------------------------------
# _dumps
# ---------------------------------------------------------------------------

class TestDumps(unittest.TestCase):
    """Tests for common/edge._dumps()"""

    def test_non_dict_string(self):
        """Non-dict leaf values must be serialized with json.dumps, not '{}'."""
        self.assertEqual('"hello"', _dumps("hello"))

    def test_non_dict_int(self):
        self.assertEqual("42", _dumps(42))

    def test_non_dict_float(self):
        self.assertEqual("3.14", _dumps(3.14))

    def test_non_dict_bool(self):
        self.assertEqual("true", _dumps(True))

    def test_non_dict_none(self):
        self.assertEqual("null", _dumps(None))

    def test_non_dict_list(self):
        self.assertEqual("[1, 2, 3]", _dumps([1, 2, 3]))

    def test_empty_dict(self):
        self.assertEqual("{}", _dumps({}))

    def test_flat_dict_scalar_values(self):
        """Flat dicts with scalar values round-trip through JSON correctly."""
        result = _dumps({"a": 1, "b": "two"})
        parsed = json.loads(result)
        self.assertEqual({"a": 1, "b": "two"}, parsed)

    def test_nested_dict(self):
        """Nested dicts produce valid JSON."""
        result = _dumps({"outer": {"inner": 99}})
        parsed = json.loads(result)
        self.assertEqual({"outer": {"inner": 99}}, parsed)


# ---------------------------------------------------------------------------
# _parse_query_parameters
# ---------------------------------------------------------------------------

class TestParseQueryParameters(unittest.TestCase):
    """Tests for common/query._parse_query_parameters()"""

    def test_simple_scalar(self):
        result = _parse_query_parameters({"k": "v"})
        self.assertEqual("k=v", result)

    def test_multiple_scalars(self):
        result = _parse_query_parameters({"a": "1", "b": "2"})
        parts = sorted(result.split("&"))
        self.assertEqual(["a=1", "b=2"], parts)

    def test_list_values_repeated_key(self):
        """Lists must be encoded as repeated keys: k=v1&k=v2."""
        result = _parse_query_parameters({"colors": ["red", "green", "blue"]})
        parts = result.split("&")
        self.assertEqual(["colors=red", "colors=green", "colors=blue"], parts)

    def test_no_trailing_ampersand(self):
        """Result must never end with '&'."""
        result = _parse_query_parameters({"x": ["a", "b", "c"]})
        self.assertFalse(result.endswith("&"))

    def test_vertex_tuple_single(self):
        """VERTEX parameters: (id, type) → k=id&k.type=type."""
        result = _parse_query_parameters({"v": ("vid1", "Person")})
        parts = result.split("&")
        self.assertIn("v=vid1", parts)
        self.assertIn("v.type=Person", parts)

    def test_set_vertex_list_of_tuples(self):
        """SET<VERTEX>: list of (id, type) tuples → k[i]=id&k[i].type=type."""
        result = _parse_query_parameters(
            {"vs": [("id0", "TypeA"), ("id1", "TypeB")]}
        )
        parts = result.split("&")
        self.assertIn("vs[0]=id0", parts)
        self.assertIn("vs[0].type=TypeA", parts)
        self.assertIn("vs[1]=id1", parts)
        self.assertIn("vs[1].type=TypeB", parts)

    def test_datetime_value(self):
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = _parse_query_parameters({"ts": dt})
        self.assertEqual("ts=2024-01-15%2010%3A30%3A00", result)

    def test_large_list_no_on2_regression(self):
        """Performance sanity: 10 000-element list should complete quickly."""
        params = {"big": list(range(10_000))}
        result = _parse_query_parameters(params)
        parts = result.split("&")
        self.assertEqual(10_000, len(parts))
        self.assertTrue(all(p.startswith("big=") for p in parts))

    def test_empty_params(self):
        self.assertEqual("", _parse_query_parameters({}))


# ---------------------------------------------------------------------------
# _prep_upsert_edge_dataframe
# ---------------------------------------------------------------------------

class TestPrepUpsertEdgeDataframe(unittest.TestCase):
    """Tests for common/edge._prep_upsert_edge_dataframe()"""

    def _make_df(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")
        return pd.DataFrame(
            {
                "src": ["A", "B"],
                "dst": ["X", "Y"],
                "weight": [1.0, 2.0],
            }
        )

    def test_explicit_from_to_ids(self):
        """When from_id and to_id columns are given, they are used as vertex IDs."""
        df = self._make_df()
        result = _prep_upsert_edge_dataframe(df, "src", "dst", None)
        self.assertEqual(2, len(result))
        src_id, dst_id, attrs = result[0]
        self.assertEqual("A", src_id)
        self.assertEqual("X", dst_id)
        self.assertIn("weight", attrs)

    def test_default_from_to_uses_index(self):
        """Empty from_id / to_id defaults must fall back to DataFrame index."""
        df = self._make_df()
        # With default "" arguments the dataframe index (0, 1) should be used.
        result = _prep_upsert_edge_dataframe(df, "", "", None)
        src_id_0, dst_id_0, _ = result[0]
        self.assertEqual(0, src_id_0)
        self.assertEqual(0, dst_id_0)
        src_id_1, dst_id_1, _ = result[1]
        self.assertEqual(1, src_id_1)
        self.assertEqual(1, dst_id_1)

    def test_attribute_projection(self):
        """Only mapped attributes should appear when attributes dict is given."""
        df = self._make_df()
        result = _prep_upsert_edge_dataframe(df, "src", "dst", {"w": "weight"})
        _, _, attrs = result[0]
        self.assertIn("w", attrs)
        self.assertNotIn("weight", attrs)
        self.assertNotIn("src", attrs)

    def test_no_attributes_returns_all_columns(self):
        """When attributes is None, all columns are included."""
        df = self._make_df()
        result = _prep_upsert_edge_dataframe(df, "src", "dst", None)
        _, _, attrs = result[0]
        self.assertIn("src", attrs)
        self.assertIn("dst", attrs)
        self.assertIn("weight", attrs)


if __name__ == "__main__":
    unittest.main()
