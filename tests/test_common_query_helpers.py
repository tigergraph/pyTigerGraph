"""Unit tests for pyTigerGraph.common.query helper functions.

Focuses on _encode_str_for_post and _prep_query_parameters_json to ensure
POST query parameters are never silently corrupted.
"""

import unittest
from datetime import datetime

from pyTigerGraph.common.exception import TigerGraphException
from pyTigerGraph.common.query import (
    _encode_str_for_post,
    _parse_query_parameters,
    _prep_query_parameters_json,
)


class TestEncodeStrForPost(unittest.TestCase):
    """_encode_str_for_post: only % → %25, everything else unchanged."""

    def test_no_percent(self):
        self.assertEqual(_encode_str_for_post("hello world"), "hello world")

    def test_single_percent(self):
        self.assertEqual(_encode_str_for_post("50%"), "50%25")

    def test_multiple_percents(self):
        self.assertEqual(_encode_str_for_post("10% and 20%"), "10%25 and 20%25")

    def test_already_encoded_percent(self):
        # %25 in input → %2525 (double-encode is intentional; server decodes once)
        self.assertEqual(_encode_str_for_post("%25"), "%2525")

    def test_empty_string(self):
        self.assertEqual(_encode_str_for_post(""), "")

    def test_no_mutation_of_unrelated_chars(self):
        s = "abc/123?key=val&other=true"
        self.assertEqual(_encode_str_for_post(s), s)


class TestPrepQueryParametersJson(unittest.TestCase):
    """_prep_query_parameters_json: every conversion rule documented in the docstring."""

    # ------------------------------------------------------------------
    # Guard rails
    # ------------------------------------------------------------------

    def test_none_passthrough(self):
        self.assertIsNone(_prep_query_parameters_json(None))

    def test_empty_dict_passthrough(self):
        # empty dict is falsy → returned as-is
        result = _prep_query_parameters_json({})
        self.assertEqual(result, {})

    def test_non_dict_passthrough(self):
        # wrong type returns the value unchanged
        self.assertEqual(_prep_query_parameters_json("raw_string"), "raw_string")

    # ------------------------------------------------------------------
    # Primitive / scalar values — must not be corrupted
    # ------------------------------------------------------------------

    def test_int_unchanged(self):
        result = _prep_query_parameters_json({"limit": 100})
        self.assertEqual(result, {"limit": 100})

    def test_float_unchanged(self):
        result = _prep_query_parameters_json({"score": 3.14})
        self.assertEqual(result, {"score": 3.14})

    def test_bool_unchanged(self):
        result = _prep_query_parameters_json({"flag": True})
        self.assertEqual(result, {"flag": True})

    def test_none_value_unchanged(self):
        result = _prep_query_parameters_json({"x": None})
        self.assertEqual(result, {"x": None})

    # ------------------------------------------------------------------
    # String values
    # ------------------------------------------------------------------

    def test_plain_string_unchanged(self):
        result = _prep_query_parameters_json({"name": "Alice"})
        self.assertEqual(result, {"name": "Alice"})

    def test_string_with_percent_encoded(self):
        result = _prep_query_parameters_json({"pattern": "50%"})
        self.assertEqual(result, {"pattern": "50%25"})

    def test_string_without_percent_not_touched(self):
        result = _prep_query_parameters_json({"key": "hello/world"})
        self.assertEqual(result, {"key": "hello/world"})

    # ------------------------------------------------------------------
    # datetime values
    # ------------------------------------------------------------------

    def test_datetime_converted_to_string(self):
        dt = datetime(2024, 6, 15, 12, 30, 45)
        result = _prep_query_parameters_json({"ts": dt})
        self.assertEqual(result, {"ts": "2024-06-15 12:30:45"})

    def test_datetime_format_is_exact(self):
        dt = datetime(2000, 1, 1, 0, 0, 0)
        result = _prep_query_parameters_json({"ts": dt})
        self.assertEqual(result["ts"], "2000-01-01 00:00:00")

    # ------------------------------------------------------------------
    # Tuple → vertex dict
    # ------------------------------------------------------------------

    def test_tuple_typed_vertex_1tuple(self):
        # VERTEX<T>: (id,)  →  {"id": id}
        result = _prep_query_parameters_json({"v": ("vid123",)})
        self.assertEqual(result, {"v": {"id": "vid123"}})

    def test_tuple_typed_vertex_1tuple_int_id(self):
        result = _prep_query_parameters_json({"v": (42,)})
        self.assertEqual(result, {"v": {"id": 42}})

    def test_tuple_untyped_vertex_2tuple(self):
        # VERTEX (untyped): (id, "type")  →  {"id": id, "type": "type"}
        result = _prep_query_parameters_json({"v": ("vid123", "Person")})
        self.assertEqual(result, {"v": {"id": "vid123", "type": "Person"}})

    def test_tuple_untyped_vertex_integer_id(self):
        result = _prep_query_parameters_json({"v": (42, "Order")})
        self.assertEqual(result, {"v": {"id": 42, "type": "Order"}})

    def test_tuple_invalid_3tuple_raises(self):
        with self.assertRaises(TigerGraphException):
            _prep_query_parameters_json({"v": ("id", "type", "extra")})

    def test_tuple_type_not_string_raises(self):
        # (id, None) — None is not a str → rejected by pyTigerGraph
        with self.assertRaises(TigerGraphException):
            _prep_query_parameters_json({"v": ("id", None)})

    def test_tuple_empty_type_string_raises(self):
        # (id, "") — empty string slips isinstance check but must be rejected
        with self.assertRaises(TigerGraphException):
            _prep_query_parameters_json({"v": (1, "")})

    def test_tuple_empty_type_in_list_raises(self):
        with self.assertRaises(TigerGraphException):
            _prep_query_parameters_json({"vs": [(1, "")]})

    # ------------------------------------------------------------------
    # Pre-formatted vertex dict — must pass through unchanged
    # ------------------------------------------------------------------

    def test_vertex_dict_passthrough(self):
        vertex = {"id": "vid123", "type": "Person"}
        result = _prep_query_parameters_json({"v": vertex})
        self.assertEqual(result["v"], vertex)

    def test_vertex_dict_id_only_passthrough(self):
        # Typed vertex pre-formatted with just "id" (type is optional per docs)
        vertex = {"id": "vid123"}
        result = _prep_query_parameters_json({"v": vertex})
        self.assertEqual(result["v"], vertex)

    # ------------------------------------------------------------------
    # MAP values (Python dict without "id" → TigerGraph keylist/valuelist)
    # ------------------------------------------------------------------

    def test_map_dict_converted_to_keylist_valuelist(self):
        result = _prep_query_parameters_json({"m": {49: "Alaska", 50: "Hawaii"}})
        self.assertEqual(result["m"], {"keylist": [49, 50], "valuelist": ["Alaska", "Hawaii"]})

    def test_map_string_keys(self):
        result = _prep_query_parameters_json({"m": {"a": 1, "b": 2}})
        self.assertEqual(result["m"], {"keylist": ["a", "b"], "valuelist": [1, 2]})

    def test_map_empty_dict_converted(self):
        # An empty dict has no "id" key, so it becomes an empty MAP structure.
        result = _prep_query_parameters_json({"m": {}})
        self.assertEqual(result["m"], {"keylist": [], "valuelist": []})

    def test_map_does_not_mutate_input(self):
        original_map = {"x": 10, "y": 20}
        original = {"m": original_map}
        _prep_query_parameters_json(original)
        self.assertEqual(original["m"], {"x": 10, "y": 20})

    # ------------------------------------------------------------------
    # List values
    # ------------------------------------------------------------------

    def test_list_of_ints_unchanged(self):
        result = _prep_query_parameters_json({"ids": [1, 2, 3]})
        self.assertEqual(result, {"ids": [1, 2, 3]})

    def test_list_of_strings_percent_encoded(self):
        result = _prep_query_parameters_json({"tags": ["50%", "no_percent"]})
        self.assertEqual(result, {"tags": ["50%25", "no_percent"]})

    def test_list_of_1tuples_typed_vertex_set(self):
        # SET<VERTEX<T>>: [(id,), ...]  →  [{"id": id}, ...]
        result = _prep_query_parameters_json({"vs": [("v1",), ("v2",)]})
        self.assertEqual(result, {"vs": [{"id": "v1"}, {"id": "v2"}]})

    def test_list_of_2tuples_untyped_vertex_set(self):
        # SET<VERTEX>: [(id, "type"), ...]  →  [{"id": id, "type": "type"}, ...]
        result = _prep_query_parameters_json({
            "vertices": [("v1", "Person"), ("v2", "Person")]
        })
        self.assertEqual(result, {
            "vertices": [
                {"id": "v1", "type": "Person"},
                {"id": "v2", "type": "Person"},
            ]
        })

    def test_list_of_datetimes_converted(self):
        dts = [datetime(2024, 1, 1), datetime(2024, 6, 15, 8, 0, 0)]
        result = _prep_query_parameters_json({"times": dts})
        self.assertEqual(result, {"times": ["2024-01-01 00:00:00", "2024-06-15 08:00:00"]})

    def test_list_of_dicts_passthrough(self):
        verts = [{"id": "v1", "type": "Person"}, {"id": "v2", "type": "Order"}]
        result = _prep_query_parameters_json({"vset": verts})
        self.assertEqual(result["vset"], verts)

    def test_list_of_invalid_tuples_raises(self):
        with self.assertRaises(TigerGraphException):
            _prep_query_parameters_json({"v": [("id", "type", "extra")]})

    def test_empty_list_unchanged(self):
        result = _prep_query_parameters_json({"items": []})
        self.assertEqual(result, {"items": []})

    # ------------------------------------------------------------------
    # Mixed params — keys must not cross-contaminate each other
    # ------------------------------------------------------------------

    def test_multiple_keys_independent(self):
        dt = datetime(2024, 3, 10, 9, 0, 0)
        result = _prep_query_parameters_json({
            "limit": 10,
            "name": "Bob%",
            "ts": dt,
            "v": ("vid1", "User"),
        })
        self.assertEqual(result["limit"], 10)
        self.assertEqual(result["name"], "Bob%25")
        self.assertEqual(result["ts"], "2024-03-10 09:00:00")
        self.assertEqual(result["v"], {"id": "vid1", "type": "User"})

    def test_original_dict_not_mutated(self):
        original = {"name": "Alice%", "limit": 5}
        _prep_query_parameters_json(original)
        self.assertEqual(original["name"], "Alice%")  # input untouched


class TestParseQueryParameters(unittest.TestCase):
    """_parse_query_parameters: vertex tuple conventions for GET mode."""

    def test_primitive_int(self):
        self.assertEqual(_parse_query_parameters({"n": 5}), "n=5")

    def test_primitive_string(self):
        self.assertEqual(_parse_query_parameters({"s": "hello"}), "s=hello")

    def test_list_of_primitives(self):
        result = _parse_query_parameters({"ids": [1, 2, 3]})
        self.assertEqual(result, "ids=1&ids=2&ids=3")

    def test_typed_vertex_1tuple(self):
        # VERTEX<T>: (id,)  →  k=id
        result = _parse_query_parameters({"v": ("Tom",)})
        self.assertEqual(result, "v=Tom")

    def test_typed_vertex_1tuple_int_id(self):
        result = _parse_query_parameters({"v": (42,)})
        self.assertEqual(result, "v=42")

    def test_untyped_vertex_2tuple(self):
        # VERTEX (untyped): (id, "type")  →  k=id&k.type=type
        result = _parse_query_parameters({"v": ("Tom", "Person")})
        self.assertEqual(result, "v=Tom&v.type=Person")

    def test_typed_vertex_set_list_of_1tuples(self):
        # SET<VERTEX<T>>: [(id,), ...]  →  k=id1&k=id2  (repeated, no index)
        result = _parse_query_parameters({"vs": [("Tom",), ("Mary",)]})
        self.assertEqual(result, "vs=Tom&vs=Mary")

    def test_untyped_vertex_set_list_of_2tuples(self):
        # SET<VERTEX>: [(id,"type"), ...]  →  k[i]=id&k[i].type=type
        result = _parse_query_parameters({"vs": [("Tom", "Person"), ("Mary", "Person")]})
        self.assertEqual(result, "vs[0]=Tom&vs[0].type=Person&vs[1]=Mary&vs[1].type=Person")

    def test_invalid_tuple_raises(self):
        with self.assertRaises(TigerGraphException):
            _parse_query_parameters({"v": ("id", "type", "extra")})

    def test_invalid_tuple_in_list_raises(self):
        with self.assertRaises(TigerGraphException):
            _parse_query_parameters({"vs": [("id", "type", "extra")]})

    def test_tuple_none_type_raises(self):
        # (id, None) — None is not a str → rejected by pyTigerGraph
        with self.assertRaises(TigerGraphException):
            _parse_query_parameters({"v": (1, None)})

    def test_tuple_empty_type_string_raises(self):
        # (id, "") — empty string must be rejected before sending to TigerGraph
        with self.assertRaises(TigerGraphException):
            _parse_query_parameters({"v": (1, "")})

    def test_tuple_empty_type_in_list_raises(self):
        with self.assertRaises(TigerGraphException):
            _parse_query_parameters({"vs": [(1, "")]})

    def test_datetime(self):
        result = _parse_query_parameters({"ts": datetime(2024, 1, 15, 12, 0, 0)})
        self.assertIn("2024-01-15", result)


class TestPostParamRoundTrip(unittest.TestCase):
    """Integration tests: verify every _prep_query_parameters_json conversion
    survives a real POST round-trip through TigerGraph.

    Uses the pre-installed ``query4_all_param_types`` query (defined in
    testserver.gsql) which PRINTs all 13 parameters back in declaration order.
    No query creation or teardown required.

    PRINT order → result index:
      p01_int[0]  p02_uint[1]  p03_float[2]  p04_double[3]  p05_string[4]
      p06_bool[5]  p07_vertex[6]  p08_vertex_vertex4[7]  p09_datetime[8]
      p10_set_int[9]  p11_bag_int[10]  p13_set_vertex[11]  p14_set_vertex_vertex4[12]
    """

    # Neutral baseline — every test overrides only the param(s) it cares about.
    # (id, "type") tuples are used uniformly for all vertex params; the API
    # converts them to the correct wire format for each transport.
    _BASE = {
        "p01_int":              1,
        "p02_uint":             1,
        "p03_float":            1.0,
        "p04_double":           1.0,
        "p05_string":           "x",
        "p06_bool":             True,
        "p07_vertex":           (1, "vertex4"),
        "p08_vertex_vertex4":   (1, "vertex4"),
        "p09_datetime":         datetime(2000, 1, 1),
        "p10_set_int":          [1],
        "p11_bag_int":          [1],
        "p13_set_vertex":       [(1, "vertex4")],
        "p14_set_vertex_vertex4": [(1, "vertex4")],
    }

    @classmethod
    def setUpClass(cls):
        try:
            from pyTigerGraphUnitTest import make_connection
        except ImportError:
            raise unittest.SkipTest("No test server configuration found")

        try:
            cls.conn = make_connection()
        except Exception as e:
            raise unittest.SkipTest(f"Cannot connect to test server: {e}")

        # Ensure vertex4 instances 1-3 exist (other suites may not have run yet)
        for i in range(1, 4):
            cls.conn.upsertVertex("vertex4", i, {"a01": i})

    def _run(self, overrides: dict):
        """Merge overrides into the baseline and run query4_all_param_types via POST."""
        params = {**self._BASE, **overrides}
        return self.conn.runInstalledQuery("query4_all_param_types", params, usePost=True)

    # ------------------------------------------------------------------
    # Scalar types
    # ------------------------------------------------------------------

    def test_db_int_roundtrip(self):
        """INT is left as-is by _prep_query_parameters_json; DB must echo it back exactly."""
        p = 42
        res = self._run({"p01_int": p})
        self.assertEqual(res[0]["p01_int"], p)

    def test_db_uint_roundtrip(self):
        p = 7
        res = self._run({"p02_uint": p})
        self.assertEqual(res[1]["p02_uint"], p)

    def test_db_float_roundtrip(self):
        p = 1.5
        res = self._run({"p03_float": p})
        self.assertAlmostEqual(res[2]["p03_float"], p, places=4)

    def test_db_double_roundtrip(self):
        p = 2.5
        res = self._run({"p04_double": p})
        self.assertAlmostEqual(res[3]["p04_double"], p, places=4)

    def test_db_bool_roundtrip(self):
        res = self._run({"p06_bool": False})
        self.assertEqual(res[5]["p06_bool"], False)

    # ------------------------------------------------------------------
    # String — the % encoding path is the critical one
    # ------------------------------------------------------------------

    def test_db_plain_string_roundtrip(self):
        """Plain string (no %) must arrive at the DB unchanged."""
        p = "hello world"
        res = self._run({"p05_string": p})
        self.assertEqual(res[4]["p05_string"], p)

    def test_db_percent_string_roundtrip(self):
        """String containing % must be decoded back to % by the server (not stored as %25)."""
        p = "50% done"
        res = self._run({"p05_string": p})
        self.assertEqual(res[4]["p05_string"], p)

    def test_db_special_chars_string_roundtrip(self):
        """Unicode, symbols, and emoji survive the full round-trip unchanged."""
        p = "test <>\"'`\\/{}[]!@£$%^&*-_=+;:|,.§±~` árvíztűrő 👍"
        res = self._run({"p05_string": p})
        self.assertEqual(res[4]["p05_string"], p)

    # ------------------------------------------------------------------
    # datetime
    # ------------------------------------------------------------------

    def test_db_datetime_roundtrip(self):
        """datetime is converted to 'YYYY-MM-DD HH:MM:SS' and echoed back by the DB."""
        p = datetime(2024, 6, 15, 12, 30, 45)
        res = self._run({"p09_datetime": p})
        self.assertEqual(res[8]["p09_datetime"], p.strftime("%Y-%m-%d %H:%M:%S"))

    def test_db_datetime_midnight_roundtrip(self):
        """Midnight is formatted with explicit 00:00:00 (no truncation)."""
        p = datetime(2000, 1, 1, 0, 0, 0)
        res = self._run({"p09_datetime": p})
        self.assertEqual(res[8]["p09_datetime"], "2000-01-01 00:00:00")

    # ------------------------------------------------------------------
    # Vertex (untyped and typed)
    # ------------------------------------------------------------------

    def test_db_vertex_tuple_roundtrip(self):
        """Tuple (id, type) → vertex JSON; DB echoes back the primary ID."""
        res = self._run({"p07_vertex": (2, "vertex4")})
        self.assertEqual(str(res[6]["p07_vertex"]), "2")

    def test_db_typed_vertex_tuple_roundtrip(self):
        """Tuple (id, type) for a typed vertex; DB echoes back the primary ID."""
        res = self._run({"p08_vertex_vertex4": (3, "vertex4")})
        self.assertEqual(str(res[7]["p08_vertex_vertex4"]), "3")

    # ------------------------------------------------------------------
    # SET / BAG of scalars
    # ------------------------------------------------------------------

    def test_db_set_int_deduplicates(self):
        """SET<INT>: duplicates are removed; remaining values survive unchanged."""
        sent = [1, 2, 3, 2, 3, 3]
        res = self._run({"p10_set_int": sent})
        self.assertEqual(sorted(res[9]["p10_set_int"]), sorted(set(sent)))

    def test_db_bag_int_preserves_duplicates(self):
        """BAG<INT>: all values including duplicates must be echoed back."""
        sent = [1, 2, 3, 2, 3, 3]
        res = self._run({"p11_bag_int": sent})
        self.assertEqual(sorted(res[10]["p11_bag_int"]), sorted(sent))

    # ------------------------------------------------------------------
    # SET of vertices (untyped and typed)
    # ------------------------------------------------------------------

    def test_db_set_vertex_tuples_roundtrip(self):
        """List of (id, type) tuples → vertex JSON list; DB echoes the IDs back."""
        sent_ids = [1, 2, 3]
        res = self._run({"p13_set_vertex": [(i, "vertex4") for i in sent_ids]})
        returned = sorted(str(v) for v in res[11]["p13_set_vertex"])
        self.assertEqual(returned, [str(i) for i in sorted(sent_ids)])

    def test_db_set_typed_vertex_tuples_roundtrip(self):
        """List of (id, type) tuples for a typed vertex set; DB echoes IDs back."""
        sent_ids = [1, 2, 3]
        res = self._run({"p14_set_vertex_vertex4": [(i, "vertex4") for i in sent_ids]})
        returned = sorted(str(v) for v in res[12]["p14_set_vertex_vertex4"])
        self.assertEqual(returned, [str(i) for i in sorted(sent_ids)])

    # ------------------------------------------------------------------
    # All conversions in one call
    # ------------------------------------------------------------------

    def test_db_all_conversions_together(self):
        """All converted types sent in one call; each must round-trip correctly."""
        p_str    = "combined 100% test"
        p_dt     = datetime(2024, 12, 31, 23, 59, 59)
        sent_ids = [1, 2, 3]

        res = self._run({
            "p01_int":              99,
            "p05_string":           p_str,
            "p09_datetime":         p_dt,
            "p07_vertex":           (1, "vertex4"),
            "p13_set_vertex":       [(i, "vertex4") for i in sent_ids],
            "p14_set_vertex_vertex4": [(i, "vertex4") for i in sent_ids],
        })

        self.assertEqual(res[0]["p01_int"],    99)
        self.assertEqual(res[4]["p05_string"], p_str)
        self.assertEqual(res[8]["p09_datetime"], p_dt.strftime("%Y-%m-%d %H:%M:%S"))
        self.assertEqual(str(res[6]["p07_vertex"]), "1")
        self.assertEqual(sorted(str(v) for v in res[11]["p13_set_vertex"]),
                         [str(i) for i in sorted(sent_ids)])
        self.assertEqual(sorted(str(v) for v in res[12]["p14_set_vertex_vertex4"]),
                         [str(i) for i in sorted(sent_ids)])


if __name__ == "__main__":
    unittest.main()
