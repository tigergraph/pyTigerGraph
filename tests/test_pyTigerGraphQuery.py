import asyncio
import unittest
from datetime import datetime
from time import sleep

from pyTigerGraphUnitTest import make_connection
from pyTigerGraphUnitTestAsync import make_connection as make_async_connection

from pyTigerGraph.common.exception import TigerGraphException


class test_pyTigerGraphQuery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection()
        for i in range(1, 6):
            cls.conn.upsertVertex("vertex4", i, {"a01": i})

    def test_01_getQueries(self):
        # TODO Once pyTigerGraphQuery.getQueries() is available
        pass

    def test_02_getInstalledQueries(self):
        res = self.conn.getInstalledQueries()
        self.assertIn("GET /query/tests/query1", res)
        #self.assertNotIn("GET /query/tests/query2_not_installed", res)
        self.assertIn("GET /query/tests/query3_installed", res)

    def test_03_runInstalledQuery(self):
        res = self.conn.runInstalledQuery("query1")
        self.assertIn("ret", res[0])
        self.assertEqual(15, res[0]["ret"])

        # Shared test values — used identically by both POST and GET sections
        special_string = "test <>\"'`\\/{}[]()<>!@£$%^&*-_=+;:|,.§±~` árvíztűrő tükörfúrógép 👍"
        p01_int      = 7
        p02_uint     = 42
        p03_float    = 1.5    # exact binary fraction avoids FP drift
        p04_double   = 2.5
        p06_bool     = True
        p09_datetime = datetime(2024, 6, 15, 12, 30, 45)
        expected_dt_str = p09_datetime.strftime("%Y-%m-%d %H:%M:%S")
        sent_vertex_ids = ["1", "2", "3"]

        # res[N] mirrors the PRINT order declared in query4_all_param_types:
        #   [0] p01_int  [1] p02_uint  [2] p03_float  [3] p04_double  [4] p05_string
        #   [5] p06_bool  [6] p07_vertex  [7] p08_vertex_vertex4  [8] p09_datetime
        #   [9] p10_set_int  [10] p11_bag_int  [11] p13_set_vertex  [12] p14_set_vertex_vertex4

        def _assert_all(res):
            self.assertIsInstance(res, list)
            self.assertEqual(13, len(res))
            self.assertEqual(res[0]["p01_int"],   p01_int)
            self.assertEqual(res[1]["p02_uint"],  p02_uint)
            self.assertAlmostEqual(res[2]["p03_float"],  p03_float,  places=4)
            self.assertAlmostEqual(res[3]["p04_double"], p04_double, places=4)
            self.assertEqual(res[4]["p05_string"], special_string)
            self.assertEqual(res[5]["p06_bool"],   p06_bool)
            self.assertEqual(str(res[6]["p07_vertex"]),         "1")
            self.assertEqual(str(res[7]["p08_vertex_vertex4"]), "1")
            self.assertEqual(res[8]["p09_datetime"], expected_dt_str)
            self.assertEqual(sorted(res[9]["p10_set_int"]),  [1, 2, 3])
            self.assertEqual(sorted(res[10]["p11_bag_int"]), sorted([1, 2, 3, 2, 3, 3]))
            self.assertEqual(sorted(res[11]["p13_set_vertex"]),         sent_vertex_ids)
            self.assertEqual(sorted(res[12]["p14_set_vertex_vertex4"]), sent_vertex_ids)

        # Vertex parameter conventions (same dict works for both POST and GET):
        #   VERTEX (untyped)         → (id, "type") 2-tuple
        #   VERTEX<T> (typed)        → (id,)        1-tuple
        #   SET<VERTEX> (untyped)    → [(id,"type"), ...] list of 2-tuples
        #   SET<VERTEX<T>> (typed)   → [(id,), ...]       list of 1-tuples
        params = {
            "p01_int":               p01_int,
            "p02_uint":              p02_uint,
            "p03_float":             p03_float,
            "p04_double":            p04_double,
            "p05_string":            special_string,
            "p06_bool":              p06_bool,
            "p07_vertex":            (1, "vertex4"),         # VERTEX (untyped) → 2-tuple
            "p08_vertex_vertex4":    (1,),                   # VERTEX<vertex4>  → 1-tuple
            "p09_datetime":          p09_datetime,
            "p10_set_int":           [1, 2, 3, 2, 3, 3],
            "p11_bag_int":           [1, 2, 3, 2, 3, 3],
            "p13_set_vertex":        [(1, "vertex4"), (2, "vertex4"), (3, "vertex4")],  # SET<VERTEX> → 2-tuples
            "p14_set_vertex_vertex4": [(1,), (2,), (3,)],    # SET<VERTEX<vertex4>> → 1-tuples
        }

        # usePost=True  — serialised as JSON body via _prep_query_parameters_json()
        res_post = self.conn.runInstalledQuery("query4_all_param_types", params, usePost=True)
        _assert_all(res_post)

        # usePost=False — serialised as URL query string via _parse_query_parameters()
        res_get = self.conn.runInstalledQuery("query4_all_param_types", params, usePost=False)
        _assert_all(res_get)

    def test_03b_backward_compat_typed_vertex(self):
        """Backward-compatibility: plain int/str ids for VERTEX<T> params.

        Old callers passed {"typed_vertex_param": id} (plain value) instead of
        the new {"typed_vertex_param": (id,)} 1-tuple convention.  In POST mode
        the server rejects this with REST-30000 "'id' is not found in the VERTEX
        parameter …".  runInstalledQuery() must detect that error and retry
        transparently via GET so existing code continues to work.
        """
        # Old-style backward-compat params: plain int/list-of-ints for typed
        # vertex parameters, tuples only where strictly needed (untyped VERTEX).
        compat_params = {
            "p01_int":               7,
            "p02_uint":              42,
            "p03_float":             1.5,
            "p04_double":            2.5,
            "p05_string":            "compat-test",
            "p06_bool":              True,
            "p07_vertex":            (1, "vertex4"),   # untyped VERTEX — tuple always needed
            "p08_vertex_vertex4":    1,                # VERTEX<vertex4> — old-style plain int
            "p09_datetime":          datetime(2024, 1, 1),
            "p10_set_int":           [1, 2, 3],
            "p11_bag_int":           [1, 2, 3],
            "p13_set_vertex":        [(1, "vertex4"), (2, "vertex4"), (3, "vertex4")],
            "p14_set_vertex_vertex4": [1, 2, 3],       # SET<VERTEX<vertex4>> — old-style plain ints
        }

        # --- Part 1: document the exact raw error from TigerGraph on POST ---
        # Call _req directly (bypassing the retry) to capture the real error.
        import json as _json
        from pyTigerGraph.common.query import _prep_query_parameters_json
        post_body = _prep_query_parameters_json(compat_params)
        query_url = (self.conn.restppUrl + "/query/" + self.conn.graphname
                     + "/query4_all_param_types")
        raw_error = None
        try:
            self.conn._req("POST", query_url, data=post_body,
                           resKey="results", jsonData=True)
        except Exception as e:
            raw_error = e

        self.assertIsNotNone(raw_error,
            "Expected REST-30000 from TigerGraph when plain int used for VERTEX<T> in POST")
        self.assertEqual(getattr(raw_error, "code", None), "REST-30000",
            f"Expected code REST-30000, got: {getattr(raw_error, 'code', None)!r}")
        self.assertIn("'id' is not found in the VERTEX parameter",
            getattr(raw_error, "message", ""),
            f"Unexpected error message: {getattr(raw_error, 'message', '')!r}")

        # --- Part 2: runInstalledQuery() must succeed via the GET retry ---
        # usePost auto-detects POST for a dict; the retry catches the REST-30000
        # and transparently falls back to GET where plain ids are valid.
        res = self.conn.runInstalledQuery("query4_all_param_types", compat_params)
        self.assertIsInstance(res, list)
        self.assertEqual(13, len(res))
        self.assertEqual(res[0]["p01_int"], 7)
        self.assertEqual(str(res[7]["p08_vertex_vertex4"]), "1")
        self.assertEqual(sorted(res[12]["p14_set_vertex_vertex4"]), ["1", "2", "3"])

    def test_03c_invalid_vertex_type_errors(self):
        """Document error behavior for invalid vertex-type arguments.

        Three cases are covered:

        * ``(id, None)`` — rejected by pyTigerGraph before the network call
          (None is not a str).
        * ``(id, "")``   — rejected by pyTigerGraph before the network call
          (empty type string caught by client-side guard).
        * ``(id, "wrong_type")`` — passes client validation; TigerGraph rejects
          in POST (type mismatch, REST-30000) but silently ignores the wrong
          type in GET, returning success using the graph-default vertex type.
        """
        from pyTigerGraph.common.exception import TigerGraphException as TGE

        base_params = {
            "p01_int":    1,
            "p02_uint":   1,
            "p03_float":  1.0,
            "p04_double": 1.0,
            "p05_string": "test",
            "p06_bool":   False,
            "p09_datetime": datetime(2024, 1, 1),
            "p10_set_int": [1],
            "p11_bag_int": [1],
            "p13_set_vertex":        [(1, "vertex4")],
            "p14_set_vertex_vertex4": [(1,)],
        }

        # --- Case 1: (id, None) — client rejects before any network call ---
        params_none_type = dict(base_params,
                                p07_vertex=(1, None),
                                p08_vertex_vertex4=(1,))
        with self.assertRaises(TGE) as ctx:
            self.conn.runInstalledQuery("query4_all_param_types", params_none_type)
        self.assertIn("VERTEX", ctx.exception.message or "",
                      f"Unexpected message for (id, None): {ctx.exception.message!r}")

        # --- Case 2: (id, "") — client rejects before any network call ---
        params_empty_type = dict(base_params,
                                 p07_vertex=(1, ""),
                                 p08_vertex_vertex4=(1,))
        with self.assertRaises(TGE) as ctx:
            self.conn.runInstalledQuery("query4_all_param_types", params_empty_type)
        self.assertIn("empty", ctx.exception.message or "",
                      f"Unexpected message for (id, ''): {ctx.exception.message!r}")

        # --- Case 3: (id, "wrong_type") — server rejects in POST (REST-30000) ---
        # p07_vertex is typed VERTEX (untyped), p08_vertex_vertex4 is VERTEX<vertex4>.
        # Using "vertex5" as the type for p08 causes a mismatch.
        params_wrong_type = dict(base_params,
                                 p07_vertex=(1, "vertex4"),
                                 p08_vertex_vertex4=(1, "vertex5"))  # wrong type for VERTEX<vertex4>

        # POST: TigerGraph rejects with a type-mismatch REST-30000.
        from pyTigerGraph.common.query import _prep_query_parameters_json
        post_body = _prep_query_parameters_json(params_wrong_type)
        query_url = (self.conn.restppUrl + "/query/" + self.conn.graphname
                     + "/query4_all_param_types")
        raw_error = None
        try:
            self.conn._req("POST", query_url, data=post_body,
                           resKey="results", jsonData=True)
        except TGE as e:
            raw_error = e

        self.assertIsNotNone(raw_error,
            "Expected REST-30000 from TigerGraph when wrong vertex type used in POST")
        self.assertEqual(getattr(raw_error, "code", None), "REST-30000",
            f"Expected code REST-30000, got: {getattr(raw_error, 'code', None)!r}")
        self.assertIn("vertex5", getattr(raw_error, "message", ""),
            f"Expected 'vertex5' in error message, got: {getattr(raw_error, 'message', '')!r}")

        # GET: TigerGraph ignores the extra `.type` qualifier for a typed-vertex
        # parameter and processes the id as-is.  The call succeeds.
        from pyTigerGraph.common.query import _parse_query_parameters
        qs = _parse_query_parameters(params_wrong_type)
        res = self.conn._req("GET", query_url + "?" + qs, resKey="results")
        self.assertIsInstance(res, list)
        # p08_vertex_vertex4 result — TigerGraph returns the vertex id it received.
        self.assertEqual(str(res[7]["p08_vertex_vertex4"]), "1")

    def test_04_runInterpretedQuery(self):
        queryText = \
            """INTERPRET QUERY () FOR GRAPH $graphname {
  SumAccum<INT> @@summa;
  start = {vertex4.*};
  res =
    SELECT src
    FROM   start:src
    ACCUM  @@summa += src.a01;
  PRINT @@summa AS ret;
}"""
        res = self.conn.runInterpretedQuery(queryText)
        self.assertIn("ret", res[0])
        self.assertEqual(15, res[0]["ret"])

        queryText = \
            """INTERPRET QUERY () FOR GRAPH @graphname@ {
  SumAccum<INT> @@summa;
  start = {vertex4.*};
  res =
    SELECT src
    FROM   start:src
    ACCUM  @@summa += src.a01;
  PRINT @@summa AS ret;
}"""
        res = self.conn.runInterpretedQuery(queryText)
        self.assertIn("ret", res[0])
        self.assertEqual(15, res[0]["ret"])

    def test_05_runInstalledQueryAsync(self):
        q_id = self.conn.runInstalledQuery("query1", runAsync=True)
        trials = 0
        while trials < 30:
            job = self.conn.checkQueryStatus(q_id)[0]
            if job["status"] == "success":
                break
            sleep(1)
            trials += 1
        res = self.conn.getQueryResult(q_id)
        self.assertIn("ret", res[0])
        self.assertEqual(15, res[0]["ret"])

    def test_06_checkQueryStatus(self):
        q_id = self.conn.runInstalledQuery("query1", runAsync=True)
        print(q_id)
        res = self.conn.checkQueryStatus(q_id)
        self.assertIn("requestid", res[0])
        self.assertEqual(q_id, res[0]["requestid"])

    def test_07_showQuery(self):
        query = self.conn.showQuery("query1").split("\n")[1]
        q1 = """# installed v2"""
        self.assertEqual(q1, query)

    def test_08_getQueryMetadata(self):
        query_md = self.conn.getQueryMetadata("query1")
        self.assertEqual(query_md["output"][0], {"ret": "int"})

    def test_09_getRunningQueries(self):
        rq_id = self.conn.getRunningQueries()["results"]
        self.assertEqual(len(rq_id), 0)

    def test_10_abortQuery(self):
        abort_ret = self.conn.abortQuery("all")
        self.assertEqual(abort_ret["results"], [{'aborted_queries': []}])

    def test_11_queryDescriptions(self):
        version = self.conn.getVer().split('.')
        if version[0] >= "4":  # Query descriptions only supported in Tigergraph versions >= 4.x
            self.conn.dropQueryDescription('query1')
            desc = self.conn.getQueryDescription('query1')
            self.assertEqual(desc, [{'queryName': 'query1', 'parameters': []}])
            self.conn.describeQuery('query1', 'This is a description')
            desc = self.conn.getQueryDescription('query1')
            self.assertEqual(desc[0]['description'], 'This is a description')

            self.conn.dropQueryDescription('query4_all_param_types')
            self.conn.describeQuery('query4_all_param_types', 'this is a query description',
                                    {'p01_int': 'this is a parameter description',
                                     'p02_uint': 'this is a second param desc'})
            desc = self.conn.getQueryDescription('query4_all_param_types')
            self.assertEqual(desc[0]['description'],
                             'this is a query description')
            self.assertEqual(
                desc[0]['parameters'][0]['description'], 'this is a parameter description')
            self.assertEqual(desc[0]['parameters'][1]
                             ['description'], 'this is a second param desc')

        else:
            with self.assertRaises(TigerGraphException) as tge:
                res = self.conn.dropQueryDescription('query1')
            self.assertEqual(
                "This function is only supported on versions of TigerGraph >= 4.0.0.", tge.exception.message)
            with self.assertRaises(TigerGraphException) as tge:
                res = self.conn.describeQuery('query1', 'test')
            self.assertEqual(
                "This function is only supported on versions of TigerGraph >= 4.0.0.", tge.exception.message)
            with self.assertRaises(TigerGraphException) as tge:
                res = self.conn.getQueryDescription('query1')
            self.assertEqual(
                "This function is only supported on versions of TigerGraph >= 4.0.0.", tge.exception.message)

    def test_installQueries(self):
        """Test sync installQueries — returns result dict directly on TG 4.x."""
        graphname = self.conn.graphname
        test_query_names = [
            "test_install_query",
            "test_install_query1",
            "test_install_query2",
        ]
        for qn in test_query_names:
            try:
                self.conn.dropQueries(qn)
            except Exception:
                pass

        queries = [
            'CREATE QUERY test_install_query() { PRINT "Hello World"; }',
            'CREATE QUERY test_install_query1() { PRINT "Hello World 1"; }',
            'CREATE QUERY test_install_query2() { PRINT "Hello World 2"; }',
        ]

        for query in queries:
            self.conn.gsql(f"USE GRAPH {graphname}\n{query}")

        def _check_install_result(result):
            self.assertIsInstance(result, dict)
            self.assertIn("error", result)
            self.assertFalse(result["error"])

        # Test installing a single query
        res = self.conn.installQueries("test_install_query")
        _check_install_result(res)

        # Test installing multiple queries
        res = self.conn.installQueries(["test_install_query1", "test_install_query2"])
        _check_install_result(res)

        # Test installing all queries
        res = self.conn.installQueries("all")
        _check_install_result(res)

    def test_installQueriesAsync(self):
        """Test async installQueries — polls internally and returns status dict with message."""
        async def _run():
            conn = await make_async_connection()
            graphname = conn.graphname

            test_query_names = [
                "test_async_install_query",
                "test_async_install_query_flag",
            ]
            for qn in test_query_names:
                try:
                    await conn.dropQueries(qn)
                except Exception:
                    pass

            queries = [
                'CREATE QUERY test_async_install_query() { PRINT "Async Hello"; }',
                'CREATE QUERY test_async_install_query_flag() { PRINT "Async Flag"; }',
            ]
            for query in queries:
                self.conn.gsql(f"USE GRAPH {graphname}\n{query}")

            def _check(res):
                self.assertIsInstance(res, dict)
                self.assertFalse(res.get("error", True))
                self.assertIn("message", res)

            # Test installing a single query — result is polled until done
            res = await conn.installQueries("test_async_install_query")
            _check(res)

            # Test installing with -force flag
            res = await conn.installQueries("test_async_install_query_flag", flag="-force")
            _check(res)

            # Test installing all with asterisk
            res = await conn.installQueries("*")
            _check(res)

        asyncio.run(_run())

    def test_getQueryContent(self):
        """Test getQueryContent function."""
        # Test getting content of an existing query
        res = self.conn.getQueryContent("query1")
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)

    def test_createQuery(self):
        """Test createQuery function."""
        try:
            self.conn.dropQueries("testCreateQuery")
        except Exception:
            pass

        query_text = """
        CREATE QUERY testCreateQuery() FOR GRAPH $graphname {
            PRINT "Hello World";
        }
        """
        res = self.conn.createQuery(query_text)
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)
        self.assertIn("message", res)

    def test_dropQueries(self):
        """Test dropQueries function."""
        graphname = self.conn.graphname
        query_text = f"""
        CREATE QUERY testDropQuery() FOR GRAPH {graphname} {{
            PRINT "to be dropped";
        }}
        """
        self.conn.createQuery(query_text)

        res = self.conn.dropQueries("testDropQuery")
        self.assertIsInstance(res, dict)

        # Dropping non-existent queries raises an exception on TG >= 4.0
        with self.assertRaises(Exception):
            self.conn.dropQueries(["nonExistentQ1", "nonExistentQ2"])

        # Test invalid input
        with self.assertRaises(Exception):
            self.conn.dropQueries(123)

    def test_listQueryNames(self):
        """Test listQueryNames function."""
        res = self.conn.listQueryNames()
        self.assertIsInstance(res, list)

    def test_checkQuerySemantic(self):
        """Test checkQuerySemantic function."""
        # Test valid query
        valid_query = """
        CREATE QUERY testSemanticQuery() {
            PRINT "Hello World";
        }
        """
        res = self.conn.checkQuerySemantic(valid_query)
        self.assertIsInstance(res, dict)
        self.assertIn("warnings", res)
        self.assertIn("errors", res)

        # Test invalid query
        invalid_query = "INVALID GSQL SYNTAX"
        res = self.conn.checkQuerySemantic(invalid_query)
        self.assertIsInstance(res, dict)
        self.assertIn("warnings", res)
        self.assertIn("errors", res)

    def test_getQueryInfo(self):
        """Test getQueryInfo function."""
        # Test getting info for all queries
        res = self.conn.getQueryInfo()
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)
        self.assertIn("results", res)

        # Test getting info for specific query
        res = self.conn.getQueryInfo(queryName="query1")
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)

        # Test getting info with status filter
        res = self.conn.getQueryInfo(status="VALID")
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)


if __name__ == '__main__':
    unittest.main()
