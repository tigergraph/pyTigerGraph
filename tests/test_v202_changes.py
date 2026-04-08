"""Unit tests for pyTigerGraph 2.0.2 changes.

These tests run without a live TigerGraph server by mocking HTTP calls.
They cover:
  - createGraph() with vertexTypes/edgeTypes
  - Boolean query parameter conversion (upsertEdge, upsertEdges, getGSQLVersion, rebuildGraph)
  - dropVertices() graph fallback
  - dropAllDataSources() graphname fallback
  - getVectorIndexStatus() URL construction
  - previewSampleData() graph validation
  - Schema change job APIs
  - runSchemaChange() with force parameter
  - useGraph() / useGlobal() scope control
  - Reserved keyword helpers
  - installQueries() wait parameter
"""

import json
import unittest
from unittest.mock import MagicMock, patch, call

from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.common.exception import TigerGraphException


def _make_conn(graphname="testgraph", **kwargs):
    """Create a TigerGraphConnection without network calls."""
    defaults = dict(
        host="http://127.0.0.1",
        graphname=graphname,
        username="tigergraph",
        password="tigergraph",
    )
    defaults.update(kwargs)
    with patch.object(TigerGraphConnection, "_verify_jwt_token_support", return_value=None):
        conn = TigerGraphConnection(**defaults)
    return conn


def _make_conn_v4(graphname="testgraph", **kwargs):
    """Create a connection that reports TigerGraph >= 4.0."""
    conn = _make_conn(graphname=graphname, **kwargs)
    conn._version_greater_than_4_0 = MagicMock(return_value=True)
    return conn


def _make_conn_v3(graphname="testgraph", **kwargs):
    """Create a connection that reports TigerGraph < 4.0."""
    conn = _make_conn(graphname=graphname, **kwargs)
    conn._version_greater_than_4_0 = MagicMock(return_value=False)
    return conn


# ──────────────────────────────────────────────────────────────────────
# createGraph() with vertexTypes / edgeTypes
# ──────────────────────────────────────────────────────────────────────

class TestCreateGraphWithTypes(unittest.TestCase):

    def test_create_graph_no_types_v4(self):
        conn = _make_conn_v4()
        with patch.object(conn, "_post", return_value={"error": False, "message": "ok"}) as mock:
            conn.createGraph("myGraph")
        args, kwargs = mock.call_args
        self.assertIn("/gsql/v1/schema/graphs", args[0])
        sent_data = kwargs.get("data") or args[1] if len(args) > 1 else kwargs["data"]
        self.assertEqual(sent_data["gsql"], "CREATE GRAPH myGraph()")

    def test_create_graph_with_vertex_types_v4(self):
        conn = _make_conn_v4()
        with patch.object(conn, "_post", return_value={"error": False, "message": "ok"}) as mock:
            conn.createGraph("myGraph", vertexTypes=["Person", "Company"])
        sent_data = mock.call_args[1].get("data") or mock.call_args[0][1]
        self.assertEqual(sent_data["gsql"], "CREATE GRAPH myGraph(Person, Company)")

    def test_create_graph_with_edge_types_v4(self):
        conn = _make_conn_v4()
        with patch.object(conn, "_post", return_value={"error": False, "message": "ok"}) as mock:
            conn.createGraph("myGraph", edgeTypes=["Knows", "WorksAt"])
        sent_data = mock.call_args[1].get("data") or mock.call_args[0][1]
        self.assertEqual(sent_data["gsql"], "CREATE GRAPH myGraph(Knows, WorksAt)")

    def test_create_graph_with_both_types_v4(self):
        conn = _make_conn_v4()
        with patch.object(conn, "_post", return_value={"error": False, "message": "ok"}) as mock:
            conn.createGraph("myGraph", vertexTypes=["Person"], edgeTypes=["Knows"])
        sent_data = mock.call_args[1].get("data") or mock.call_args[0][1]
        self.assertEqual(sent_data["gsql"], "CREATE GRAPH myGraph(Person, Knows)")

    def test_create_graph_with_wildcard_v4(self):
        conn = _make_conn_v4()
        with patch.object(conn, "_post", return_value={"error": False, "message": "ok"}) as mock:
            conn.createGraph("myGraph", vertexTypes=["*"])
        sent_data = mock.call_args[1].get("data") or mock.call_args[0][1]
        self.assertEqual(sent_data["gsql"], "CREATE GRAPH myGraph(*)")

    def test_create_graph_v4_params(self):
        conn = _make_conn_v4()
        with patch.object(conn, "_post", return_value={"error": False, "message": "ok"}) as mock:
            conn.createGraph("myGraph")
        _, kwargs = mock.call_args
        self.assertEqual(kwargs["params"]["gsql"], "true")
        self.assertEqual(kwargs["params"]["graphName"], "myGraph")

    def test_create_graph_v3_fallback(self):
        conn = _make_conn_v3()
        with patch.object(conn, "gsql", return_value="Graph myGraph created.") as mock:
            result = conn.createGraph("myGraph", vertexTypes=["Person"], edgeTypes=["Knows"])
        mock.assert_called_once_with("CREATE GRAPH myGraph(Person, Knows)")
        self.assertFalse(result["error"])

    def test_create_graph_v3_no_types(self):
        conn = _make_conn_v3()
        with patch.object(conn, "gsql", return_value="Graph myGraph created.") as mock:
            conn.createGraph("myGraph")
        mock.assert_called_once_with("CREATE GRAPH myGraph()")


# ──────────────────────────────────────────────────────────────────────
# Boolean query parameter conversion
# ──────────────────────────────────────────────────────────────────────

class TestBooleanParamConversion(unittest.TestCase):

    def test_upsert_edge_vertex_must_exist_true(self):
        conn = _make_conn_v4()
        with patch.object(conn, "_post", return_value=[{"accepted_edges": 1}]) as mock:
            conn.upsertEdge("Person", "1", "Knows", "Person", "2", vertexMustExist=True)
        _, kwargs = mock.call_args
        self.assertEqual(kwargs.get("params", {}).get("vertex_must_exist"), "true")

    def test_upsert_edge_vertex_must_exist_false(self):
        conn = _make_conn_v4()
        with patch.object(conn, "_post", return_value=[{"accepted_edges": 1}]) as mock:
            conn.upsertEdge("Person", "1", "Knows", "Person", "2", vertexMustExist=False)
        _, kwargs = mock.call_args
        self.assertEqual(kwargs.get("params", {}).get("vertex_must_exist"), "false")

    def test_upsert_edges_vertex_must_exist(self):
        conn = _make_conn_v4()
        edges = [("1", "2", {})]
        with patch.object(conn, "_post", return_value=[{"accepted_edges": 1}]) as mock:
            conn.upsertEdges("Person", "Knows", "Person", edges, vertexMustExist=True)
        _, kwargs = mock.call_args
        self.assertEqual(kwargs.get("params", {}).get("vertex_must_exist"), "true")

    def test_get_gsql_version_verbose(self):
        conn = _make_conn_v4()
        with patch.object(conn, "_get", return_value={"version": "4.2"}) as mock:
            conn.getGSQLVersion(verbose=True)
        _, kwargs = mock.call_args
        self.assertEqual(kwargs.get("params", {}).get("verbose"), "true")

    def test_rebuild_graph_force(self):
        conn = _make_conn_v4()
        with patch.object(conn, "_get", return_value={"error": False, "message": "ok"}) as mock:
            conn.rebuildGraph(force=True)
        _, kwargs = mock.call_args
        params = kwargs.get("params", {})
        self.assertEqual(params.get("force"), "true")

    def test_rebuild_graph_force_false_omitted(self):
        """When force=False (default), the param should not be included."""
        conn = _make_conn_v4()
        with patch.object(conn, "_get", return_value={"error": False, "message": "ok"}) as mock:
            conn.rebuildGraph(force=False)
        _, kwargs = mock.call_args
        params = kwargs.get("params", {})
        self.assertNotIn("force", params)


# ──────────────────────────────────────────────────────────────────────
# dropVertices() graph fallback
# ──────────────────────────────────────────────────────────────────────

class TestDropVerticesGraphFallback(unittest.TestCase):

    def test_explicit_graph_param(self):
        conn = _make_conn_v4()
        with patch.object(conn, "_delete", return_value={"error": False, "message": "ok"}) as mock:
            conn.dropVertices("MyVertex", graph="explicitGraph")
        _, kwargs = mock.call_args
        self.assertEqual(kwargs["params"]["graph"], "explicitGraph")

    def test_fallback_to_self_graphname(self):
        conn = _make_conn_v4(graphname="defaultGraph")
        with patch.object(conn, "_delete", return_value={"error": False, "message": "ok"}) as mock:
            conn.dropVertices("MyVertex")
        _, kwargs = mock.call_args
        self.assertEqual(kwargs["params"]["graph"], "defaultGraph")

    def test_no_graph_omits_param(self):
        conn = _make_conn_v4(graphname="")
        with patch.object(conn, "_delete", return_value={"error": False, "message": "ok"}) as mock:
            conn.dropVertices("MyVertex")
        _, kwargs = mock.call_args
        self.assertNotIn("graph", kwargs["params"])

    def test_list_of_vertex_names(self):
        conn = _make_conn_v4()
        with patch.object(conn, "_delete", return_value={"error": False, "message": "ok"}) as mock:
            conn.dropVertices(["V1", "V2", "V3"])
        _, kwargs = mock.call_args
        self.assertEqual(kwargs["params"]["vertex"], "V1,V2,V3")

    def test_empty_list_raises(self):
        conn = _make_conn_v4()
        with self.assertRaises(TigerGraphException):
            conn.dropVertices([])

    def test_invalid_type_raises(self):
        conn = _make_conn_v4()
        with self.assertRaises(TigerGraphException):
            conn.dropVertices(123)

    def test_v3_raises(self):
        conn = _make_conn_v3()
        with self.assertRaises(TigerGraphException):
            conn.dropVertices("MyVertex")

    def test_ignore_errors_retry_individually(self):
        conn = _make_conn_v4()
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("batch fail")
            vertex = kwargs.get("params", {}).get("vertex", "")
            if vertex == "V2":
                raise Exception("not found")
            return {"error": False, "message": "ok"}

        with patch.object(conn, "_delete", side_effect=side_effect):
            result = conn.dropVertices(["V1", "V2"], ignoreErrors=True)
        self.assertIn("V1", result["message"])
        self.assertIn("V2", result["message"])


# ──────────────────────────────────────────────────────────────────────
# dropAllDataSources() graphname fallback
# ──────────────────────────────────────────────────────────────────────

class TestDropAllDataSourcesGraphFallback(unittest.TestCase):

    def test_uses_explicit_graphname(self):
        conn = _make_conn_v4(graphname="defaultG")
        with patch.object(conn, "_req", return_value={"message": "ok"}) as mock:
            conn.dropAllDataSources(graphName="explicitG")
        url = mock.call_args[0][1]
        self.assertIn("graph=explicitG", url)

    def test_fallback_to_self_graphname(self):
        conn = _make_conn_v4(graphname="defaultG")
        with patch.object(conn, "_req", return_value={"message": "ok"}) as mock:
            conn.dropAllDataSources()
        url = mock.call_args[0][1]
        self.assertIn("graph=defaultG", url)

    def test_no_graph_no_query_param(self):
        conn = _make_conn_v4(graphname="")
        with patch.object(conn, "_req", return_value={"message": "ok"}) as mock:
            conn.dropAllDataSources()
        url = mock.call_args[0][1]
        self.assertNotIn("graph=", url)
        self.assertIn("/data-sources/dropAll", url)


# ──────────────────────────────────────────────────────────────────────
# getVectorIndexStatus() URL construction
# ──────────────────────────────────────────────────────────────────────

class TestGetVectorIndexStatus(unittest.TestCase):

    def test_with_all_params(self):
        conn = _make_conn_v4()
        with patch.object(conn, "_req", return_value={"status": "ready"}) as mock:
            conn.getVectorIndexStatus(graphName="g", vertexType="Person", vectorName="emb")
        url = mock.call_args[0][1]
        self.assertTrue(url.endswith("/vector/status/g/Person/emb"))

    def test_with_graph_only(self):
        conn = _make_conn_v4()
        with patch.object(conn, "_req", return_value={"status": "ready"}) as mock:
            conn.getVectorIndexStatus(graphName="g")
        url = mock.call_args[0][1]
        self.assertTrue(url.endswith("/vector/status/g"))

    def test_fallback_to_self_graphname(self):
        conn = _make_conn_v4(graphname="defaultG")
        with patch.object(conn, "_req", return_value={"status": "ready"}) as mock:
            conn.getVectorIndexStatus()
        url = mock.call_args[0][1]
        self.assertTrue(url.endswith("/vector/status/defaultG"))

    def test_no_graph_global_scope(self):
        """Without a graph, the URL should be /vector/status (no trailing graph segment)."""
        conn = _make_conn_v4(graphname="")
        with patch.object(conn, "_req", return_value={"status": "ready"}) as mock:
            conn.getVectorIndexStatus()
        url = mock.call_args[0][1]
        self.assertTrue(url.endswith("/vector/status"))
        self.assertNotIn("/vector/status/", url)

    def test_vertex_type_ignored_without_graph(self):
        """vertexType requires a graph segment; without graph, it's silently ignored."""
        conn = _make_conn_v4(graphname="")
        with patch.object(conn, "_req", return_value={"status": "ready"}) as mock:
            conn.getVectorIndexStatus(vertexType="Person")
        url = mock.call_args[0][1]
        self.assertNotIn("Person", url)


# ──────────────────────────────────────────────────────────────────────
# previewSampleData() graph validation
# ──────────────────────────────────────────────────────────────────────

class TestPreviewSampleData(unittest.TestCase):

    def test_raises_without_graph(self):
        conn = _make_conn_v4(graphname="")
        with self.assertRaises(TigerGraphException) as ctx:
            conn.previewSampleData("ds1", "/path/to/file.csv")
        self.assertIn("graph name", str(ctx.exception).lower())

    def test_raises_on_v3(self):
        conn = _make_conn_v3()
        with self.assertRaises(NotImplementedError):
            conn.previewSampleData("ds1", "/path/to/file.csv")

    def test_uses_explicit_graph(self):
        conn = _make_conn_v4(graphname="defaultG")
        with patch.object(conn, "_req", return_value={"results": []}) as mock:
            conn.previewSampleData("ds1", "/file.csv", graphName="explicitG")
        data = mock.call_args[1].get("data") or mock.call_args[0][2]
        self.assertEqual(data["graphName"], "explicitG")

    def test_fallback_to_self_graphname(self):
        conn = _make_conn_v4(graphname="defaultG")
        with patch.object(conn, "_req", return_value={"results": []}) as mock:
            conn.previewSampleData("ds1", "/file.csv")
        data = mock.call_args[1].get("data") or mock.call_args[0][2]
        self.assertEqual(data["graphName"], "defaultG")


# ──────────────────────────────────────────────────────────────────────
# Schema Change Job APIs
# ──────────────────────────────────────────────────────────────────────

class TestSchemaChangeJobAPIs(unittest.TestCase):

    def test_create_schema_change_job_gsql(self):
        conn = _make_conn_v4(graphname="g1")
        with patch.object(conn, "_post", return_value={"error": False, "message": "ok"}) as mock:
            conn.createSchemaChangeJob("job1", "ADD VERTEX V1 (PRIMARY_ID id UINT);")
        url = mock.call_args[0][0]
        self.assertIn("/gsql/v1/schema/jobs/job1", url)
        kwargs = mock.call_args[1]
        self.assertEqual(kwargs["params"]["gsql"], "true")
        self.assertEqual(kwargs["params"]["graph"], "g1")

    def test_create_schema_change_job_gsql_list(self):
        conn = _make_conn_v4(graphname="g1")
        stmts = [
            "ADD VERTEX V1 (PRIMARY_ID id UINT)",
            "ADD VERTEX V2 (PRIMARY_ID id UINT)"
        ]
        with patch.object(conn, "_post", return_value={"error": False, "message": "ok"}) as mock:
            conn.createSchemaChangeJob("job1", stmts)
        sent_data = json.loads(mock.call_args[1].get("data", "{}"))
        self.assertIn("ADD VERTEX V1", sent_data["gsql"])
        self.assertIn("ADD VERTEX V2", sent_data["gsql"])

    def test_create_schema_change_job_global(self):
        conn = _make_conn_v4(graphname="")
        with patch.object(conn, "_post", return_value={"error": False, "message": "ok"}) as mock:
            conn.createSchemaChangeJob("job1", "ADD VERTEX V1 (PRIMARY_ID id UINT);")
        kwargs = mock.call_args[1]
        self.assertEqual(kwargs["params"]["gsql"], "true")
        self.assertEqual(kwargs["params"]["type"], "global")
        sent_data = json.loads(kwargs.get("data", "{}"))
        self.assertIn("CREATE GLOBAL SCHEMA_CHANGE JOB", sent_data["gsql"])

    def test_create_schema_change_job_json(self):
        conn = _make_conn_v4(graphname="g1")
        json_body = {"some": "config"}
        with patch.object(conn, "_post", return_value={"error": False, "message": "ok"}) as mock:
            conn.createSchemaChangeJob("job1", json_body)
        kwargs = mock.call_args[1]
        self.assertEqual(kwargs["params"]["graph"], "g1")
        self.assertNotIn("gsql", kwargs["params"])

    def test_get_schema_change_jobs_single(self):
        conn = _make_conn_v4(graphname="g1")
        with patch.object(conn, "_get", return_value={"jobs": []}) as mock:
            conn.getSchemaChangeJobs(jobName="job1")
        url = mock.call_args[0][0]
        self.assertIn("/gsql/v1/schema/jobs/job1", url)
        kwargs = mock.call_args[1]
        self.assertEqual(kwargs["params"]["graph"], "g1")
        self.assertEqual(kwargs["params"]["json"], "true")

    def test_get_schema_change_jobs_all(self):
        conn = _make_conn_v4(graphname="g1")
        with patch.object(conn, "_get", return_value={"jobs": []}) as mock:
            conn.getSchemaChangeJobs()
        url = mock.call_args[0][0]
        self.assertTrue(url.endswith("/gsql/v1/schema/jobs"))

    def test_run_schema_change_job(self):
        conn = _make_conn_v4(graphname="g1")
        with patch.object(conn, "_put", return_value={"error": False, "message": "ok"}) as mock:
            conn.runSchemaChangeJob("job1")
        url = mock.call_args[0][0]
        self.assertIn("/gsql/v1/schema/jobs/job1", url)
        kwargs = mock.call_args[1]
        self.assertEqual(kwargs["params"]["graph"], "g1")

    def test_run_schema_change_job_with_force(self):
        conn = _make_conn_v4(graphname="g1")
        with patch.object(conn, "_put", return_value={"error": False, "message": "ok"}) as mock:
            conn.runSchemaChangeJob("job1", force=True)
        kwargs = mock.call_args[1]
        self.assertEqual(kwargs["params"]["force"], "true")
        self.assertEqual(kwargs["params"]["graph"], "g1")

    def test_drop_schema_change_jobs_single(self):
        conn = _make_conn_v4(graphname="g1")
        with patch.object(conn, "_delete", return_value={"error": False, "message": "ok"}) as mock:
            conn.dropSchemaChangeJobs("job1")
        kwargs = mock.call_args[1]
        self.assertEqual(kwargs["params"]["jobName"], "job1")
        self.assertEqual(kwargs["params"]["graph"], "g1")

    def test_drop_schema_change_jobs_list(self):
        conn = _make_conn_v4(graphname="g1")
        with patch.object(conn, "_delete", return_value={"error": False, "message": "ok"}) as mock:
            conn.dropSchemaChangeJobs(["job1", "job2"])
        kwargs = mock.call_args[1]
        self.assertEqual(kwargs["params"]["jobName"], "job1,job2")

    def test_drop_schema_change_jobs_explicit_graph(self):
        conn = _make_conn_v4(graphname="g1")
        with patch.object(conn, "_delete", return_value={"error": False, "message": "ok"}) as mock:
            conn.dropSchemaChangeJobs("job1", graphName="otherGraph")
        kwargs = mock.call_args[1]
        self.assertEqual(kwargs["params"]["graph"], "otherGraph")


# ──────────────────────────────────────────────────────────────────────
# runSchemaChange() with force parameter
# ──────────────────────────────────────────────────────────────────────

class TestRunSchemaChangeForce(unittest.TestCase):

    def test_json_path_with_force(self):
        conn = _make_conn_v4(graphname="g1")
        with patch.object(conn, "_post", return_value={"error": False, "message": "ok"}) as mock:
            conn.runSchemaChange({"schema": "change"}, force=True)
        kwargs = mock.call_args[1]
        self.assertEqual(kwargs["params"]["force"], "true")
        self.assertEqual(kwargs["params"]["graph"], "g1")

    def test_json_path_without_force(self):
        conn = _make_conn_v4(graphname="g1")
        with patch.object(conn, "_post", return_value={"error": False, "message": "ok"}) as mock:
            conn.runSchemaChange({"schema": "change"}, force=False)
        kwargs = mock.call_args[1]
        self.assertNotIn("force", kwargs["params"])

    def test_json_path_raises_on_v3(self):
        conn = _make_conn_v3()
        with self.assertRaises(TigerGraphException):
            conn.runSchemaChange({"schema": "change"})

    def test_gsql_string_path(self):
        conn = _make_conn_v4(graphname="g1")
        with patch.object(conn, "gsql", return_value="ok") as mock:
            conn.runSchemaChange("ADD VERTEX V1 (PRIMARY_ID id UINT);")
        gsql_cmd = mock.call_args[0][0]
        self.assertIn("USE GRAPH g1", gsql_cmd)
        self.assertIn("CREATE SCHEMA_CHANGE JOB", gsql_cmd)
        self.assertIn("RUN SCHEMA_CHANGE JOB", gsql_cmd)
        self.assertIn("DROP JOB", gsql_cmd)

    def test_gsql_list_path(self):
        conn = _make_conn_v4(graphname="g1")
        stmts = ["ADD VERTEX V1 (PRIMARY_ID id UINT)", "ADD VERTEX V2 (PRIMARY_ID id UINT)"]
        with patch.object(conn, "gsql", return_value="ok") as mock:
            conn.runSchemaChange(stmts)
        gsql_cmd = mock.call_args[0][0]
        self.assertIn("ADD VERTEX V1", gsql_cmd)
        self.assertIn("ADD VERTEX V2", gsql_cmd)

    def test_gsql_global_scope(self):
        conn = _make_conn_v4(graphname="")
        with patch.object(conn, "gsql", return_value="ok") as mock:
            conn.runSchemaChange("ADD VERTEX V1 (PRIMARY_ID id UINT);")
        gsql_cmd = mock.call_args[0][0]
        self.assertIn("CREATE GLOBAL SCHEMA_CHANGE JOB", gsql_cmd)
        self.assertIn("RUN GLOBAL SCHEMA_CHANGE JOB", gsql_cmd)
        self.assertNotIn("USE GRAPH", gsql_cmd)


# ──────────────────────────────────────────────────────────────────────
# useGraph() / useGlobal() scope control
# ──────────────────────────────────────────────────────────────────────

class TestGraphScopeControl(unittest.TestCase):

    def test_use_graph(self):
        conn = _make_conn(graphname="original")
        conn.useGraph("newGraph")
        self.assertEqual(conn.graphname, "newGraph")

    def test_use_graph_empty_delegates_to_global(self):
        conn = _make_conn(graphname="original")
        conn.useGraph("")
        self.assertEqual(conn.graphname, "")

    def test_use_global(self):
        conn = _make_conn(graphname="original")
        conn.useGlobal()
        self.assertEqual(conn.graphname, "")

    def test_use_global_context_manager(self):
        conn = _make_conn(graphname="original")
        with conn.useGlobal():
            self.assertEqual(conn.graphname, "")
        self.assertEqual(conn.graphname, "original")

    def test_use_global_context_manager_restores_on_exception(self):
        conn = _make_conn(graphname="original")
        try:
            with conn.useGlobal():
                self.assertEqual(conn.graphname, "")
                raise ValueError("test error")
        except ValueError:
            pass
        self.assertEqual(conn.graphname, "original")

    def test_use_global_context_manager_nested(self):
        conn = _make_conn(graphname="original")
        with conn.useGlobal():
            self.assertEqual(conn.graphname, "")
            conn.useGraph("inner")
            self.assertEqual(conn.graphname, "inner")
        self.assertEqual(conn.graphname, "original")


# ──────────────────────────────────────────────────────────────────────
# Reserved keyword helpers
# ──────────────────────────────────────────────────────────────────────

class TestReservedKeywords(unittest.TestCase):

    def test_get_reserved_keywords_returns_frozenset(self):
        kw = TigerGraphConnection.getReservedKeywords()
        self.assertIsInstance(kw, frozenset)

    def test_get_reserved_keywords_not_empty(self):
        kw = TigerGraphConnection.getReservedKeywords()
        self.assertGreater(len(kw), 50)

    def test_known_keywords_present(self):
        kw = TigerGraphConnection.getReservedKeywords()
        for word in ["SELECT", "CREATE", "DROP", "VERTEX", "EDGE", "GRAPH",
                     "FROM", "WHERE", "AND", "OR", "NOT", "INT", "STRING",
                     "BOOL", "FLOAT", "DOUBLE", "UINT", "PRIMARY_ID"]:
            self.assertIn(word, kw, f"{word} should be a reserved keyword")

    def test_is_reserved_keyword_true(self):
        self.assertTrue(TigerGraphConnection.isReservedKeyword("SELECT"))
        self.assertTrue(TigerGraphConnection.isReservedKeyword("VERTEX"))

    def test_is_reserved_keyword_case_insensitive(self):
        self.assertTrue(TigerGraphConnection.isReservedKeyword("select"))
        self.assertTrue(TigerGraphConnection.isReservedKeyword("Select"))
        self.assertTrue(TigerGraphConnection.isReservedKeyword("VERTEX"))
        self.assertTrue(TigerGraphConnection.isReservedKeyword("vertex"))

    def test_is_reserved_keyword_false(self):
        self.assertFalse(TigerGraphConnection.isReservedKeyword("myCustomName"))
        self.assertFalse(TigerGraphConnection.isReservedKeyword("foobar"))
        self.assertFalse(TigerGraphConnection.isReservedKeyword(""))


# ──────────────────────────────────────────────────────────────────────
# _wrap_gsql_result helper
# ──────────────────────────────────────────────────────────────────────

class TestWrapGsqlResult(unittest.TestCase):

    def test_success_result(self):
        from pyTigerGraph.common.gsql import _wrap_gsql_result
        result = _wrap_gsql_result("Graph g1 created successfully.")
        self.assertFalse(result["error"])
        self.assertEqual(result["message"], "Graph g1 created successfully.")

    def test_error_result_raises(self):
        from pyTigerGraph.common.gsql import _wrap_gsql_result
        with self.assertRaises(TigerGraphException):
            _wrap_gsql_result("Semantic Check Fails: vertex type does not exist")

    def test_error_result_skip_check(self):
        from pyTigerGraph.common.gsql import _wrap_gsql_result
        result = _wrap_gsql_result(
            "Semantic Check Fails: vertex type does not exist", skipCheck=True
        )
        self.assertTrue(result["error"])

    def test_none_result(self):
        from pyTigerGraph.common.gsql import _wrap_gsql_result
        result = _wrap_gsql_result(None)
        self.assertFalse(result["error"])
        self.assertEqual(result["message"], "")


# ──────────────────────────────────────────────────────────────────────
# _parse_graph_list helper
# ──────────────────────────────────────────────────────────────────────

class TestParseGraphList(unittest.TestCase):

    def test_parses_typed_entries(self):
        from pyTigerGraph.common.gsql import _parse_graph_list
        output = (
            "  - Graph g1(Person:v, Company:v, Knows:e, WorksAt:e)\n"
            "  - Graph g2(V1:v)\n"
        )
        result = _parse_graph_list(output)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["GraphName"], "g1")
        self.assertEqual(sorted(result[0]["VertexTypes"]), ["Company", "Person"])
        self.assertEqual(sorted(result[0]["EdgeTypes"]), ["Knows", "WorksAt"])
        self.assertEqual(result[1]["GraphName"], "g2")
        self.assertEqual(result[1]["VertexTypes"], ["V1"])
        self.assertEqual(result[1]["EdgeTypes"], [])

    def test_empty_output(self):
        from pyTigerGraph.common.gsql import _parse_graph_list
        result = _parse_graph_list("")
        self.assertEqual(result, [])

    def test_none_output(self):
        from pyTigerGraph.common.gsql import _parse_graph_list
        result = _parse_graph_list(None)
        self.assertEqual(result, [])


# ──────────────────────────────────────────────────────────────────────
# installQueries() wait parameter
# ──────────────────────────────────────────────────────────────────────

def _make_conn_v41(graphname="testgraph", **kwargs):
    """Create a connection that reports TigerGraph 4.1.0."""
    conn = _make_conn(graphname=graphname, **kwargs)
    conn.getVer = MagicMock(return_value="4.1.0")
    conn.ver = "4.1.0"
    return conn


class TestInstallQueriesWaitParam(unittest.TestCase):

    def test_wait_true_polls_until_success(self):
        """With wait=True, installQueries polls until SUCCESS."""
        conn = _make_conn_v41()
        install_response = {"requestId": "req123", "message": "submitted"}
        pending_response = {"requestId": "req123", "message": "RUNNING"}
        success_response = {"requestId": "req123", "message": "SUCCESS"}

        with patch.object(conn, "_req", side_effect=[
            install_response, pending_response, success_response
        ]) as mock_req, \
             patch("pyTigerGraph.pyTigerGraphQuery.time.sleep"):
            result = conn.installQueries("my_query", wait=True)

        self.assertEqual(result, success_response)
        # 1 install call + 2 status poll calls
        self.assertEqual(mock_req.call_count, 3)

    def test_wait_true_polls_until_failed(self):
        """With wait=True, installQueries stops polling on FAILED."""
        conn = _make_conn_v41()
        install_response = {"requestId": "req123", "message": "submitted"}
        failed_response = {"requestId": "req123", "message": "FAILED: compile error"}

        with patch.object(conn, "_req", side_effect=[
            install_response, failed_response
        ]), \
             patch("pyTigerGraph.pyTigerGraphQuery.time.sleep"):
            result = conn.installQueries("my_query", wait=True)

        self.assertEqual(result, failed_response)

    def test_wait_false_returns_immediately(self):
        """With wait=False, installQueries returns the initial response."""
        conn = _make_conn_v41()
        install_response = {"requestId": "req123", "message": "submitted"}

        with patch.object(conn, "_req", return_value=install_response) as mock_req:
            result = conn.installQueries("my_query", wait=False)

        self.assertEqual(result, install_response)
        mock_req.assert_called_once()

    def test_wait_true_no_request_id_returns_directly(self):
        """With wait=True, if no requestId in response, returns immediately."""
        conn = _make_conn_v41()
        sync_response = {"message": "SUCCESS"}

        with patch.object(conn, "_req", return_value=sync_response) as mock_req:
            result = conn.installQueries("my_query", wait=True)

        self.assertEqual(result, sync_response)
        mock_req.assert_called_once()

    def test_sync_default_wait_is_true(self):
        """Sync installQueries defaults to wait=True."""
        import inspect
        from pyTigerGraph.pyTigerGraphQuery import pyTigerGraphQuery
        sig = inspect.signature(pyTigerGraphQuery.installQueries)
        self.assertTrue(sig.parameters["wait"].default)

    def test_async_default_wait_is_false(self):
        """Async installQueries defaults to wait=False."""
        import inspect
        from pyTigerGraph.pytgasync.pyTigerGraphQuery import AsyncPyTigerGraphQuery
        sig = inspect.signature(AsyncPyTigerGraphQuery.installQueries)
        self.assertFalse(sig.parameters["wait"].default)

    def test_queries_list_joined(self):
        """A list of query names is joined with commas."""
        conn = _make_conn_v41()
        with patch.object(conn, "_req", return_value={"message": "SUCCESS"}) as mock_req:
            conn.installQueries(["q1", "q2", "q3"], wait=False)

        _, kwargs = mock_req.call_args
        self.assertEqual(kwargs["params"]["queries"], "q1,q2,q3")

    def test_flag_list_joined(self):
        """A list of flags is joined with commas."""
        conn = _make_conn_v41()
        with patch.object(conn, "_req", return_value={"message": "SUCCESS"}) as mock_req:
            conn.installQueries("q1", flag=["-single", "-force"], wait=False)

        _, kwargs = mock_req.call_args
        self.assertEqual(kwargs["params"]["flag"], "-single,-force")

    def test_rejects_version_below_4_1(self):
        """installQueries raises on TigerGraph < 4.1."""
        conn = _make_conn(graphname="testgraph")
        conn.getVer = MagicMock(return_value="4.0.0")
        conn.ver = "4.0.0"

        with self.assertRaises(TigerGraphException):
            conn.installQueries("my_query")

    def test_wait_true_times_out(self):
        """Polling raises TigerGraphException after max retries."""
        conn = _make_conn_v41()
        install_response = {"requestId": "req123", "message": "submitted"}
        running_response = {"requestId": "req123", "message": "RUNNING"}

        with patch.object(conn, "_req", side_effect=[install_response] + [running_response] * 360), \
             patch("pyTigerGraph.pyTigerGraphQuery.time.sleep"):
            with self.assertRaises(TigerGraphException) as ctx:
                conn.installQueries("my_query", wait=True)
            self.assertIn("timed out", str(ctx.exception).lower())

    def test_wait_true_handles_missing_message(self):
        """Polling handles responses without 'message' key gracefully."""
        conn = _make_conn_v41()
        install_response = {"requestId": "req123", "message": "submitted"}
        no_message_response = {"requestId": "req123", "status": "unknown"}
        success_response = {"requestId": "req123", "message": "SUCCESS"}

        with patch.object(conn, "_req", side_effect=[
            install_response, no_message_response, success_response
        ]), \
             patch("pyTigerGraph.pyTigerGraphQuery.time.sleep"):
            result = conn.installQueries("my_query", wait=True)

        self.assertEqual(result, success_response)


# ──────────────────────────────────────────────────────────────────────
# _GlobalScope context manager
# ──────────────────────────────────────────────────────────────────────

class TestGlobalScopeDeferredUse(unittest.TestCase):

    def test_deferred_context_manager_restores_correctly(self):
        """Deferred use of context manager saves graphname at __enter__ time."""
        conn = _make_conn(graphname="original")
        scope = conn.useGlobal()          # graphname -> ""
        conn.useGraph("changed")          # graphname -> "changed"
        with scope:
            self.assertEqual(conn.graphname, "")
        # Should restore "changed" (captured at __enter__), not "original"
        self.assertEqual(conn.graphname, "changed")


# ──────────────────────────────────────────────────────────────────────
# runSchemaChangeJob URL encoding
# ──────────────────────────────────────────────────────────────────────

class TestRunSchemaChangeJobParams(unittest.TestCase):

    def test_passes_graph_in_params(self):
        conn = _make_conn_v4(graphname="myGraph")
        with patch.object(conn, "_put", return_value={"error": False, "message": "ok"}) as mock:
            conn.runSchemaChangeJob("job1")
        kwargs = mock.call_args[1]
        self.assertEqual(kwargs["params"]["graph"], "myGraph")

    def test_passes_force_in_params(self):
        conn = _make_conn_v4(graphname="myGraph")
        with patch.object(conn, "_put", return_value={"error": False, "message": "ok"}) as mock:
            conn.runSchemaChangeJob("job1", force=True)
        kwargs = mock.call_args[1]
        self.assertEqual(kwargs["params"]["force"], "true")

    def test_no_graph_omits_param(self):
        conn = _make_conn_v4(graphname="")
        with patch.object(conn, "_put", return_value={"error": False, "message": "ok"}) as mock:
            conn.runSchemaChangeJob("job1")
        kwargs = mock.call_args[1]
        self.assertNotIn("graph", kwargs["params"])


# ──────────────────────────────────────────────────────────────────────
# createSchemaChangeJob Content-Type
# ──────────────────────────────────────────────────────────────────────

class TestCreateSchemaChangeJobContentType(unittest.TestCase):

    def test_gsql_path_sends_text_plain_content_type(self):
        conn = _make_conn_v4(graphname="g1")
        with patch.object(conn, "_post", return_value={"error": False, "message": "ok"}) as mock:
            conn.createSchemaChangeJob("job1", "ADD VERTEX V1 (PRIMARY_ID id UINT);")
        kwargs = mock.call_args[1]
        self.assertEqual(kwargs["headers"]["Content-Type"], "text/plain")

    def test_dict_path_sends_json_content_type(self):
        conn = _make_conn_v4(graphname="g1")
        with patch.object(conn, "_post", return_value={"error": False, "message": "ok"}) as mock:
            conn.createSchemaChangeJob("job1", {"some": "config"})
        kwargs = mock.call_args[1]
        self.assertEqual(kwargs["headers"]["Content-Type"], "application/json")


if __name__ == "__main__":
    unittest.main(verbosity=2)
