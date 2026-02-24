"""Tests for pyTigerGraph.mcp.tools.vector_tools.

This is the most critical test file because the vector tools had multiple
bugs fixed (DROP JOB before CREATE, NameError in exception handlers, etc.).
"""

import unittest
from unittest.mock import AsyncMock, call, patch

from tests.mcp import MCPToolTestBase
from pyTigerGraph.mcp.tools.vector_tools import (
    add_vector_attribute,
    drop_vector_attribute,
    fetch_vector,
    get_vector_index_status,
    list_vector_attributes,
    load_vectors_from_csv,
    load_vectors_from_json,
    search_top_k_similarity,
    upsert_vectors,
)

PATCH_TARGET = "pyTigerGraph.mcp.tools.vector_tools.get_connection"


# =========================================================================
# Vector Schema Tools
# =========================================================================


class TestAddVectorAttribute(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success_local(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.side_effect = [
            "",  # SHOW VERTEX (not global)
            "Successfully created schema change job",
        ]

        result = await add_vector_attribute(
            vertex_type="Person",
            vector_name="embedding",
            dimension=1536,
            metric="COSINE",
        )
        resp = self.assert_success(result)
        self.assertIn("embedding", resp["summary"])
        self.assertEqual(resp["data"]["dimension"], 1536)

    @patch(PATCH_TARGET)
    async def test_invalid_metric(self, mock_gc):
        mock_gc.return_value = self.mock_conn

        result = await add_vector_attribute(
            vertex_type="Person",
            vector_name="emb",
            dimension=128,
            metric="INVALID",
        )
        self.assert_error(result)

    @patch(PATCH_TARGET)
    async def test_gsql_error(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.side_effect = [
            "",  # SHOW VERTEX
            "SEMANTIC ERROR: vertex type does not exist",
        ]

        result = await add_vector_attribute(
            vertex_type="NoType", vector_name="emb", dimension=128
        )
        self.assert_error(result)


class TestDropVectorAttribute(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.side_effect = [
            "",  # SHOW VERTEX
            "Successfully ran schema change",
        ]

        result = await drop_vector_attribute(
            vertex_type="Person", vector_name="embedding"
        )
        self.assert_success(result)


class TestListVectorAttributes(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_parse_ls_output(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = (
            "Some header\n"
            "Vector Embeddings:\n"
            "  - Person:\n"
            '    - embedding(Dimension=1536, IndexType="HNSW", DataType="FLOAT", Metric="COSINE")\n'
            "Other section\n"
        )

        result = await list_vector_attributes()
        resp = self.assert_success(result)
        attrs = resp["data"]["vector_attributes"]
        self.assertEqual(len(attrs), 1)
        self.assertEqual(attrs[0]["vertex_type"], "Person")
        self.assertEqual(attrs[0]["vector_name"], "embedding")
        self.assertEqual(attrs[0]["dimension"], 1536)
        self.assertEqual(attrs[0]["metric"], "COSINE")

    @patch(PATCH_TARGET)
    async def test_filter_by_vertex_type(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = (
            "Vector Embeddings:\n"
            "  - Person:\n"
            '    - emb1(Dimension=128, Metric="L2")\n'
            "  - Product:\n"
            '    - emb2(Dimension=256, Metric="IP")\n'
        )

        result = await list_vector_attributes(vertex_type="Person")
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["count"], 1)
        self.assertEqual(resp["data"]["vector_attributes"][0]["vertex_type"], "Person")

    @patch(PATCH_TARGET)
    async def test_no_vector_attrs(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Vertex Types:\n  - Person\nEdge Types:\n"

        result = await list_vector_attributes()
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["count"], 0)


class TestGetVectorIndexStatus(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_ready(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn._req.return_value = {"NeedRebuildServers": []}

        result = await get_vector_index_status()
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["status"], "Ready_for_query")

    @patch(PATCH_TARGET)
    async def test_rebuilding(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn._req.return_value = {"NeedRebuildServers": ["server1"]}

        result = await get_vector_index_status()
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["status"], "Rebuild_processing")

    @patch(PATCH_TARGET)
    async def test_no_result(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn._req.return_value = None

        result = await get_vector_index_status()
        self.assert_success(result)

    @patch(PATCH_TARGET)
    async def test_exception(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn._req.side_effect = Exception("timeout")

        result = await get_vector_index_status()
        self.assert_error(result)


# =========================================================================
# Vector Data Tools
# =========================================================================


class TestUpsertVectors(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.upsertVertex.return_value = None

        vectors = [
            {"vertex_id": "v1", "vector": [0.1, 0.2, 0.3]},
            {"vertex_id": "v2", "vector": [0.4, 0.5, 0.6]},
        ]
        result = await upsert_vectors(
            vertex_type="Person", vector_attribute="emb", vectors=vectors
        )
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["success_count"], 2)
        self.assertEqual(resp["data"]["dimensions"], 3)

    @patch(PATCH_TARGET)
    async def test_partial_failure(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.upsertVertex.side_effect = [None, Exception("bad vertex")]

        vectors = [
            {"vertex_id": "v1", "vector": [0.1]},
            {"vertex_id": "v2", "vector": [0.2]},
        ]
        result = await upsert_vectors(
            vertex_type="Person", vector_attribute="emb", vectors=vectors
        )
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["success_count"], 1)
        self.assertEqual(resp["data"]["failed_count"], 1)


class TestSearchTopKSimilarity(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success_flow(self, mock_gc):
        """Verify create → install → run → drop lifecycle."""
        mock_gc.return_value = self.mock_conn
        # LS for dimension check
        self.mock_conn.gsql.side_effect = [
            'embedding(Dimension=3, Metric="COSINE")',  # LS
            "Successfully created and installed query",   # CREATE+INSTALL
            "Successfully dropped query",                 # DROP
        ]
        self.mock_conn.runInstalledQuery.return_value = [
            {"v": [{"v_id": "v1"}]},
            {"distances": {"v1": 0.95}},
        ]

        result = await search_top_k_similarity(
            vertex_type="Person",
            vector_attribute="embedding",
            query_vector=[0.1, 0.2, 0.3],
            top_k=5,
        )
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["top_k"], 5)
        self.mock_conn.runInstalledQuery.assert_called_once()

    @patch(PATCH_TARGET)
    async def test_dimension_mismatch(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = 'embedding(Dimension=1536, Metric="COSINE")'

        result = await search_top_k_similarity(
            vertex_type="Person",
            vector_attribute="embedding",
            query_vector=[0.1, 0.2, 0.3],  # dim=3, expected=1536
        )
        resp = self.assert_error(result)
        self.assertIn("mismatch", resp["error"])

    @patch(PATCH_TARGET)
    async def test_gsql_create_error(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.side_effect = [
            "",  # LS (no dimension info found)
            "SEMANTIC ERROR: vertex type does not exist",
        ]

        result = await search_top_k_similarity(
            vertex_type="NoType",
            vector_attribute="emb",
            query_vector=[0.1],
        )
        self.assert_error(result)

    @patch(PATCH_TARGET)
    async def test_cleanup_on_run_error(self, mock_gc):
        """Temp query should be dropped even when runInstalledQuery fails."""
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.side_effect = [
            "",  # LS
            "Successfully created query",   # CREATE+INSTALL
            "Successfully dropped query",   # DROP (in finally)
        ]
        self.mock_conn.runInstalledQuery.side_effect = Exception("runtime error")

        result = await search_top_k_similarity(
            vertex_type="Person",
            vector_attribute="emb",
            query_vector=[0.1],
        )
        self.assert_error(result)
        # DROP should have been called (either in finally or in except)
        drop_calls = [
            c for c in self.mock_conn.gsql.call_args_list
            if "DROP QUERY" in str(c)
        ]
        self.assertTrue(len(drop_calls) > 0, "Temp query should be dropped on error")


class TestFetchVector(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success_flow(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.side_effect = [
            "Successfully created and installed query",
            "Successfully dropped query",
        ]
        self.mock_conn.runInstalledQuery.return_value = [
            {"v": [{"v_id": "v1", "embedding": [0.1, 0.2]}]}
        ]

        result = await fetch_vector(
            vertex_type="Person", vertex_ids=["v1", "v2"]
        )
        resp = self.assert_success(result)
        self.assertIn("2 vertex ID(s)", resp["summary"])

    @patch(PATCH_TARGET)
    async def test_gsql_create_error(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "SEMANTIC ERROR: bad vertex type"

        result = await fetch_vector(vertex_type="Bad", vertex_ids=["v1"])
        self.assert_error(result)

    @patch(PATCH_TARGET)
    async def test_cleanup_on_exception(self, mock_gc):
        """NameError should not occur if get_connection succeeds but run fails."""
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.side_effect = [
            "Successfully created query",
            "OK",  # DROP
        ]
        self.mock_conn.runInstalledQuery.side_effect = Exception("fail")

        result = await fetch_vector(vertex_type="Person", vertex_ids=["v1"])
        self.assert_error(result)

    @patch(PATCH_TARGET)
    async def test_no_nameerror_on_connection_failure(self, mock_gc):
        """If get_connection() itself throws, gname/query_name are None — no NameError."""
        mock_gc.side_effect = Exception("no connection")

        result = await fetch_vector(vertex_type="Person", vertex_ids=["v1"])
        self.assert_error(result)


# =========================================================================
# Vector File Loading
# =========================================================================


class TestLoadVectorsFromCsv(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.side_effect = [
            "Job not found",               # DROP (ignored)
            "Successfully created job",     # CREATE
            "Successfully dropped job",     # DROP after run
        ]
        self.mock_conn.runLoadingJobWithFile.return_value = {"loaded": 100}

        result = await load_vectors_from_csv(
            vertex_type="Person",
            vector_attribute="emb",
            file_path="/data/vectors.csv",
        )
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["loading_result"]["loaded"], 100)

    @patch(PATCH_TARGET)
    async def test_drop_before_create_does_not_fail(self, mock_gc):
        """The bug fix: DROP JOB is now separate, so its error doesn't block CREATE."""
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.side_effect = [
            Exception("job does not exist"),  # DROP — exception swallowed
            "Successfully created loading job",
            "OK",  # DROP after run
        ]
        self.mock_conn.runLoadingJobWithFile.return_value = {"loaded": 50}

        result = await load_vectors_from_csv(
            vertex_type="Person",
            vector_attribute="emb",
            file_path="/data/v.csv",
        )
        self.assert_success(result)

    @patch(PATCH_TARGET)
    async def test_create_gsql_error(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.side_effect = [
            None,  # DROP
            "SEMANTIC ERROR: bad vertex",
        ]

        result = await load_vectors_from_csv(
            vertex_type="Bad",
            vector_attribute="emb",
            file_path="/data/v.csv",
        )
        self.assert_error(result)

    @patch(PATCH_TARGET)
    async def test_no_nameerror_on_connection_failure(self, mock_gc):
        mock_gc.side_effect = Exception("no connection")

        result = await load_vectors_from_csv(
            vertex_type="P", vector_attribute="e", file_path="/x"
        )
        self.assert_error(result)

    @patch(PATCH_TARGET)
    async def test_custom_separators(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.side_effect = [None, "OK", "OK"]
        self.mock_conn.runLoadingJobWithFile.return_value = {"loaded": 10}

        result = await load_vectors_from_csv(
            vertex_type="Person",
            vector_attribute="emb",
            file_path="/data/v.tsv",
            field_separator="\t",
            element_separator=";",
            header=True,
        )
        self.assert_success(result)
        create_call = self.mock_conn.gsql.call_args_list[1][0][0]
        self.assertIn("\t", create_call)
        self.assertIn(";", create_call)
        self.assertIn("HEADER", create_call)


class TestLoadVectorsFromJson(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.side_effect = [
            None,                           # DROP
            "Successfully created job",     # CREATE
            "OK",                           # DROP after run
        ]
        self.mock_conn.runLoadingJobWithFile.return_value = {"loaded": 200}

        result = await load_vectors_from_json(
            vertex_type="Person",
            vector_attribute="emb",
            file_path="/data/vectors.jsonl",
        )
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["loading_result"]["loaded"], 200)

    @patch(PATCH_TARGET)
    async def test_drop_before_create_does_not_fail(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.side_effect = [
            Exception("job does not exist"),
            "Successfully created job",
            "OK",
        ]
        self.mock_conn.runLoadingJobWithFile.return_value = {"loaded": 1}

        result = await load_vectors_from_json(
            vertex_type="Person",
            vector_attribute="emb",
            file_path="/data/v.jsonl",
        )
        self.assert_success(result)

    @patch(PATCH_TARGET)
    async def test_no_nameerror_on_connection_failure(self, mock_gc):
        mock_gc.side_effect = Exception("no connection")

        result = await load_vectors_from_json(
            vertex_type="P", vector_attribute="e", file_path="/x"
        )
        self.assert_error(result)

    @patch(PATCH_TARGET)
    async def test_json_file_clause_present(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.side_effect = [None, "OK", "OK"]
        self.mock_conn.runLoadingJobWithFile.return_value = {}

        await load_vectors_from_json(
            vertex_type="Person",
            vector_attribute="emb",
            file_path="/data/v.jsonl",
        )
        create_call = self.mock_conn.gsql.call_args_list[1][0][0]
        self.assertIn('JSON_FILE="true"', create_call)


if __name__ == "__main__":
    unittest.main()
