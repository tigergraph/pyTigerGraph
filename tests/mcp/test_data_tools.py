"""Tests for pyTigerGraph.mcp.tools.data_tools."""

import unittest
from unittest.mock import patch

from tests.mcp import MCPToolTestBase
from pyTigerGraph.mcp.tools.data_tools import (
    _generate_loading_job_gsql,
    create_loading_job,
    drop_loading_job,
    run_loading_job_with_data,
    run_loading_job_with_file,
)

PATCH_TARGET = "pyTigerGraph.mcp.tools.data_tools.get_connection"


class TestGenerateLoadingJobGsql(unittest.TestCase):
    """Pure-function tests for GSQL generation logic."""

    def test_node_mapping(self):
        gsql_str = _generate_loading_job_gsql(
            graph_name="G",
            job_name="load_people",
            files=[{
                "file_alias": "f1",
                "file_path": "/data/people.csv",
                "node_mappings": [
                    {
                        "vertex_type": "Person",
                        "attribute_mappings": {"id": 0, "name": 1, "age": 2},
                    }
                ],
            }],
        )
        self.assertIn("CREATE LOADING JOB load_people", gsql_str)
        self.assertIn("Person", gsql_str)
        self.assertIn("$0", gsql_str)
        self.assertIn("$1", gsql_str)

    def test_edge_mapping(self):
        gsql_str = _generate_loading_job_gsql(
            graph_name="G",
            job_name="load_follows",
            files=[{
                "file_alias": "f1",
                "file_path": "/data/follows.csv",
                "edge_mappings": [
                    {
                        "edge_type": "FOLLOWS",
                        "source_column": 0,
                        "target_column": 1,
                    }
                ],
            }],
        )
        self.assertIn("CREATE LOADING JOB load_follows", gsql_str)
        self.assertIn("FOLLOWS", gsql_str)
        self.assertIn("$0", gsql_str)
        self.assertIn("$1", gsql_str)

    def test_header_columns(self):
        gsql_str = _generate_loading_job_gsql(
            graph_name="G",
            job_name="load_h",
            files=[{
                "file_alias": "f",
                "file_path": "/data/h.csv",
                "header": "true",
                "node_mappings": [
                    {
                        "vertex_type": "V",
                        "attribute_mappings": {"id": "id", "name": "name"},
                    }
                ],
            }],
        )
        self.assertIn("HEADER", gsql_str)
        self.assertIn('$"id"', gsql_str)

    def test_custom_separator(self):
        gsql_str = _generate_loading_job_gsql(
            graph_name="G",
            job_name="tsv_job",
            files=[{
                "file_alias": "f",
                "file_path": "/data/tab.tsv",
                "separator": "\\t",
                "node_mappings": [
                    {"vertex_type": "V", "attribute_mappings": {"id": 0}}
                ],
            }],
        )
        self.assertIn("\\t", gsql_str)

    def test_mixed_vertex_and_edge(self):
        gsql_str = _generate_loading_job_gsql(
            graph_name="G",
            job_name="mixed",
            files=[{
                "file_alias": "f",
                "file_path": "/data/m.csv",
                "node_mappings": [
                    {"vertex_type": "Person", "attribute_mappings": {"id": 0, "name": 1}}
                ],
                "edge_mappings": [
                    {
                        "edge_type": "KNOWS",
                        "source_column": 0,
                        "target_column": 2,
                    }
                ],
            }],
        )
        self.assertIn("Person", gsql_str)
        self.assertIn("KNOWS", gsql_str)
        self.assertIn("$0", gsql_str)
        self.assertIn("$2", gsql_str)


class TestCreateLoadingJob(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Successfully created loading job"

        result = await create_loading_job(
            job_name="load_test",
            files=[{
                "file_alias": "f1",
                "file_path": "/data/test.csv",
                "node_mappings": [
                    {"vertex_type": "Person", "attribute_mappings": {"id": 0, "name": 1}}
                ],
            }],
        )
        resp = self.assert_success(result)
        self.assertIn("load_test", resp["summary"])

    @patch(PATCH_TARGET)
    async def test_with_run(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Successfully created and ran"

        result = await create_loading_job(
            job_name="load_run",
            files=[{
                "file_alias": "f1",
                "node_mappings": [
                    {"vertex_type": "V", "attribute_mappings": {"id": 0}}
                ],
            }],
            run_job=True,
        )
        resp = self.assert_success(result)
        gsql_arg = self.mock_conn.gsql.call_args[0][0]
        self.assertIn("RUN LOADING JOB load_run", gsql_arg)

    @patch(PATCH_TARGET)
    async def test_with_drop_after_run(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Successfully created, ran, and dropped"

        result = await create_loading_job(
            job_name="load_drop",
            files=[{
                "file_alias": "f1",
                "node_mappings": [
                    {"vertex_type": "V", "attribute_mappings": {"id": 0}}
                ],
            }],
            run_job=True,
            drop_after_run=True,
        )
        resp = self.assert_success(result)
        gsql_arg = self.mock_conn.gsql.call_args[0][0]
        self.assertIn("DROP JOB load_drop", gsql_arg)

    @patch(PATCH_TARGET)
    async def test_gsql_error(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "SEMANTIC ERROR: bad schema"

        result = await create_loading_job(
            job_name="bad",
            files=[{
                "file_alias": "f",
                "node_mappings": [
                    {"vertex_type": "V", "attribute_mappings": {"id": 0}}
                ],
            }],
        )
        self.assert_error(result)


class TestRunLoadingJobWithFile(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runLoadingJobWithFile.return_value = {"loaded": 500}

        result = await run_loading_job_with_file(
            job_name="my_job",
            file_path="/data/file.csv",
            file_tag="f1",
        )
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["result"]["loaded"], 500)

    @patch(PATCH_TARGET)
    async def test_no_result(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runLoadingJobWithFile.return_value = None

        result = await run_loading_job_with_file(
            job_name="my_job",
            file_path="/data/file.csv",
            file_tag="f1",
        )
        self.assert_error(result)

    @patch(PATCH_TARGET)
    async def test_exception(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runLoadingJobWithFile.side_effect = Exception("file not found")

        result = await run_loading_job_with_file(
            job_name="my_job",
            file_path="/missing.csv",
            file_tag="f1",
        )
        self.assert_error(result)


class TestRunLoadingJobWithData(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runLoadingJobWithData.return_value = {"loaded": 3}

        result = await run_loading_job_with_data(
            job_name="inline_job",
            data="v1,Alice\nv2,Bob",
            file_tag="f1",
        )
        resp = self.assert_success(result)
        self.assertEqual(resp["data"]["result"]["loaded"], 3)

    @patch(PATCH_TARGET)
    async def test_exception(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.runLoadingJobWithData.side_effect = Exception("parse error")

        result = await run_loading_job_with_data(
            job_name="bad",
            data="garbage",
            file_tag="f1",
        )
        self.assert_error(result)


class TestDropLoadingJob(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.dropLoadingJob.return_value = "OK"

        result = await drop_loading_job(job_name="old_job")
        self.assert_success(result)

    @patch(PATCH_TARGET)
    async def test_not_found(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.dropLoadingJob.side_effect = Exception(
            "Loading job 'old_job' does not exist"
        )

        result = await drop_loading_job(job_name="old_job")
        self.assert_error(result)


if __name__ == "__main__":
    unittest.main()
