"""Tests for pyTigerGraph.mcp.tools.datasource_tools."""

import unittest
from unittest.mock import patch

from tests.mcp import MCPToolTestBase
from pyTigerGraph.mcp.tools.datasource_tools import (
    create_data_source,
    drop_all_data_sources,
    drop_data_source,
    get_all_data_sources,
    get_data_source,
    preview_sample_data,
    update_data_source,
)

PATCH_TARGET = "pyTigerGraph.mcp.tools.datasource_tools.get_connection"


class TestCreateDataSource(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Successfully created data source"

        result = await create_data_source(
            data_source_name="my_s3",
            data_source_type="s3",
            config={"bucket": "my-bucket"},
        )
        resp = self.assert_success(result)
        self.assertIn("my_s3", resp["summary"])

    @patch(PATCH_TARGET)
    async def test_gsql_error(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "already exists"

        result = await create_data_source(
            data_source_name="dup", data_source_type="s3", config={}
        )
        self.assert_error(result)


class TestUpdateDataSource(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Data source updated"

        result = await update_data_source(
            data_source_name="my_s3", config={"bucket": "new-bucket"}
        )
        self.assert_success(result)


class TestGetDataSource(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Data source my_s3: type=S3"

        result = await get_data_source(data_source_name="my_s3")
        resp = self.assert_success(result)
        self.assertIn("my_s3", resp["summary"])

    @patch(PATCH_TARGET)
    async def test_not_found(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Data source 'nope' does not exist"

        result = await get_data_source(data_source_name="nope")
        self.assert_error(result)


class TestDropDataSource(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Successfully dropped data source"

        result = await drop_data_source(data_source_name="old_ds")
        self.assert_success(result)


class TestGetAllDataSources(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Data sources:\n  - s3_1\n  - local_1"

        result = await get_all_data_sources()
        self.assert_success(result)


class TestDropAllDataSources(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_requires_confirm(self, mock_gc):
        mock_gc.return_value = self.mock_conn

        result = await drop_all_data_sources(confirm=False)
        self.assert_error(result)

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "All data sources dropped"

        result = await drop_all_data_sources(confirm=True)
        self.assert_success(result)


class TestPreviewSampleData(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "col1|col2\nval1|val2"

        result = await preview_sample_data(
            data_source_name="my_s3",
            file_path="/data/sample.csv",
            num_rows=5,
        )
        resp = self.assert_success(result)
        self.assertIn("5", resp["summary"])

    @patch(PATCH_TARGET)
    async def test_file_not_found(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "File does not exist"

        result = await preview_sample_data(
            data_source_name="my_s3", file_path="/no/file.csv"
        )
        self.assert_error(result)


class TestProfilePropagation(MCPToolTestBase):
    """Verify profile is forwarded to get_connection for datasource tools."""

    @patch(PATCH_TARGET)
    async def test_create_data_source_with_profile(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "Successfully created data source"

        result = await create_data_source(
            data_source_name="my_s3",
            data_source_type="s3",
            config={"bucket": "test"},
            profile="staging",
        )
        self.assert_success(result)
        mock_gc.assert_called_with(profile="staging")

    @patch(PATCH_TARGET)
    async def test_get_all_data_sources_with_profile(self, mock_gc):
        mock_gc.return_value = self.mock_conn
        self.mock_conn.gsql.return_value = "data sources: none"

        result = await get_all_data_sources(profile="analytics")
        mock_gc.assert_called_with(profile="analytics")


if __name__ == "__main__":
    unittest.main()
