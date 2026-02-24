# MCP Tools Test Suite

Unit tests for all `pyTigerGraph.mcp.tools` modules. Every tool function is tested with a mocked `AsyncTigerGraphConnection`, so **no live TigerGraph instance is required**.

## Prerequisites

Python 3.10+ (the MCP server uses `match` statements).

```bash
cd pyTigerGraph
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[mcp]"
```

## Running the Tests

```bash
# All MCP tests
python -m unittest discover -s tests/mcp -v

# A single test file
python -m unittest tests.mcp.test_vector_tools -v

# A single test class
python -m unittest tests.mcp.test_vector_tools.TestSearchTopKSimilarity -v

# A single test method
python -m unittest tests.mcp.test_vector_tools.TestSearchTopKSimilarity.test_success_flow -v
```

## Test File Layout

| File | Source Module | What It Tests |
|------|--------------|---------------|
| `__init__.py` | — | `MCPToolTestBase` class, `parse_response` / `assert_success` / `assert_error` helpers |
| `test_response_formatter.py` | `mcp.response_formatter` | `gsql_has_error`, `format_success`, `format_error`, `format_list_response` |
| `test_schema_tools.py` | `mcp.tools.schema_tools` | `create_graph`, `drop_graph`, `list_graphs`, `get_graph_schema`, `_build_vertex_stmt`, `_build_edge_stmt`, `clear_graph_data`, `show_graph_details` |
| `test_node_tools.py` | `mcp.tools.node_tools` | `add_node`, `add_nodes`, `get_node`, `get_nodes`, `delete_node`, `delete_nodes`, `has_node`, `get_node_edges` |
| `test_edge_tools.py` | `mcp.tools.edge_tools` | `add_edge`, `add_edges`, `get_edge`, `get_edges`, `delete_edge`, `delete_edges`, `has_edge` |
| `test_query_tools.py` | `mcp.tools.query_tools` | `run_query`, `run_installed_query`, `install_query`, `drop_query`, `show_query`, `get_query_metadata`, `is_query_installed`, `get_neighbors` |
| `test_statistics_tools.py` | `mcp.tools.statistics_tools` | `get_vertex_count`, `get_edge_count`, `get_node_degree` |
| `test_gsql_tools.py` | `mcp.tools.gsql_tools` | `gsql`, `get_llm_config` |
| `test_vector_tools.py` | `mcp.tools.vector_tools` | `add_vector_attribute`, `drop_vector_attribute`, `list_vector_attributes`, `get_vector_index_status`, `upsert_vectors`, `search_top_k_similarity`, `fetch_vector`, `load_vectors_from_csv`, `load_vectors_from_json` |
| `test_datasource_tools.py` | `mcp.tools.datasource_tools` | `create_data_source`, `update_data_source`, `get_data_source`, `drop_data_source`, `get_all_data_sources`, `drop_all_data_sources`, `preview_sample_data` |
| `test_data_tools.py` | `mcp.tools.data_tools` | `_generate_loading_job_gsql`, `create_loading_job`, `run_loading_job_with_file`, `run_loading_job_with_data`, `drop_loading_job` |

## How Mocking Works

Each test class patches `get_connection` at the module level so the tool function receives an `AsyncMock` instead of a real connection:

```python
from unittest.mock import patch
from tests.mcp import MCPToolTestBase

PATCH_TARGET = "pyTigerGraph.mcp.tools.node_tools.get_connection"

class TestAddNode(MCPToolTestBase):

    @patch(PATCH_TARGET)
    async def test_success(self, mock_gc):
        mock_gc.return_value = self.mock_conn          # pre-configured in setUp()
        self.mock_conn.upsertVertex.return_value = None

        result = await add_node(vertex_type="Person", vertex_id="u1")
        resp = self.assert_success(result)             # parses JSON, asserts success=True
```

## Adding New Tests

1. Create a new file `test_<module>.py` in this directory.
2. Import `MCPToolTestBase` from `tests.mcp`.
3. Subclass it — `self.mock_conn` is ready in `setUp()`.
4. Patch `get_connection` for the module under test.
5. Use `self.assert_success(result)` / `self.assert_error(result)` to validate responses.
