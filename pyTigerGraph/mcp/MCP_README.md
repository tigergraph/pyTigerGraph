# pyTigerGraph MCP Support

pyTigerGraph now includes Model Context Protocol (MCP) support, allowing AI agents to interact with TigerGraph through the MCP standard. All MCP tools use pyTigerGraph's async APIs for optimal performance.

## Installation

To use MCP functionality, install pyTigerGraph with the `mcp` extra:

```bash
pip install pyTigerGraph[mcp]
```

This will install:
- `mcp>=1.0.0` - The MCP SDK
- `pydantic>=2.0.0` - For data validation
- `click` - For the CLI entry point
- `python-dotenv>=1.0.0` - For loading .env files

## Usage

### Running the MCP Server

You can run the MCP server as a standalone process:

```bash
tigergraph-mcp
```

With a custom .env file:

```bash
tigergraph-mcp --env-file /path/to/.env
```

With verbose logging:

```bash
tigergraph-mcp -v    # INFO level
tigergraph-mcp -vv   # DEBUG level
```

Or programmatically:

```python
from pyTigerGraph.mcp import serve
import asyncio

asyncio.run(serve())
```

### Configuration

The MCP server reads connection configuration from environment variables. You can set these either directly as environment variables or in a `.env` file.

#### Using a .env File (Recommended)

Create a `.env` file in your project directory:

```bash
# .env
TG_HOST=http://localhost
TG_GRAPHNAME=MyGraph  # Optional - can be omitted if database has multiple graphs
TG_USERNAME=tigergraph
TG_PASSWORD=tigergraph
TG_RESTPP_PORT=9000
TG_GS_PORT=14240
```

The server will automatically load the `.env` file if it exists. Environment variables take precedence over `.env` file values.

You can also specify a custom path to the `.env` file:

```bash
tigergraph-mcp --env-file /path/to/custom/.env
```

#### Environment Variables

The following environment variables are supported:

- `TG_HOST` - TigerGraph host (default: http://127.0.0.1)
- `TG_GRAPHNAME` - Graph name (optional - can be omitted if database has multiple graphs. Use `tigergraph__list_graphs` tool to see available graphs)
- `TG_USERNAME` - Username (default: tigergraph)
- `TG_PASSWORD` - Password (default: tigergraph)
- `TG_SECRET` - GSQL secret (optional)
- `TG_API_TOKEN` - API token (optional)
- `TG_JWT_TOKEN` - JWT token (optional)
- `TG_RESTPP_PORT` - REST++ port (default: 9000)
- `TG_GS_PORT` - GSQL port (default: 14240)
- `TG_SSL_PORT` - SSL port (default: 443)
- `TG_TGCLOUD` - Whether using TigerGraph Cloud (default: False)
- `TG_CERT_PATH` - Path to certificate (optional)

### Using with Existing Connection

You can also use MCP with an existing `TigerGraphConnection` (sync) or `AsyncTigerGraphConnection`:

**With Sync Connection:**
```python
from pyTigerGraph import TigerGraphConnection

conn = TigerGraphConnection(
    host="http://localhost",
    graphname="MyGraph",
    username="tigergraph",
    password="tigergraph"
)

# Enable MCP support for this connection
# This creates an async connection internally for MCP tools
conn.start_mcp_server()
```

**With Async Connection (Recommended):**
```python
from pyTigerGraph import AsyncTigerGraphConnection
from pyTigerGraph.mcp import ConnectionManager

conn = AsyncTigerGraphConnection(
    host="http://localhost",
    graphname="MyGraph",
    username="tigergraph",
    password="tigergraph"
)

# Set as default for MCP tools
ConnectionManager.set_default_connection(conn)
```

This sets the connection as the default for MCP tools. Note that MCP tools use async APIs internally, so using `AsyncTigerGraphConnection` directly is more efficient.

## Available Tools

The MCP server provides the following tools:

### Global Schema Operations (Database Level)
These operations work with the global schema that spans across the entire TigerGraph database.

- `tigergraph__get_global_schema` - Get the complete global schema (all global vertex/edge types, graphs, and members) via GSQL 'LS' command

### Graph Operations (Database Level)
These operations manage individual graphs within the TigerGraph database. A database can contain multiple graphs.

- `tigergraph__list_graphs` - List all graph names in the database (names only, no details)
- `tigergraph__create_graph` - Create a new graph with its schema (vertex types, edge types)
- `tigergraph__drop_graph` - Drop (delete) a graph and its schema
- `tigergraph__clear_graph_data` - Clear all data from a graph (keeps schema structure)

### Schema Operations (Graph Level)
These operations work with the schema of a specific graph. Each graph has its own independent schema.

- `tigergraph__get_graph_schema` - Get the schema of a specific graph (raw JSON)
- `tigergraph__describe_graph` - Get a human-readable description of a specific graph's schema
- `tigergraph__get_graph_metadata` - Get metadata about a specific graph (vertex types, edge types, queries, loading jobs)

### Node Operations
- `tigergraph__add_node` - Add a single node
- `tigergraph__add_nodes` - Add multiple nodes
- `tigergraph__get_node` - Get a single node
- `tigergraph__get_nodes` - Get multiple nodes
- `tigergraph__delete_node` - Delete a single node
- `tigergraph__delete_nodes` - Delete multiple nodes
- `tigergraph__has_node` - Check if a node exists
- `tigergraph__get_node_edges` - Get all edges connected to a node

### Edge Operations
- `tigergraph__add_edge` - Add a single edge
- `tigergraph__add_edges` - Add multiple edges
- `tigergraph__get_edge` - Get a single edge
- `tigergraph__get_edges` - Get multiple edges
- `tigergraph__delete_edge` - Delete a single edge
- `tigergraph__delete_edges` - Delete multiple edges
- `tigergraph__has_edge` - Check if an edge exists

### Query Operations
- `tigergraph__run_query` - Run an interpreted query
- `tigergraph__run_installed_query` - Run an installed query
- `tigergraph__install_query` - Install a query
- `tigergraph__drop_query` - Drop (delete) an installed query
- `tigergraph__show_query` - Show query text
- `tigergraph__get_query_metadata` - Get query metadata
- `tigergraph__is_query_installed` - Check if a query is installed
- `tigergraph__get_neighbors` - Get neighbor vertices of a node

### Loading Job Operations
- `tigergraph__create_loading_job` - Create a loading job from structured config (file mappings, node/edge mappings)
- `tigergraph__run_loading_job_with_file` - Execute a loading job with a data file
- `tigergraph__run_loading_job_with_data` - Execute a loading job with inline data string
- `tigergraph__get_loading_jobs` - Get all loading jobs for the graph
- `tigergraph__get_loading_job_status` - Get status of a specific loading job
- `tigergraph__drop_loading_job` - Drop a loading job

### Statistics Operations
- `tigergraph__get_vertex_count` - Get vertex count
- `tigergraph__get_edge_count` - Get edge count
- `tigergraph__get_node_degree` - Get the degree (number of edges) of a node

### GSQL Operations
- `tigergraph__gsql` - Execute GSQL command

### Vector Schema Operations
- `tigergraph__add_vector_attribute` - Add a vector attribute to a vertex type (DIMENSION, METRIC: COSINE/L2/IP)
- `tigergraph__drop_vector_attribute` - Drop a vector attribute from a vertex type
- `tigergraph__get_vector_index_status` - Check vector index rebuild status (Ready_for_query/Rebuild_processing)

### Vector Data Operations
- `tigergraph__upsert_vectors` - Upsert multiple vertices with vector data using REST API (batch support)
- `tigergraph__search_top_k_similarity` - Perform vector similarity search using `vectorSearch()` function
- `tigergraph__fetch_vector` - Fetch vertices with vector data using GSQL `PRINT WITH VECTOR`

**Note:** Vector attributes can ONLY be fetched via GSQL queries with `PRINT v WITH VECTOR;` - they cannot be retrieved via REST API.

### Data Source Operations
- `tigergraph__create_data_source` - Create a new data source (S3, GCS, Azure Blob, local)
- `tigergraph__update_data_source` - Update an existing data source
- `tigergraph__get_data_source` - Get information about a data source
- `tigergraph__drop_data_source` - Drop a data source
- `tigergraph__get_all_data_sources` - Get all data sources
- `tigergraph__drop_all_data_sources` - Drop all data sources
- `tigergraph__preview_sample_data` - Preview sample data from a file

## Backward Compatibility

All existing pyTigerGraph APIs continue to work as before. MCP support is completely optional and does not affect existing code. The MCP functionality is only available when:

1. The `mcp` extra is installed
2. You explicitly use MCP-related imports or methods

## Example: Using with MCP Clients

### Using MultiServerMCPClient

```python
from langchain_mcp_adapters import MultiServerMCPClient
from pathlib import Path
from dotenv import dotenv_values
import asyncio

# Load environment variables
env_dict = dotenv_values(dotenv_path=Path(".env").expanduser().resolve())

# Configure the client
client = MultiServerMCPClient(
    {
        "tigergraph-mcp": {
            "transport": "stdio",
            "command": "tigergraph-mcp",
            "args": ["-vv"],  # Enable debug logging
            "env": env_dict,
        },
    }
)

# Get tools and use them
tools = asyncio.run(client.get_tools())
# Tools are now available for use
```

### Using MCP Client SDK Directly

```python
import asyncio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def call_tool():
    # Configure server parameters
    server_params = StdioServerParameters(
        command="tigergraph-mcp",
        args=["-vv"],  # Enable debug logging
        env=None,  # Uses .env file or environment variables
    )
    
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            
            # List available tools
            tools = await session.list_tools()
            print(f"Available tools: {[t.name for t in tools.tools]}")
            
            # Call a tool
            result = await session.call_tool(
                "tigergraph__list_graphs",
                arguments={}
            )
            
            # Print result
            for content in result.content:
                print(content.text)

asyncio.run(call_tool())
```

**Note:** When using `MultiServerMCPClient` or similar MCP clients with stdio transport, the `args` parameter is required. For the `tigergraph-mcp` command (which is a standalone entry point), set `args` to an empty list `[]`. If you need to pass arguments to the command, include them in the list (e.g., `["-v"]` for verbose mode, `["-vv"]` for debug mode).

## Notes

- **Async APIs**: All MCP tools use pyTigerGraph's async APIs (`AsyncTigerGraphConnection`) for optimal performance
- **Transport**: The MCP server uses stdio transport by default
- **Tool Responses**: All tools are async and return `TextContent` responses
- **Error Handling**: Error handling is built into each tool
- **Connection Management**: The connection manager automatically creates async connections from environment variables
- **Performance**: Using async APIs ensures non-blocking I/O operations, making the MCP server more efficient for concurrent requests

