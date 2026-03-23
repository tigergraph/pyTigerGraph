# pyTigerGraph

pyTigerGraph is a Python client for [TigerGraph](https://www.tigergraph.com/) databases. It wraps the REST++ and GSQL APIs and provides both a synchronous and an asynchronous interface.

Full documentation: <https://docs.tigergraph.com/pytigergraph/current/intro/>

Downloads: [![Total Downloads](https://static.pepy.tech/badge/pyTigergraph)](https://pepy.tech/project/pyTigergraph) | [![Monthly Downloads](https://static.pepy.tech/badge/pyTigergraph/month)](https://pepy.tech/project/pyTigergraph) | [![Weekly Downloads](https://static.pepy.tech/badge/pyTigergraph/week)](https://pepy.tech/project/pyTigergraph)

---

## Installation

### Base package

```sh
pip install pyTigerGraph
```

### Optional extras

| Extra | What it adds | Install command |
|-------|-------------|-----------------|
| `gds` | Graph Data Science — data loaders for PyTorch Geometric, DGL, and Pandas | `pip install 'pyTigerGraph[gds]'` |
| `mcp` | Model Context Protocol server — installs [`pyTigerGraph-mcp`](https://github.com/tigergraph/tigergraph-mcp) (convenience alias) | `pip install 'pyTigerGraph[mcp]'` |
| `fast` | [orjson](https://github.com/ijl/orjson) JSON backend — 2–10× faster parsing, releases the GIL under concurrent load | `pip install 'pyTigerGraph[fast]'` |

Extras can be combined:

```sh
pip install 'pyTigerGraph[fast,gds,mcp]'
```

#### `[gds]` prerequisites

Install `torch` before installing the `gds` extra:

1. [Install Torch](https://pytorch.org/get-started/locally/)
2. Optionally [Install PyTorch Geometric](https://pytorch-geometric.readthedocs.io/en/latest/notes/installation.html) or [Install DGL](https://www.dgl.ai/pages/start.html)
3. `pip install 'pyTigerGraph[gds]'`

#### `[fast]` — orjson JSON backend

`orjson` is a Rust-backed JSON library that is detected and used automatically when installed. No code changes are required. It improves throughput in two ways:

- **Faster parsing** — 2–10× vs stdlib `json`
- **GIL release** — threads parse responses concurrently instead of serialising on the GIL

If `orjson` is not installed the library falls back to stdlib `json` transparently.

---

## Quickstart

### Synchronous connection

```python
from pyTigerGraph import TigerGraphConnection

conn = TigerGraphConnection(
    host="http://localhost",
    graphname="my_graph",
    username="tigergraph",
    password="tigergraph",
)

print(conn.echo())
```

Use as a context manager to ensure the underlying HTTP session is closed:

```python
with TigerGraphConnection(host="http://localhost", graphname="my_graph") as conn:
    result = conn.runInstalledQuery("my_query", {"param": "value"})
```

### Asynchronous connection

`AsyncTigerGraphConnection` exposes the same API as `TigerGraphConnection` but with `async`/`await` syntax. It uses [aiohttp](https://docs.aiohttp.org/) internally and shares a single connection pool across all concurrent tasks, making it significantly more efficient than threaded sync code at high concurrency.

```python
import asyncio
from pyTigerGraph import AsyncTigerGraphConnection

async def main():
    async with AsyncTigerGraphConnection(
        host="http://localhost",
        graphname="my_graph",
        username="tigergraph",
        password="tigergraph",
    ) as conn:
        result = await conn.runInstalledQuery("my_query", {"param": "value"})
        print(result)

asyncio.run(main())
```

### Token-based authentication

```python
conn = TigerGraphConnection(
    host="http://localhost",
    graphname="my_graph",
    gsqlSecret="my_secret",   # generates a session token automatically
)
```

### HTTPS / TigerGraph Cloud

```python
conn = TigerGraphConnection(
    host="https://my-instance.i.tgcloud.io",
    graphname="my_graph",
    username="tigergraph",
    password="tigergraph",
    tgCloud=True,
)
```

---

## Connection parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | `str` | `"http://127.0.0.1"` | Server URL including scheme (`http://` or `https://`) |
| `graphname` | `str` | `""` | Target graph name |
| `username` | `str` | `"tigergraph"` | Database username |
| `password` | `str` | `"tigergraph"` | Database password |
| `gsqlSecret` | `str` | `""` | GSQL secret for token-based auth (preferred over username/password) |
| `apiToken` | `str` | `""` | Pre-obtained REST++ API token |
| `jwtToken` | `str` | `""` | JWT token for customer-managed authentication |
| `restppPort` | `int\|str` | `"9000"` | REST++ port (auto-fails over to `14240/restpp` for TigerGraph 4.x) |
| `gsPort` | `int\|str` | `"14240"` | GSQL server port |
| `certPath` | `str` | `None` | Path to CA certificate for HTTPS |
| `tgCloud` | `bool` | `False` | Set to `True` for TigerGraph Cloud instances |

---

## Performance notes

### Synchronous mode (`TigerGraphConnection`)

- Each thread gets its own dedicated HTTP session and connection pool, so concurrent threads never block each other.
- Install `pyTigerGraph[fast]` to activate the `orjson` backend and reduce JSON parsing overhead under concurrent load.
- Use `ThreadPoolExecutor` to run queries in parallel:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

with TigerGraphConnection(...) as conn:
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(conn.runInstalledQuery, "q", {"p": v}) for v in values]
        for f in as_completed(futures):
            print(f.result())
```

### Asynchronous mode (`AsyncTigerGraphConnection`)

- Uses a single `aiohttp.ClientSession` with an unbounded connection pool shared across all concurrent coroutines — no GIL, no thread-scheduling overhead.
- Typically achieves higher QPS and lower tail latency than the threaded sync mode for I/O-bound workloads.

```python
import asyncio
from pyTigerGraph import AsyncTigerGraphConnection

async def main():
    async with AsyncTigerGraphConnection(...) as conn:
        tasks = [conn.runInstalledQuery("q", {"p": v}) for v in values]
        results = await asyncio.gather(*tasks)

asyncio.run(main())
```

---

## Graph Data Science (GDS)

The `gds` sub-module provides data loaders that stream vertex and edge data from TigerGraph directly into PyTorch Geometric, DGL, or Pandas DataFrames for machine learning workflows.

Install requirements, then access via `conn.gds`:

```python
conn = TigerGraphConnection(host="...", graphname="...")
loader = conn.gds.vertexLoader(attributes=["feat", "label"], batch_size=1024)
for batch in loader:
    train(batch)
```

See the [GDS documentation](https://docs.tigergraph.com/pytigergraph/current/gds/) for full details.

---

## MCP Server

The TigerGraph MCP server is now a standalone package: **[pyTigerGraph-mcp](https://github.com/tigergraph/tigergraph-mcp)**. It exposes TigerGraph operations as tools for AI agents and LLM applications (Claude Desktop, Cursor, Copilot, etc.).

```sh
# Recommended — install the standalone package directly
pip install pyTigerGraph-mcp

# Or via the pyTigerGraph convenience alias (installs pyTigerGraph-mcp automatically)
pip install 'pyTigerGraph[mcp]'

# Start the server (reads connection config from environment variables)
tigergraph-mcp
```

For full setup instructions, available tools, configuration examples, and multi-profile support, see the **[pyTigerGraph-mcp README](https://github.com/tigergraph/tigergraph-mcp#readme)**.

> **Migrating from `pyTigerGraph.mcp`?** Update your imports:
> ```python
> # Old
> from pyTigerGraph.mcp import serve, ConnectionManager
> # New
> from tigergraph_mcp import serve, ConnectionManager
> ```

---

## Getting started video

[![pyTigerGraph 101](https://img.youtube.com/vi/2BcC3C-qfX4/hqdefault.jpg)](https://www.youtube.com/watch?v=2BcC3C-qfX4)

Companion notebook: [Google Colab](https://colab.research.google.com/drive/1JhYcnGVWT51KswcXZzyPzKqCoPP5htcC)

---

## Links

- [Documentation](https://docs.tigergraph.com/pytigergraph/current/intro/)
- [PyPI](https://pypi.org/project/pyTigerGraph/)
- [GitHub Issues](https://github.com/tigergraph/pyTigerGraph/issues)
- [Source](https://github.com/tigergraph/pyTigerGraph)
