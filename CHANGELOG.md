# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.2] - 2026-03-27

### New Features

- **Schema Change Job APIs** — `createSchemaChangeJob()`, `getSchemaChangeJobs()`, `runSchemaChangeJob()`, `dropSchemaChangeJobs()` for managing schema change jobs via REST.
- **`createGraph()` accepts vertex/edge types** — optional `vertexTypes` and `edgeTypes` parameters to include existing global types when creating a graph (e.g. `createGraph("g", vertexTypes=["Person"], edgeTypes=["Knows"])`). Pass `vertexTypes=["*"]` to include all global types.
- **`force` parameter for `runSchemaChange()`** — allows forcing schema changes even when they would cause data loss. Also accepts `dict` (JSON format) for TigerGraph >= 4.0 and supports global schema changes.
- **Graph scope control** — `useGraph(graphName)` and `useGlobal()` methods on the connection object, mirroring GSQL's `USE GRAPH` / `USE GLOBAL`. `useGlobal()` doubles as a context manager for temporary global scoping (`with conn.useGlobal(): ...`).
- **GSQL reserved keyword helpers** — `getReservedKeywords()` and `isReservedKeyword(name)` static methods to query the canonical set of GSQL reserved keywords.
- **Conda build support** — `build.sh` now supports `--conda-build`, `--conda-upload`, `--conda-all`, and `--conda-forge-test` for building and validating conda packages.

### Fixed

- **`_refresh_auth_headers()` init ordering** — auth header cache is now built immediately after credentials are set, before the tgCloud ping and JWT verification. Prevents `AttributeError` on `_cached_token_auth` when connecting without a token (e.g. `TigerGraphConnection(host=..., username=..., password=...)`).
- **Boolean query parameters causing `yarl` errors** — `upsertEdge()`, `upsertEdges()` (`vertexMustExist`), `getVersion()` (`verbose`), and `rebuildGraph()` (`force`) now convert boolean values to lowercase strings before passing them as URL query parameters.
- **`dropVertices()`** now correctly falls back to `self.graphname` when the `graph` parameter is `None`.
- **`dropAllDataSources()`** now correctly uses `self.graphname` fallback for the 4.x REST API path.
- **`getVectorIndexStatus()`** no longer produces a malformed URL when called without a graph name; now supports global scope (returns status for all graphs).
- **`previewSampleData()`** now raises `TigerGraphException` when no graph name is available, instead of sending an empty graph name to the server.
- **Docstring fixes** — corrected `timeout` parameter descriptions across vertex and edge query methods.

---

## [2.0.1] - 2026-03-23

### Breaking Changes

- **`showSecrets()` removed.** Use `getSecrets()` instead.
- **`runInstalledQuery()` now auto-selects GET or POST** based on `params` type (`dict` → POST, `str` → GET). Passing a raw query string with `usePost=True` raises `TigerGraphException`.
- **MCP tools moved to [`pytigergraph-mcp`](https://github.com/tigergraph/pytigergraph-mcp).** Do not import from `pyTigerGraph.mcp` directly.

### New Features

- **Unified vertex parameter syntax for `runInstalledQuery()`.** Use `(id,)` for `VERTEX<T>`, `(id, "type")` for untyped `VERTEX`, and lists of tuples for sets. Works identically for both GET and POST.
- **MAP parameter support.** Pass a Python `dict` for a `MAP` query parameter; it is converted to TigerGraph's wire format automatically.
- **`getVectorIndexStatus()`** — poll vector index build status without writing raw GSQL.
- **`runSchemaChange()`** — run GSQL DDL as a schema-change job via a single API call.
- **Data-source management APIs** — `createDataSource()`, `updateDataSource()`, `getDataSource()`, `getDataSources()`, `dropDataSource()`, `dropAllDataSources()`.
- **Improved performance for parallel workloads.** Sync connections use per-thread HTTP sessions; async connections use `aiohttp`. Both reduce contention and improve throughput under concurrent load.
- **TigerGraph 3.x compatibility.** Queries, loading jobs, and schema operations automatically fall back to 3.x `gsqlserver` endpoints, so the same client code works on both 3.x and 4.x.

### Fixed

- **Edge upsert `vertexMustExist`** flag now correctly forwarded to TigerGraph in all code paths.
- **Edge upsert attribute payloads** serialized correctly for all attribute types.

---

## [2.0.0] - 2025-03-04

### Added

- **MCP (Model Context Protocol) tools.** pyTigerGraph now ships with built-in MCP tool definitions, enabling integration with MCP-compatible AI frameworks.

---

## [1.9.1] - 2024-11-04

### Changed

- API enhancements.

---

## [1.9.0] - 2025-06-30

### Changed

- Multiple API enhancements.

---

## [1.8.4] - 2025-01-20

### Fixed

- Fixed URL construction when `gsPort` and `restppPort` are set to the same value.

---

## [1.8.3] - 2024-12-04

### Fixed

- Fixed `httpx` timeout during async function calls, most notably when installing a query via `.gsql()`.

---

## [1.8.1] - 2024-11-19

### Fixed

- Fixed import error of `TigerGraphException` in the GDS submodule.

---

## [1.8.0] - 2024-11-04

### Added

- **`AsyncTigerGraphConnection`** — full async communication with TigerGraph using the new `AsyncTigerGraphConnection` class.
- **`delVerticesByType()`** — delete all vertices of a given type in one call.
- **`limit` parameter for `getEdgesByType()`** — cap the number of edges returned. Note: the limit is applied client-side after retrieval.
- **Upsert atomicity configuration** — new parameters to control atomicity behaviour of upsert operations.
- **`runLoadingJobWithDataFrame()`** — run a GSQL loading job directly from a Pandas DataFrame.
- **`runLoadingJobWithData()`** — run a GSQL loading job from a raw data string.

---

## [1.7.4] - 2024-10-16

### Fixed

- Fixed error when generating a token via `getToken()` with a secret key.

---

## [1.7.3] - 2024-10-14

### Fixed

- Fixed error when generating a token via `getToken()` on TigerGraph Cloud v3.x instances.

---

## [1.7.2] - 2024-10-01

### Added

- **`delVerticesByType()`** — delete all vertices of a specified type. Supports `permanent` (prevent re-insertion of the same IDs) and `ack` (`"all"` or `"none"`) parameters.

---

## [1.1] - 2022-09-06

Release of pyTigerGraph version 1.1. 

## Added:
* TensorFlow support for homogeneous GNNs via the Spektral library.
* Heterogeneous Graph Dataloading support for DGL.
* Support of lists of strings in dataloaders.

## Changed:
* Fixed KeyError when creating a data loader on a graph where PrimaryIdAsAttribute is False.
* Error catch if Kafka dataloader doesn't run in async mode.
* Refresh schema during dataloader instantiation and featurizer attribute addition.
* Reduce connection instantiation time.
* Reinstall query if it is disabled.
* Confirm Kafka topic is created before subscription.
* More efficient use of Kafka resources.
* Allow multiple consumers on the same data.
* Improved deprecation warnings.

## [1.0] - 2022-07-11

Release of pyTigerGraph version 1.0, in conjunction with version 1.0 of the link:https://docs.tigergraph.com/ml-workbench/current/overview/[TigerGraph Machine Learning Workbench]. 

## Added:
* Kafka authentication support for ML Workbench enterprise users.
* Custom query support for Featurizer, allowing developers to generate their own graph-based features as well as use our link:https://docs.tigergraph.com/graph-ml/current/intro/[built-in Graph Data Science algorithms].

## Changed:
* Additional testing of GDS functionality
* More demos and tutorials for TigerGraph ML Workbench, found link:https://github.com/TigerGraph-DevLabs/mlworkbench-docs[here].
* Various bug fixes.

## [0.9] - 2022-05-16
We are excited to announce the pyTigerGraph v0.9 release! This release adds many new features for graph machine learning and graph data science, a refactoring of core code, and more robust testing. Additionally, we have officially “graduated” it to an official TigerGraph product. This means brand-new documentation, a new GitHub repository, and future feature enhancements. While becoming an official product, we are committed to keeping pyTigerGraph true to its roots as an open-source project. Check out the contributing page and GitHub issues if you want to help with pyTigerGraph’s development. 
## Changed
* Feature: Include Graph Data Science Capability
    - Many new capabilities added for graph data science and graph machine learning. Highlights include data loaders for training Graph Neural Networks in DGL and PyTorch Geometric, a "featurizer" to generate graph-based features for machine learning, and utilities to support those activities.

* Documentation: We have moved the documentation to [the official TigerGraph Documentation site](https://docs.tigergraph.com/pytigergraph/current/intro/) and updated many of the contents with type hints and more descriptive parameter explanations.

* Testing: There is now well-defined testing for every function in the package. A more defined testing framework is coming soon.

* Code Structure: A major refactor of the codebase was performed. No breaking changes were made to accomplish this.

## [0.0.9.7.8] - 2021-09-27
## Changed
* Fix :  added safeChar method to fix URL encoding

## [0.0.9.7.7] - 2021-09-20
## Changed
* Fix :  removed the localhost to 127.0.0.1 translation


## [0.0.9.7.6] - 2021-09-01
## Changed
* Fix :  SSL issue with Rest++ for self-signed certs 
* Fix :  Updates for pyTigerDriver bounding 
* Feature : added the checks to debug
* Fix :  added USE GRAPH cookie

## [0.0.9.7.0] - 2021-07-07
### Changed
* runInstalledQuery(usePost=True) will post params as body 


## [0.0.9.6.9] - 2021-06-03
### Changed
* Made SSL Port configurable to grab SSL cert from different port in case of firewall on 443


## [0.0.9.6.3] - 2020-12-14
### Fix : 
* Fix :  (more) runInstalledQuery() params 

## [0.0.9.6.2] - 2020-10-08
### Fix : 
* Fix :  (more) runInstalledQuery() params processing bugs


## [0.0.9.6] - 2020-10-08
### Fix : 
* Fix :  (more) runInstalledQuery() params processing bugs


## [0.0.9.5] - 2020-10-07
### Fix : 
* Fix :  runInstalledQuery() params processing


## [0.0.9.4] - 2020-10-03
### Changed
* Add Path finding endpoint
* Add Full schema retrieval

### Fix : 
* Fix GSQL client
* Fix parseQueryOutput
* Code cleanup

## [0.0.9.3] - 2020-09-30
### Changed
* Remove urllib as dependency
### Fix : 

## [0.0.9.2] - 2020-09-30
### Changed
* Fix space in query param issue #22
### Fix : 

## [0.0.9.1] - 2020-09-03
### Changed
* SSL Cert support on REST requests
### Fix : 

## [0.0.9.0] - 2020-08-22
### Changed
### Fix : 
* Fix getVertexDataframeById()
* Fix GSQL versioning issue

## [0.0.8.4] - 2020-08-19
### Changed
### Fix : 
* Fix GSQL Bug

## [0.0.8.4] - 2020-08-19
### Changed
### Fix : 
* Fix GSQL getVer() bug

## [0.0.8.3] - 2020-08-08
### Changed
### Fix : 
* Fix initialization of gsql bug

## [0.0.8.2] - 2020-08-08
### Changed
### Fix : 
* Fix initialization of gsql bug

## [0.0.8.1] - 2020-08-08
### Changed
### Fix : 
* Fix bug in gsqlInit()

## [0.0.8.0] - 2020-08-07
### Changed
* Add getVertexSet()
### Fix : 

## [0.0.7.0] - 2020-07-26
### Changed
* Move GSQL functionality to main package
### Fix : 

## [0.0.6.9] - 2020-07-23
### Changed
* Main functionality exists and is in relatively stable
### Fix : 
* Minor bug fixes

