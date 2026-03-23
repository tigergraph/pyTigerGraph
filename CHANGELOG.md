# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.1] - 2026-03-23

### Breaking Changes

- **`showSecrets()` has been removed.** Replace all calls with `getSecrets()`.
- **`usePost` default changed from `False` to `None`** in `runInstalledQuery()`. The transport is now auto-selected based on `params` type (dict → POST, string → GET). Code that relied on dict params always going through POST is unaffected; code that passed a raw query string and expected POST will now use GET instead.
- **`usePost=True` with a string `params` now raises `TigerGraphException`** instead of silently sending a malformed request body. Convert the string to a dict or drop `usePost=True` instead.

### New Features

- **Unified vertex parameter syntax for `runInstalledQuery()`.** The same tuple notation now works correctly for both GET and POST — no need to format params differently depending on `usePost`:
  - `(id,)` — typed vertex `VERTEX<T>`
  - `(id, "type")` — untyped vertex `VERTEX`
  - `[(id,), ...]` — typed vertex set `SET<VERTEX<T>>`
  - `[(id, "type"), ...]` — untyped vertex set `SET<VERTEX>`
- **MAP query parameter support.** Pass a Python `dict` directly for a `MAP` parameter; it is converted to TigerGraph's wire format automatically. A dict with an `"id"` key is treated as a pre-formatted vertex object and passed through unchanged.

### Compatibility Notes

- **Old-style plain IDs for typed vertex params still work**, but at a cost. If you pass `{"p": 1}` instead of `{"p": (1,)}` for a `VERTEX<T>` parameter, `runInstalledQuery()` catches the server-side rejection and retries transparently via GET, logging a warning. Each such call incurs one extra HTTP round-trip. Migrate to `(id,)` tuples to eliminate the overhead.
- **`(id, "")` (empty type string) now raises immediately** on the client instead of forwarding to TigerGraph. Update any code relying on the server-side error to handle the client-side `TigerGraphException` instead.
- **MCP tools have moved to [`pytigergraph-mcp`](https://github.com/tigergraph/pytigergraph-mcp).** Do not import from `pyTigerGraph.mcp` directly; install and use the dedicated `pytigergraph-mcp` package instead.

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

