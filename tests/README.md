# pyTigerGraph Unit Tests

This folder contains all the unit tests for pyTigerGraph. The `test_pyTigerGraph*` files are tests for the core functions, and the `test_gds_*` files are tests for the GDS functions.

## Preparation

Most unit tests need an accessible TigerGraph database with a specific graph. Not all tests work with all the versions of DB nor DB environments (e.g., on-prem vs. cloud). To simplify the testing environment, a docker image for the DB is provided at `tigergraphml/qa-db:3.7.0`, which has all data pre-loaded. It is also used by the CI/CD pipeline.

If you need to manually prepare a DB for testing, run the `testserver.gsql` script to create the graph for testing core functions (via the `gsql` command line tool; GraphStudio cannot be used.) The script will create a graph called "tests" and will populate it with various object types and some data.

⚠️ **NOTE**: The script drops all existing graphs and objects, so use it with a TigerGraph instance
that does not have operational or otherwise important data, schema design or code.

About testing data for the GDS functions, please contact one of the maintainers. 

## TigerGraph connection configuration file

The test suite (obviously) need to connect to a TigerGraph instance. The unit tests first look for
a configuration file called `testserver.cfg` and if it exists, the configuration parameters in it
will be used. If the file cannot be found then hardcoded defaults will be applied.

### The `testserver.cfg` file
This configuration file is a simple text file with a simple assigment per line, with the parameter
name and value separated by a single `=` operator (no spaces before or after). For example:
```
host=https://my-tgc-instance.i.tgcloud.io/
graphname=tests
```
The following parameter names are accepted:
* host (str)
* graphname (str)
* username (str)
* password (str)
* restppPort (str, int)
* gsPort (str, int)
* gsqlVersion (str)
* useCert (str: "True", "False")
* certPath (str)
* sslPort (str, int)
* gcp (str: "True", "False")

See [`testserver.cfg`](testserver.cfg) for a template/example.


Notes:
* You need to specify only those parameters that are different from the defaults (see below).
* If there are more than one `=` operators in a line, all values after the second `=` are ignored.
* Comment lines start with `#` and are ignored.
* Paramer names other than the above listed ones are ignored.

### Hard coded defaults
* host=http://127.0.0.1
* graphname=tests
* username=tigergraph
* password=tigergraph
* restppPort=9000
* gsPort=14240
* gsqlVersion=""
* userCert=True
* certPath=""
* sslPort=443
* gcp=False
