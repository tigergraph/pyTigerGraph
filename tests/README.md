# pyTigerGraph Unit Tests

This folder contains all the unit tests for pyTigerGraph. The `test_pyTigerGraph*` files are tests for the core functions, and the `test_gds_*` files are tests for the GDS functions.

## Preparation

Most unit tests need an accessible TigerGraph database with a specific graph. Not all tests work with all the versions of DB. To simplify the testing environment, a few docker images for the DB is provided at `tginternal/ml-qa-db` (permission required), which has all the data pre-loaded. It is also used by our CI/CD pipeline.

If you need to manually prepare a DB for testing, run the `testserver.gsql` script to create the graph for testing core functions (via the `gsql` command line tool; GraphStudio cannot be used.) The script will create a graph called "tests" and will populate it with various object types and some data.

⚠️ **NOTE**: The script drops all existing graphs and objects, so use it with a TigerGraph instance
that does not have operational or otherwise important data, schema design or code.

About testing data for the GDS functions, please contact one of the maintainers. 

## TigerGraph connection configuration file

The test suite (obviously) need to connect to a TigerGraph instance. The unit tests first look for
a configuration file called `testserver.json` and if it exists, the configuration parameters in it
will be used. If the file cannot be found then hardcoded defaults will be applied.

### The `testserver.json` file
This configuration file is a simple json file with DB configs.
The following keys are used:
* host (str)
* graphname (str)
* username (str)
* password (str)
* restppPort (str, )
* gsPort (str, int)
* gsqlVersion (str)
* useCert (boolean)
* certPath (str)
* sslPort (str, int)
* gcp (boolean)

See [`testserver.json`](testserver.json) for a template/example.

### Hard coded defaults
```
{
        "host": "http://127.0.0.1",
        "graphname": "tests",
        "username": "tigergraph",
        "password": "tigergraph",
        "gsqlSecret": "",
        "restppPort": "9000",
        "gsPort": "14240",
        "gsqlVersion": "",
        "userCert": None,
        "certPath": None,
        "sslPort": "443",
        "tgCloud": False,
        "gcp": False,
    }
```
