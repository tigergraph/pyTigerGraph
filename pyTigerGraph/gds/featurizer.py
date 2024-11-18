"""Featurizer
The Featurizer class provides methods for installing and running Graph Data Science Algorithms onto a TigerGraph server.

To use the Featurizer, you must first create a connection to a TigerGraph server using the `TigerGraphConnection` class.

For example, to run PageRank, you would use the following code:

[source,python]
----
import pyTigerGraph as tg

conn = tg.TigerGraphConnection(host="HOSTNAME_HERE", username="USERNAME_HERE", password="PASSWORD_HERE", graphname="GRAPHNAME_HERE")

conn.getToken()

feat = conn.gds.featurizer()

res = feat.runAlgorithm("tg_pagerank", params={"v_type": "Paper", "e_type": "CITES"})
print(res)
----
"""

from typing import TYPE_CHECKING, Any, List, Tuple

if TYPE_CHECKING:
    from ..pyTigerGraph import TigerGraphConnection

from ..common.exception import TigerGraphException
import json
import re
import time
from os.path import join as pjoin

import requests

from .utilities import is_query_installed, random_string, add_attribute


class AsyncFeaturizerResult:
    """AsyncFeaturizerResult
    Object to keep track of featurizer algorithms being ran in asynchronous mode. (`runAsync=True`).
    """

    def __init__(self, conn, algorithm, query_id, results=None):
        """NO DOC:
        class for asynchronous featurizer results. Populated during `runAlgorithm()` if `runAsync = True`.
        """
        self.conn = conn
        self.algorithm = algorithm
        self.query_id = query_id
        self.results = results

    def wait(self, refresh: float = 1):
        """
        Function call to block all execution if called until algorithm result is returned.
        Args:
            refresh (float):
                How often to check for results. Defaults to 1 time every second.

        Returns:
            Algorithm results when they become available.
        """
        while not self.results:
            if self.algorithmComplete():
                return self._getAlgorithmResults()
            time.sleep(refresh)
        return self.results

    def algorithmComplete(self):
        """
        Function to check if the algorithm has completed execution.
        Returns:
            True if algorithm has completed, False if the algorithm is still running.
        Raises:
            TigerGraphException if the algorithm was aborted or timed out.
        """
        res = self.conn.checkQueryStatus(self.query_id)[0]
        if res["status"] == "success":
            return True
        elif res["status"] == "running":
            return False
        elif res["status"] == "aborted":
            raise TigerGraphException("Algorithm was aborted")
        else:
            raise TigerGraphException(
                "Algorithm timed-out. Increase your timeout and try again."
            )

    def _getAlgorithmResults(self):
        """NO DOC: internal function to get algorithm results."""
        res = self.conn.getQueryResult(self.query_id)
        self.results = res
        return res

    @property
    def result(self):
        """
        Property to get the results of an algorithm's execution.
        If the results are available, returns them.
        If the results are not available yet, returns the string 'Algorithm Results not Available Yet'
        """
        if self.results:
            return self.results
        else:
            if self.algorithmComplete():
                return self._getAlgorithmResults()
            else:
                return "Algorithm Results not Available Yet"


class Featurizer:
    """The Featurizer enables installation and execution of algorithms in the Graph Data Science (GDS) libarary. 
    The Featurizer pulls the most up-to-date version of the algorithm available in our public GitHub repository that is
    compatible with your database version.
    Note: In environments not connected to the public internet, you can download the repository manually and use the featurizer
    like this:
    ```
    import pyTigerGraph as tg
    from pyTigerGraph.gds.featurizer import Featurizer

    conn = tg.TigerGraphConnection(host="HOSTNAME_HERE", username="USERNAME_HERE", password="PASSWORD_HERE", graphname="GRAPHNAME_HERE")
    conn.getToken(conn.createSecret())
    feat = Featurizer(conn, repo="PATH/TO/MANUALLY_DOWNLOADED_REPOSITORY")

    res = feat.runAlgorithm("tg_pagerank", params={"v_type": "Paper", "e_type": "CITES"})

    print(res)
    ```
    """

    def __init__(
        self, conn: "TigerGraphConnection", repo: str = None, algo_version: str = None
    ):
        """NO DOC: Class for feature extraction.

        The job of a feature extracter is to install and run algorithms in the Graph Data Science (GDS) libarary.
        Currently, a set of graph algorithms are moved to the `gsql` folder, which you can find in the same directory as this file,
         and have been saved into a dictionary along with their output type.
        To add a specific algorithm, it should be added both to the `gsql` folder and class variable dictionary.
        Args:
            conn (TigerGraphConnection):
                Connection to the TigerGraph database.
        """

        self.conn = conn
        # Get DB version if algo version is not given
        if not algo_version:
            self.major_ver, self.minor_ver, self.patch_ver = self._get_db_version()
        else:
            self.algo_ver = algo_version
            self.major_ver, self.minor_ver = algo_version.split(".")[:2]
        # Get repo address
        if not repo:
            if self.major_ver and self.minor_ver:
                repo = "https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/{}.{}".format(
                    self.major_ver, self.minor_ver
                )
            elif self.major_ver == "master":
                repo = "https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/{}".format(
                    self.major_ver
                )
            else:
                raise ValueError(
                    "Database version {} not supported.".format(self.algo_ver))
        self.repo = repo
        # Get algo dict from manifest
        try:
            manifest = pjoin(repo, "manifest.json")
            self.algo_dict = self._get_algo_dict(manifest)
        except:
            print("Cannot read manifest file. Trying master branch.")
            repo = "https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/master"
            manifest = pjoin(repo, "manifest.json")
            self.algo_dict = self._get_algo_dict(manifest)
            self.repo = repo

        self.algo_paths = None

        self.query = None
        self.query_name = None
        self.query_result_type = None
        self.sch_type = None
        self.template_queries = {}

    def _get_db_version(self) -> Tuple[str, str, str]:
        # Get DB version
        try:
            self.algo_ver = self.conn.getVer()
            major_ver, minor_ver, patch_ver = self.algo_ver.split(".")
            if int(major_ver) < 3 or (int(major_ver) == 3 and int(minor_ver) <= 7):
                # For DB version <= 3.7, use version 3.7.
                major_ver = "3"
                minor_ver = "7"
        except AttributeError:
            version = self.conn.getVersion()
            self.algo_ver = version[0]["version"]
            major_ver, minor_ver, patch_ver = self.algo_ver, "", ""
        return major_ver, minor_ver, patch_ver

    def _get_algo_dict(self, manifest_file: str) -> dict:
        # Get algo dict from manifest
        if manifest_file.startswith("http"):
            resp = requests.get(
                manifest_file, allow_redirects=False, timeout=10)
            resp.raise_for_status()
            algo_dict = resp.json()
        else:
            with open(manifest_file) as infile:
                algo_dict = json.load(infile)
        return algo_dict

    def listAlgorithms(self, category: str = None) -> None:
        """
        Print the list of available algorithms in GDS.

        Args:
            category (str):
                the category of algorithms to print, if it is None then a summary will be printed.
        """

        def get_num_algos(algo_dict: dict) -> int:
            if "name" in algo_dict.keys():
                return 1
            num_algos = 0
            for k in algo_dict:
                if isinstance(algo_dict[k], dict):
                    num_algos += get_num_algos(algo_dict[k])
            return num_algos

        def print_algos(algo_dict: dict, depth: int, algo_num: int = 0) -> int:
            for k, v in algo_dict.items():
                if k == "name":
                    algo_num += 1
                    print("{}{:02}. name: {}".format(
                        "  " * depth, algo_num, v))
                    return algo_num
                if isinstance(v, dict):
                    print("{}{}:".format("  " * depth, k))
                    algo_num = print_algos(v, depth + 1, algo_num)
            return algo_num

        if category:
            if category in self.algo_dict.keys():
                print("Available algorithms for {}:".format(category))
                print_algos(self.algo_dict[category], 1)
                print("Call runAlgorithm() with the algorithm name to execute it")
            else:
                print("No available algorithms for category {}".format(category))
        else:
            print("Available algorithms per category:")
            for k in self.algo_dict:
                print("- {}: {} algorithms".format(k,
                      get_num_algos(self.algo_dict[k])))
            print(
                "Call listAlgorithms() with the category name to see the list of algorithms"
            )

    def _install_query_file(
        self,
        query_path: str,
        replace: dict = None,
        force: bool = False,
        global_change: bool = False,
        distributed_mode: bool = False
    ) -> str:
        """
        Reads the first line of the query file to get the query name, e.g, CREATE QUERY query_name ...

        Args:
            query_name (str):
                The name of the query
            replace (dict):
                If the suffix name needs to be replaced
            global_change (bool):
                False by default. Set to true if you want to run `GLOBAL SCHEMA_CHANGE JOB`. For Algorithms that are not schema free we need to specify this argument.
                See https://docs.tigergraph.com/gsql-ref/current/ddl-and-loading/modifying-a-graph-schema#_global_vs_local_schema_changes.
            distributed_mode (bool):
                False by default. Set to true if DISTRIBUTED algorithm execution is desired.

        Return:
            Name of the installed query
        """
        # Read in the query
        if query_path.startswith("http"):
            resp = requests.get(query_path, allow_redirects=False, timeout=10)
            resp.raise_for_status()
            query = resp.text
        else:
            with open(query_path) as f:
                query = f.read()
        # Get query name from the first line
        firstline = query.split("\n", 1)[0]
        try:
            query_name = re.search(
                r"QUERY (.+?)\(", firstline).group(1).strip()
        except:
            raise ValueError(
                "Cannot parse the query file. It should start with CREATE QUERY ... "
            )
        # If query is already installed, skip unless force install.
        is_installed, is_enabled = is_query_installed(
            self.conn, query_name, return_status=True
        )
        if is_installed:
            if force or (not is_enabled):
                query = "USE GRAPH {}\nDROP QUERY {}\n".format(
                    self.conn.graphname, query_name
                )
                resp = self.conn.gsql(query)
                status = resp.splitlines()[-1]
                if "Failed" in status:
                    raise ConnectionError(resp)
            else:
                return query_name
        # Replace placeholders with actual content if given
        if replace:
            for placeholder in replace:
                query = query.replace(placeholder, replace[placeholder])
        self.query = query
        if (
            query_name == "tg_fastRP"
            and self.major_ver != "master"
            and int(self.major_ver) <= 3
            and int(self.minor_ver) <= 7
        ):
            # Drop all jobs on the graph
            self.conn.gsql("USE GRAPH {}\n".format(
                self.conn.graphname) + "drop job *")
            res = add_attribute(
                self.conn,
                schema_type="VERTEX",
                attr_type="LIST<DOUBLE>",
                attr_name="embedding",
                global_change=global_change,
            )
        if distributed_mode:
            query.replace("CREATE QUERY", "CREATE DISTRIBUTED QUERY")
        query = (
            "USE GRAPH {}\n".format(self.conn.graphname)
            + query
            + "\nInstall Query {}\n".format(query_name)
        )
        print(
            "Installing and optimizing the queries, it might take a minute...",
            flush=True,
        )
        resp = self.conn.gsql(query)
        status = resp.splitlines()[-1]
        if "Failed" in status:
            raise ConnectionError(resp)
        print("Queries installed successfully", flush=True)
        return query_name

    def installAlgorithm(
        self, query_name: str, query_path: str = None, global_change: bool = False,
        distributed_query: bool = False
    ) -> str:
        """
        Checks if the query is already installed.
        If the query is not installed, it installs the query and changes the schema if an attribute needs to be added.

        Args:
            query_name (str):
                The name of query to be installed.
            query_path (str, optional):
                If using a custom query, the path to the `.gsql` file that contains the query.
                Note: you must have the `query_name` parameter match the name of the query in the file.
            global_change (bool, optional):
                False by default. Set to true if you want to run `GLOBAL SCHEMA_CHANGE JOB`. For algorithms that are not schema free we need to specify this argument.
                See https://docs.tigergraph.com/gsql-ref/current/ddl-and-loading/modifying-a-graph-schema#_global_vs_local_schema_changes.
            distributed_query (bool, optional):
                False by default. 
        Returns:
            String of query name installed.
        """
        # If a query file is given, install it directly.
        if query_path:
            self.query_name = self._install_query_file(
                query_path, global_change=global_change, distributed_mode=distributed_query
            )
            return self.query_name
        # Else, install query by name from the repo.
        # If the query paths are not collected yet, do it now.
        if not self.algo_paths:
            (
                self.algo_paths,
                self.query_result_type,
                self.sch_type,
            ) = self._get_algo_details(self.algo_dict)
        if query_name not in self.algo_paths:
            raise ValueError(
                "Cannot find {} in the library.".format(query_name))
        for query in self.algo_paths[query_name]:
            _ = self._install_query_file(
                query, global_change=global_change, distributed_mode=distributed_query)
        self.query_name = query_name
        return self.query_name

    def _get_algo_details(self, algo_dict: dict) -> dict:
        def get_details(d: dict, paths: dict, types: dict, sch_obj: dict) -> None:
            if "name" in d.keys():
                if "path" not in d.keys():
                    raise Exception(
                        "Cannot find path for {} in the manifest file".format(
                            d["name"])
                    )
                paths[d["name"]] = [pjoin(self.repo, p)
                                    for p in d["path"].split(";")]
                if "value_type" in d.keys():
                    types[d["name"]] = d["value_type"]
                if "schema_type" in d.keys():
                    sch_obj[d["name"]] = d["schema_type"]
                return
            for k in d:
                if isinstance(d[k], dict):
                    get_details(d[k], paths, types, sch_obj)
            return

        algo_paths = {}
        algo_result_types = {}
        sch_types = {}
        get_details(algo_dict, algo_paths, algo_result_types, sch_types)
        return algo_paths, algo_result_types, sch_types

    def _get_query(self, query_name: str) -> str:
        if not self.algo_paths:
            (
                self.algo_paths,
                self.query_result_type,
                self.sch_type,
            ) = self._get_algo_details(self.algo_dict)
        if query_name not in self.algo_paths:
            raise ValueError(
                "Cannot find {} in the library.".format(query_name))
        query_path = self.algo_paths[query_name][-1]
        if query_path.startswith("http"):
            resp = requests.get(query_path, allow_redirects=False, timeout=10)
            resp.raise_for_status()
            query = resp.text
        else:
            with open(query_path) as f:
                query = f.read()
        return query

    def getParams(self, query_name: str, printout: bool = True) -> dict:
        """Get paramters for an algorithm.

        Args:
            query_name (str):
                Name of the algorithm.
            printout (bool, optional):
                Whether to print out the parameters. Defaults to True.

        Returns:
            Parameter dict the algorithm takes as input.
        """
        query = self._get_query(query_name)
        param_values, param_types = self._get_params(query)
        if printout:
            print(
                "Parameters for {} (parameter: type [= default value]):".format(
                    query_name
                )
            )
            for param in param_values:
                if param_values[param] is not None:
                    if param_types[param] == "str":
                        print(
                            '- {}: {} = "{}"'.format(
                                param, param_types[param], param_values[param]
                            )
                        )
                    else:
                        print(
                            "- {}: {} = {}".format(
                                param, param_types[param], param_values[param]
                            )
                        )
                else:
                    print("- {}: {}".format(param, param_types[param]))
        return param_values

    def _get_params(self, query: str):
        """
        Returns query parameters and their types by parsing the query header.

        Args:
            query (str):
                Content of the query as a string.
        """
        param_values = {}
        param_types = {}
        header = query[query.find("(") + 1: query.find(")")].strip()
        if not header:
            return {}, {}
        header = header.split(",")
        for i in header:
            param_type, param_raw = i.strip().split(maxsplit=1)
            param_type = param_type.strip()
            if "=" in param_raw:
                param, default = param_raw.strip().split("=")
                param = param.strip()
                default = default.strip()
            else:
                param, default = param_raw.strip(), None

            if param_type.lower() == "float" or param_type.lower() == "double":
                param_values[param] = float(default) if default else None
                param_types[param] = "float"
            elif param_type.lower() == "int":
                param_values[param] = int(default) if default else None
                param_types[param] = "int"
            elif param_type.lower() == "bool":
                if default and default.lower() == "true":
                    param_values[param] = True
                elif default and default.lower() == "false":
                    param_values[param] = False
                else:
                    param_values[param] = None
                param_types[param] = "bool"
            elif param_type.lower() == "string":
                param_values[param] = default.strip(
                    '"').strip("'") if default else None
                param_types[param] = "str"
            else:
                param_values[param] = default
                param_types[param] = param_type

        return param_values, param_types

    def runAlgorithm(
        self,
        query_name: str,
        params: dict = None,
        runAsync: bool = False,
        threadLimit: int = None,
        memoryLimit: int = None,
        feat_name: str = None,
        feat_type: str = None,
        custom_query: bool = False,
        schema_name: list = None,
        global_schema: bool = False,
        timeout: int = 2147480,
        sizeLimit: int = None,
        templateQuery: bool = False,
        distributed_query: bool = False
    ) -> Any:
        """
        Runs a TigerGraph Graph Data Science Algorithm. If a built-in algorithm is not installed, it will automatically install before execution.
        Custom algorithms will have to be installed using the `installAlgorithm()` method.
        If the query accepts input parameters and the parameters have not been provided, calling this function runs the query with the default values for the parameters.
        If the there isn't a default value in the query definition and no parameters are provided, the function raises a `ValueError`.

        Args:
            query_name (str):
                The name of the query to be executed.
            params (dict):
                Query parameters. A dictionary that corresponds to the algorithm parameters.
                If specifying vertices as sources or destinations, must use the following form:
                `{"id": "vertex_id", "type": "vertex_type"}`, such as `params = {"source": {"id": "Bob", "type": "Person"}}`
            runAsync (bool, optional):
                If True, runs the algorithm in asynchronous mode and returns a `AsyncFeaturizerResult` object. Defaults to False.
            threadLimit:
                Specify a limit of the number of threads the query is allowed to use on each node of the TigerGraph cluster.
                See xref:tigergraph-server:API:built-in-endpoints#_specify_thread_limit[Thread limit]
            memoryLimit:
                Specify a limit to the amount of memory consumed by the query (in MB). If the limit is exceeded, the query will abort automatically.
                Supported in database versions >= 3.8.
                See xref:tigergraph-server:system-management:memory-management#_by_http_header[Memory limit]
            feat_name (str, optional):
                An attribute name that needs to be added to the vertex/edge. If the result attribute parameter is specified in the parameters, that will be used.
            feat_type (str, optional):
                Type of attribute that needs to be added to the vertex/edge. Only needed if `custom_query` is set to `True`.
            custom_query (bool, optional):
                If the query is a custom query. Defaults to False.
            schema_name (list, optional):
                List of Vertices/Edges that the attr_name need to added to them.
                If the algorithm contains the parameters of `v_type` and `e_type` or `v_type_set` and `e_type_set`, these will be used automatically.
            global_schema (bool, optional):
                False by default. Set to true if you want to run `GLOBAL SCHEMA_CHANGE JOB`.
                See https://docs.tigergraph.com/gsql-ref/current/ddl-and-loading/modifying-a-graph-schema#_global_vs_local_schema_changes.
            timeout (int, optional):
                Maximum duration for successful query execution (in milliseconds).
            sizeLimit (int, optional):
                Maximum size of response (in bytes).
            templateQuery (bool, optional):
                Whether to call packaged template query. See https://docs.tigergraph.com/graph-ml/current/using-an-algorithm/#_packaged_template_queries for more details.
                Note that currently not every algorithm supports template query. More will be added in the future.
                Default: False.
            distributed_query (bool, optional):
                Whether to run the query in distributed mode. Defaults to False.

        Returns:
            The output of the query, a list of output elements (vertex sets, edge sets, variables,
            accumulators, etc.)
        """
        # If template query is used
        if templateQuery:
            # Check if DB version is >= 3.9.
            if self.major_ver != "master" and (int(self.major_ver) < 3 or (
                int(self.major_ver) == 3 and int(self.minor_ver) < 9)
            ):
                raise ValueError(
                    "Template query is only avaiable for database version 3.9 and above."
                )
            # Check if GDBMS_ALGO is imported. If not, import.
            if "GDBMS_ALGO" not in self.conn.gsql("SHOW PACKAGE"):
                _ = self.conn.gsql("IMPORT PACKAGE GDBMS_ALGO")
            # Check if query_name has a template query.
            if not self.template_queries:
                self._get_template_queries()
            if not query_name.startswith("tg_"):
                query_name = "tg_" + query_name
            temp_query_name = query_name[3:]
            found_query = False
            for category, queries in self.template_queries.items():
                if temp_query_name in queries:
                    found_query = True
                    break
            if not found_query:
                raise ValueError(
                    "Template query {} is not available currently.".format(
                        temp_query_name
                    )
                )
            # Change schema if needed.
            if params.get("result_attribute", None):
                if not self.query_result_type:
                    (
                        self.algo_paths,
                        self.query_result_type,
                        self.sch_type,
                    ) = self._get_algo_details(self.algo_dict)
                self._add_result_attribute(query_name, params)
            # Convert vertex dict in params to the right format
            vertex = params.get("v_start", None)
            if vertex:
                params = params.copy()
                params["v_start"] = vertex["id"]
                params["v_start.type"] = vertex["type"]
            vertex = params.get("source", None)
            if vertex:
                params = params.copy()
                params["source"] = vertex["id"]
                params["source.type"] = vertex["type"]
            # Finally, run the query
            print("Running the algorithm. It might take a minute to install the query if this is the first time it runs.")
            resp = self.conn._post(
                "{}:{}/gsqlserver/gsql/library?graph={}&functionName=GDBMS_ALGO.{}.{}".format(
                    self.conn.host,
                    self.conn.gsPort,
                    self.conn.graphname,
                    category,
                    temp_query_name,
                ),
                data=params,
                headers={"GSQL-TIMEOUT": str(timeout)},
                jsonData=True,
            )
            if resp["error"]:
                raise TigerGraphException(resp["message"])
            return resp["results"]
        # If use non-template query
        # Check if query is installed. If not, install if it is built-in.
        if not is_query_installed(self.conn, query_name):
            if custom_query:
                raise ValueError(
                    "Please run installAlgorithm() to install this custom query first."
                )
            self.installAlgorithm(
                query_name, global_change=global_schema, distributed_query=distributed_query)

        # Check query parameters for built-in queries.
        if not custom_query:
            if params is None:
                params = self.getParams(query_name, printout=False)
                if params:
                    missing_params = [
                        k for k, v in params.items() if v is None]
                    if missing_params:
                        raise ValueError(
                            'Missing mandatory parameters: {}. Please run getParams("{}") for parameter details.'.format(
                                list(missing_params), query_name
                            )
                        )
            else:
                query_params = self.getParams(query_name, printout=False)
                unknown_params = set(params.keys()) - set(query_params.keys())
                if unknown_params:
                    raise ValueError(
                        'Unknown parameters: {}. Please run getParams("{}") for required parameters.'.format(
                            list(unknown_params), query_name
                        )
                    )
                query_params.update(params)
                missing_params = [
                    k for k, v in query_params.items() if v is None]
                if missing_params:
                    raise ValueError(
                        'Missing mandatory parameters: {}. Please run getParams("{}") for parameter details.'.format(
                            list(missing_params), query_name
                        )
                    )
                params = query_params
            # Check if there is the attribute to store similarity
            if "similarity_edge" in params:
                if params["similarity_edge"] not in self.conn.getEdgeTypes():
                    raise ValueError(
                        "The edge type "
                        + params["similarity_edge"]
                        + " must be present in the graph schema with a FLOAT attribute to write to it."
                    )
            elif "similarity_edge_type" in params:
                if params["similarity_edge_type"] not in self.conn.getEdgeTypes():
                    raise ValueError(
                        "The edge type "
                        + params["similarity_edge_type"]
                        + " must be present in the graph schema with a FLOAT attribute to write to it."
                    )

        # Change schema to save results if needed
        if custom_query and feat_name:
            # feat_type and schema_name should be provided for custom query.
            if not (feat_type and schema_name):
                raise ValueError(
                    "Please provide feat_type and schema_name if adding attribute for custom query."
                )
            self._add_result_attribute(
                query_name, params, feat_name, feat_type, custom_query, schema_name
            )
        elif not custom_query:
            if not (
                query_name == "tg_fastRP"
                and self.major_ver != "master"
                and int(self.major_ver) <= 3
                and int(self.minor_ver) <= 7
            ):  # fastRP in 3.7 creates attribute at install time
                if params.get("result_attr", None) or params.get("result_attribute", None):
                    self._add_result_attribute(query_name, params)
        # Run query.
        result = self.conn.runInstalledQuery(
            query_name,
            params,
            timeout=timeout,
            sizeLimit=sizeLimit,
            usePost=True,
            runAsync=runAsync,
            threadLimit=threadLimit,
            memoryLimit=memoryLimit
        )
        # Return result
        if result is not None:
            if runAsync:
                return AsyncFeaturizerResult(self.conn, query_name, result)
            else:
                return result

    def _get_template_queries(self):
        categories = self.conn.gsql(
            "SHOW PACKAGE GDBMS_ALGO").strip().split("\n")[2:]
        for cat in categories:
            resp = self.conn.gsql(
                "SHOW PACKAGE GDBMS_ALGO.{}".format(cat.strip("- ")))
            self.template_queries[cat.strip("- ")] = resp.strip()

    def _add_result_attribute(
        self,
        query_name: str,
        params: dict,
        feat_name: str = "",
        feat_type: str = "",
        custom_query: bool = False,
        schema_name: list = [],
    ):
        if custom_query:
            # For custom query, feat_name, feat_type and schema_name should be
            # provided. Infer schema type from schema name.
            if (
                schema_name[0] in self.conn.getEdgeTypes()
            ):  # assuming all schema changes are either edge types or vertex types, no mixing.
                schema_type = "EDGE"
            else:
                schema_type = "VERTEX"
        else:
            # For built-in query, infer feat_name and schema_name from params.
            feat_name = (
                params["result_attr"]
                if "result_attr" in params
                else params["result_attribute"]
            )
            feat_type = self.query_result_type[query_name]
            schema_type = self.sch_type[query_name]
            if schema_type == "VERTEX" and (
                "v_type" in params or "v_type_set" in params
            ):
                key = "v_type" if "v_type" in params else "v_type_set"
                if isinstance(params[key], str):
                    schema_name = [params[key]]
                elif isinstance(params[key], list):
                    schema_name = params[key]
                else:
                    raise ValueError(
                        "v_type should be either a list or string")
            elif schema_type == "EDGE" and (
                "e_type" in params or "e_type_set" in params
            ):
                key = "e_type" if "e_type" in params else "e_type_set"
                if isinstance(params[key], str):
                    schema_name = [params[key]]
                elif isinstance(params[key], list):
                    schema_name = params[key]
                else:
                    raise ValueError(
                        "e_type should be either a list or string")
        # Find whether global or local changes are needed by checking schema type.
        global_types = []
        local_types = []
        if schema_type == "VERTEX":
            for v_type in schema_name:
                if "IsLocal" in self.conn.getVertexType(v_type, force=True):
                    local_types.append(v_type)
                else:
                    global_types.append(v_type)
        if schema_type == "EDGE":
            for e_type in schema_name:
                if "IsLocal" in self.conn.getEdgeType(e_type, force=True):
                    local_types.append(e_type)
                else:
                    global_types.append(e_type)
        # Change schema
        if global_types:
            _ = add_attribute(
                conn=self.conn,
                schema_type=schema_type,
                attr_type=feat_type,
                attr_name=feat_name,
                schema_name=global_types,
                global_change=True,
            )
        if local_types:
            _ = add_attribute(
                conn=self.conn,
                schema_type=schema_type,
                attr_type=feat_type,
                attr_name=feat_name,
                schema_name=local_types,
                global_change=False,
            )
