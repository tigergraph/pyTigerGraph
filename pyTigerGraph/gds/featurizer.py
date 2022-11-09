"""Featurizer
The Featurizer class provides methods for installing and running Graph Data Science Algorithms onto a TigerGraph server.
"""

from typing import TYPE_CHECKING, Any, List, Tuple

if TYPE_CHECKING:
    from ..pyTigerGraph import TigerGraphConnection

import json
import re
from os.path import join as pjoin

import requests

from .utilities import is_query_installed, random_string


class Featurizer:
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
            major_ver, minor_ver, patch_ver = self._get_db_version()
        else:
            self.algo_ver = algo_version
            major_ver, minor_ver = algo_version.split(".")[:2]
        # Get repo address
        if not repo:
            repo = "https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/{}.{}".format(
                major_ver, minor_ver
            )
        self.repo = repo
        # Get algo dict from manifest
        manifest = pjoin(repo, "manifest.json")
        self.algo_dict = self._get_algo_dict(manifest)
        self.algo_paths = None

        self.params_dict = {}  # input parameter for the desired algorithm to be run
        self.query = None
        self.query_name = None
        self.query_result_type = None

    def _get_db_version(self) -> Tuple[str, str, str]:
        # Get DB version
        self.algo_ver = self.conn.getVer()
        major_ver, minor_ver, patch_ver = self.algo_ver.split(".")
        if int(major_ver) < 3 or (int(major_ver) == 3 and int(minor_ver) <= 7):
            # For DB version <= 3.7, use version 3.7.
            major_ver = "3"
            minor_ver = "7"
        return major_ver, minor_ver, patch_ver

    def _get_algo_dict(self, manifest_file: str) -> dict:
        # Get algo dict from manifest
        if manifest_file.startswith("http"):
            resp = requests.get(manifest_file)
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
                    print("{}{:02}. name: {}".format("  " * depth, algo_num, v))
                    return algo_num
                if isinstance(v, dict):
                    print("{}{}:".format("  " * depth, k))
                    algo_num = print_algos(v, depth + 1, algo_num)
            return algo_num

        if category:
            if category in self.algo_dict.keys():
                print("Available algorithms for {}:".format(category))
                print_algos(self.algo_dict[category], 1)
                print("Call run() with the algorithm name to execute it")
            else:
                print("No available algorithms for category {}".format(category))
        else:
            print("Available algorithms per category:")
            for k in self.algo_dict:
                print("- {}: {} algorithms".format(k, get_num_algos(self.algo_dict[k])))
            print(
                "Call listAlgorithms() with the category name to see the list of algorithms"
            )

    def _install_query_file(
        self,
        query_path: str,
        replace: dict = None,
        force: bool = False,
        global_change: bool = False,
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
        
        Return:
            Name of the installed query
        """
        # Read in the query
        if query_path.startswith("http"):
            resp = requests.get(query_path)
            resp.raise_for_status()
            query = resp.text
        else:
            with open(query_path) as f:
                query = f.read()
        # Get query name from the first line
        firstline = query.split("\n", 1)[0]
        try:
            query_name = re.search(r"QUERY (.+?)\(", firstline).group(1).strip()
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
        if query_name == "tg_fastRP":
            # Drop all jobs on the graph
            self.conn.gsql("USE GRAPH {}\n".format(self.conn.graphname) + "drop job *")
            res = self._add_attribute(
                schema_type="VERTEX",
                attr_type=" LIST<DOUBLE>",
                attr_name="embedding",
                global_change=global_change,
            )
        # TODO: Check if Distributed query is needed.
        query = (
            "USE GRAPH {}\n".format(self.conn.graphname)
            + query
            + "\nInstall Query {}\n".format(query_name)
        )
        print("Installing and optimizing the queries, it might take a minute...")
        resp = self.conn.gsql(query)
        status = resp.splitlines()[-1]
        if "Failed" in status:
            raise ConnectionError(resp)
        print("Queries installed successfully")
        return query_name

    def installAlgorithm(
        self, query_name: str, query_path: str = None, global_change: bool = False
    ) -> str:
        """
        Checks if the query is already installed.
        If the query is not installed, it installs the query and changes the schema if an attribute needs to be added.

        Args:
            query_name (str):
                The name of query to be installed.
            query_path (str):
                If using a custom query, the path to the `.gsql` file that contains the query.
                Note: you must have the `query_name` parameter match the name of the query in the file.
            global_change (bool):
                False by default. Set to true if you want to run `GLOBAL SCHEMA_CHANGE JOB`. For algorithms that are not schema free we need to specify this argument.
                See https://docs.tigergraph.com/gsql-ref/current/ddl-and-loading/modifying-a-graph-schema#_global_vs_local_schema_changes.
        Returns:
            String of query name installed.
        """
        # If a query file is given, install it directly.
        if query_path:
            self.query_name = self._install_query_file(query_path, global_change=global_change)
            return self.query_name
        # Else, install query by name from the repo.
        # If the query paths are not collected yet, do it now.
        if not self.algo_paths:
            self.algo_paths, self.query_result_type = self._get_algo_details(self.algo_dict)
        if query_name not in self.algo_paths:
            raise ValueError("Cannot find {} in the library.".format(query_name))
        for query in self.algo_paths[query_name]:
            _ = self._install_query_file(query, global_change=global_change)
        self.query_name = query_name
        return self.query_name

    def _get_algo_details(self, algo_dict: dict) -> dict:
        def get_details(d: dict, paths: dict, types: dict) -> None:
            if "name" in d.keys():
                if "path" not in d.keys():
                    raise Exception(
                        "Cannot find path for {} in the manifest file".format(d["name"])
                    )
                paths[d["name"]] = [pjoin(self.repo, p) for p in d["path"].split(";")]
                if "value_type" in d.keys():
                    types[d["name"]] = d["value_type"]
                return
            for k in d:
                if isinstance(d[k], dict):
                    get_details(d[k], paths, types)
            return

        algo_paths = {}
        algo_result_types = {}
        get_details(algo_dict, algo_paths, algo_result_types)
        return algo_paths, algo_result_types

    def _add_attribute(
        self,
        schema_type: str,
        attr_type: str,
        attr_name: str = None,
        schema_name: List[str] = None,
        global_change: bool = False,
    ):
        """
        If the current attribute is not already added to the schema, it will create the schema job to do that.
        Check whether to add the attribute to vertex(vertices) or edge(s).

        Args:
            schema_type (str):
                Vertex or edge
            attr_type (str):
                Type of attribute which can be INT, DOUBLE,FLOAT,BOOL, or LIST
            attr_name (str):
                An attribute name that needs to be added to the vertex/edge
            schema_name (List[str]):
                List of Vertices/Edges that need the `attr_name` added to them.
            global_change (bool):
                False by default. Set to true if you want to run `GLOBAL SCHEMA_CHANGE JOB`.
                See https://docs.tigergraph.com/gsql-ref/current/ddl-and-loading/modifying-a-graph-schema#_global_vs_local_schema_changes.
                If the schema change should be global or local.
        """
        # Check whether to add the attribute to vertex(vertices) or edge(s)
        self.result_attr = attr_name
        v_type = False
        if schema_type.upper() == "VERTEX":
            target = self.conn.getVertexTypes(force=True)
            v_type = True
        elif schema_type.upper() == "EDGE":
            target = self.conn.getEdgeTypes(force=True)
        else:
            raise Exception("schema_type has to be VERTEX or EDGE")
        # If attribute should be added to a specific vertex/edge name
        if schema_name != None:
            target.clear()
            target = schema_name
        # For every vertex or edge type
        tasks = []
        for t in target:
            attributes = []
            if v_type:
                meta_data = self.conn.getVertexType(t, force=True)
            else:
                meta_data = self.conn.getEdgeType(t, force=True)
            for i in range(len(meta_data["Attributes"])):
                attributes.append(meta_data["Attributes"][i]["AttributeName"])
            # If attribute is not in list of vertex attributes, do the schema change to add it
            if attr_name != None and attr_name not in attributes:
                tasks.append(
                    "ALTER {} {} ADD ATTRIBUTE ({} {});\n".format(
                        schema_type, t, attr_name, attr_type
                    )
                )
        # If attribute already exists for schema type t, nothing to do
        if not tasks:
            return "Attribute already exists"
        # Drop all jobs on the graph
        # self.conn.gsql("USE GRAPH {}\n".format(self.conn.graphname) + "DROP JOB *")
        # Create schema change job
        job_name = "add_{}_attr_{}".format(schema_type, random_string(6))
        if not (global_change):
            job = (
                "USE GRAPH {}\n".format(self.conn.graphname)
                + "CREATE SCHEMA_CHANGE JOB {} {{\n".format(job_name)
                + "".join(tasks)
                + "}}\nRUN SCHEMA_CHANGE JOB {}".format(job_name)
            )
        else:
            job = (
                "USE GRAPH {}\n".format(self.conn.graphname)
                + "CREATE GLOBAL SCHEMA_CHANGE JOB {} {{\n".format(job_name)
                + "".join(tasks)
                + "}}\nRUN GLOBAL SCHEMA_CHANGE JOB {}".format(job_name)
            )
        # Submit the job
        resp = self.conn.gsql(job)
        status = resp.splitlines()[-1]
        if "Failed" in status:
            raise ConnectionError(resp)
        else:
            print(status)
        return "Schema change succeeded."

    def _get_query(self, query_name: str) -> str:
        if not self.algo_paths:
            self.algo_paths, self.query_result_type = self._get_algo_details(self.algo_dict)
        if query_name not in self.algo_paths:
            raise ValueError("Cannot find {} in the library.".format(query_name))
        query_path = self.algo_paths[query_name][-1]
        if query_path.startswith("http"):
            resp = requests.get(query_path)
            resp.raise_for_status()
            query = resp.text
        else:
            with open(query_path) as f:
                query = f.read()
        return query

    def _get_Params(self, query_name: str):
        """
        Returns default query parameters by parsing the query header.
        Args:
            query_name (str):
                The name of the query to be executed.
        """
        _dict = {}
        query = self._get_query(query_name)
        try:
            input_params = query[query.find("(") + 1 : query.find(")")]
            list_params = input_params.split(",")
            for i in range(len(list_params)):
                if "=" in list_params[i]:
                    params_type = list_params[i].split("=")[0].split()[0]
                    if (
                        params_type.lower() == "float"
                        or params_type.lower() == "double"
                    ):
                        _dict[list_params[i].split("=")[0].split()[1]] = float(
                            list_params[i].split("=")[1]
                        )
                    if params_type.lower() == "bool":
                        _dict[list_params[i].split("=")[0].split()[1]] = bool(
                            list_params[i].split("=")[1]
                        )
                    if params_type.lower() == "int":
                        _dict[list_params[i].split("=")[0].split()[1]] = int(
                            list_params[i].split("=")[1]
                        )
                    if params_type.lower() == "string":
                        _dict[list_params[i].split("=")[0].split()[1]] = (
                            list_params[i].split("=")[1].split()[0][1:-2]
                        )
                else:
                    _dict[list_params[i].split()[1]] = None
        except:
            print("The algorithm does not have any input parameter.")
        self.params_dict[query_name] = _dict
        return _dict

    def runAlgorithm(
        self,
        query_name: str,
        params: dict = None,
        feat_name: str = None,
        feat_type: str = None,
        custom_query: bool = False,
        schema_name: list = None,
        global_schema: bool = False,
        timeout: int = 2147480,
        sizeLimit: int = None,
    ) -> Any:
        """
        Runs an installed query.
        The query must be already created and installed in the graph.
        If the query accepts input parameters and the parameters have not been provided, calling this function runs the query with the default values for the parameters.
        If the there isn't a default value in the query definition and no parameters are provided, the function raises a `ValueError`.

        Args:
            query_name (str):
                The name of the query to be executed.
            params (dict):
                Query parameters. A dictionary.
            feat_name (str):
                An attribute name that needs to be added to the vertex/edge
            feat_type (str):
                Type of attribute that needs to be added to the vertex/edge. Only needed if `custom_query` is set to `True`.
            custom_query (bool):
                If the query is a custom query. Defaults to False.
            schema_name:
                List of Vertices/Edges that the attr_name need to added to them.
            global_schema (bool):
                False by default. Set to true if you want to run `GLOBAL SCHEMA_CHANGE JOB`.
                See https://docs.tigergraph.com/gsql-ref/current/ddl-and-loading/modifying-a-graph-schema#_global_vs_local_schema_changes.
            timeout (int):
                Maximum duration for successful query execution (in milliseconds).
            sizeLimit (int):
                Maximum size of response (in bytes).

        Returns:
            The output of the query, a list of output elements (vertex sets, edge sets, variables,
            accumulators, etc.)
        """
        schema_type = "VERTEX"
        if params == None:
            if not (custom_query):
                params = self._get_Params(query_name)
            print("Default parameters are:", params)
            if params:
                if None in params.values():
                    query_ulr = self.algo_paths[query_name][-1]
                    raise ValueError(
                        "Query parameters which are not initialized by default need to be initialized, visit "
                        + query_ulr
                        + "."
                    )
            else:
                result = self.conn.runInstalledQuery(
                    query_name, timeout=timeout, sizeLimit=sizeLimit, usePost=True
                )
                if result != None:
                    return result
        else:
            if not (custom_query):
                default_params = self._get_Params(query_name)
            else:
                if feat_name == None or feat_type == None:
                    default_params = {}
                else:
                    default_params = {"result_attr": feat_name}
            if feat_name:
                if "result_attr" in default_params.keys():
                    params["result_attr"] = feat_name
                    if query_name != "tg_fastRP":
                        if not (feat_type):
                            feat_type = self.query_result_type[query_name]
                        _ = self._add_attribute(
                            schema_type,
                            feat_type,
                            feat_name,
                            schema_name,
                            global_change=global_schema,
                        )
                else:
                    query_ulr = self.algo_paths[query_name][-1]
                    raise ValueError(
                        "The algorithm does not provide any feature, see the algorithm details:"
                        + query_ulr
                        + "."
                    )
            result = self.conn.runInstalledQuery(
                query_name, params, timeout=timeout, sizeLimit=sizeLimit, usePost=True
            )
            if result != None:
                return result
