"""GDS Utilities
Utilities for the Graph Data Science functions.
"""
import logging
import os
import random
import re
import string
from os.path import join as pjoin
from typing import TYPE_CHECKING, Union
from urllib.parse import urlparse

if TYPE_CHECKING:
    from ..pyTigerGraph import TigerGraphConnection

logger = logging.getLogger(__name__)

'''
import boto3
from google.cloud import storage as gcs


def download_from_gcs(gcs_path: str, local_path: str, silent: bool = False) -> str:
    """Download a file from Google cloud storage to local.
    Args:
        local_path (str): Path to the local folder.
        gcs_path (str): Path to the file on Google cloud storage. Format: gs://bucket/path
    """
    url = urlparse(gcs_path)
    if url.scheme != "gs":
        raise ValueError("Unrecognized GCS url. Expect format: gs://bucket/path")
    if not url.netloc:
        raise ValueError("Cannot find bucket name. Expect format: gs://bucket/path")

    client = gcs.Client()
    bucket = client.bucket(url.netloc)
    blob = bucket.blob(url.path.strip("/"))
    filename = os.path.basename(url.path)
    local_file = pjoin(local_path, filename)
    blob.download_to_filename(local_file)
    if not silent:
        print("Downloaded file {} to {}".format(gcs_path, local_file))

    return local_file


def download_from_s3(
    s3_path: str,
    local_path: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    silent: bool = True,
) -> str:
    """Download a file from S3 to local.

    Args:
        local_path (str): Path to the local folder.
        s3_path (str): Path to the file on S3. Format: s3://bucket/path
    """
    url = urlparse(s3_path)
    if url.scheme != "s3":
        raise ValueError("Unrecognized S3 url. Expect format: s3://bucket/path")
    if not url.netloc:
        raise ValueError("Cannot find bucket name. Expect format: s3://bucket/path")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    filename = os.path.basename(url.path)
    local_file = pjoin(local_path, filename)
    s3.download_file(url.netloc, url.path.strip("/"), local_file)
    if not silent:
        print("Downloaded file {} to {}".format(s3_path, local_file))

    return local_file
'''


def random_string(length: int = 1, chars: str = string.ascii_letters) -> str:
    return "".join(random.choice(chars) for _ in range(length))


def validate_attributes_input(attributes: str) -> str:
    regex = re.compile(r"^((\w+(:\w+)?)(,\w+(:\w+)?)*)?$")
    if regex.match(attributes) is None:
        raise ValueError(
            "Illegal characters in {}. Required format: 'attr1:type1,attr2:type2,...' where type can be omitted with colon.".format(
                attributes
            )
        )
    return attributes


def is_query_installed(
    conn: "TigerGraphConnection", query_name: str, return_status: bool = False
) -> bool:
    # If the query already installed return true
    target = "GET /query/{}/{}".format(conn.graphname, query_name)
    queries = conn.getInstalledQueries()
    is_installed = target in queries
    if return_status:
        if is_installed:
            is_enabled = queries[target]["enabled"]
        else:
            is_enabled = None
        return is_installed, is_enabled
    else:
        return is_installed


def install_query_file(
    conn: "TigerGraphConnection",
    file_path: str,
    replace: dict = None,
    distributed: bool = False,
    force: bool = False,
) -> str:
    # Read the first line of the file to get query name. The first line should be
    # something like CREATE QUERY query_name (...
    with open(file_path) as infile:
        firstline = infile.readline()
    try:
        query_name = re.search(r"QUERY (.+?)\(", firstline).group(1).strip()
    except:
        raise ValueError(
            "Cannot parse the query file. It should start with CREATE QUERY ... "
        )
    # If a suffix is to be added to query name
    if replace and ("{QUERYSUFFIX}" in replace):
        query_name = query_name.replace(
            "{QUERYSUFFIX}", replace["{QUERYSUFFIX}"])
    # If query is already installed, skip unless force install.
    is_installed, is_enabled = is_query_installed(
        conn, query_name, return_status=True)
    if is_installed:
        if force or (not is_enabled):
            query = "USE GRAPH {}\nDROP QUERY {}\n".format(
                conn.graphname, query_name)
            resp = conn.gsql(query)
            if "Successfully dropped queries" not in resp:
                raise ConnectionError(resp)
        else:
            return query_name
    # Otherwise, install the query from file
    with open(file_path) as infile:
        query = infile.read()
    # Replace placeholders with actual content if given
    if replace:
        for placeholder in replace:
            query = query.replace(placeholder, replace[placeholder])
    if distributed:
        query = query.replace("CREATE QUERY", "CREATE DISTRIBUTED QUERY")
    logger.debug(query)
    query = (
        "USE GRAPH {}\n".format(conn.graphname)
        + query
        + "\nInstall Query {}\n".format(query_name)
    )
    print(
        "Installing and optimizing queries. It might take a minute or two."
    )
    resp = conn.gsql(query)
    if "Query installation finished" not in resp:
        raise ConnectionError(resp)
    else:
        print("Query installation finished.")
    return query_name


def add_attribute(conn: "TigerGraphConnection", schema_type: str, attr_type: str = None, attr_name: Union[str, dict] = None, schema_name: list = None, global_change: bool = False):
    '''
    If the current attribute is not already added to the schema, it will create the schema job to do that.
    Check whether to add the attribute to vertex(vertices) or edge(s).

    Args:
        schema_type (str): 
            Vertex or edge
        attr_type (str): 
            Type of attribute which can be INT, DOUBLE, FLOAT, BOOL, or LIST. Defaults to None. Required if attr_name is of type string.
        attr_name (str, dict): 
            An attribute name that needs to be added to the vertex/edge if string. If dict, must be of format {"attr_name": "attr_type"}.
        schema_name (List[str]):
            List of Vertices/Edges that need the `attr_name` added to them.
        global_change (bool):
            False by default. Set to true if you want to run `GLOBAL SCHEMA_CHANGE JOB`.
            See https://docs.tigergraph.com/gsql-ref/current/ddl-and-loading/modifying-a-graph-schema#_global_vs_local_schema_changes.
            If the schema change should be global or local.
    '''
    # Check whether to add the attribute to vertex(vertices) or edge(s)
    v_type = False
    if schema_type.upper() == "VERTEX":
        target = conn.getVertexTypes(force=True)
        v_type = True
    elif schema_type.upper() == "EDGE":
        target = conn.getEdgeTypes(force=True)
    else:
        raise Exception('schema_type has to be VERTEX or EDGE')
    # If attribute should be added to a specific vertex/edge name
    if schema_name != None:
        target.clear()
        target = schema_name
    # For every vertex or edge type
    tasks = []
    for t in target:
        attributes = []
        if v_type:
            meta_data = conn.getVertexType(t, force=True)
        else:
            meta_data = conn.getEdgeType(t, force=True)
        for i in range(len(meta_data['Attributes'])):
            attributes.append(meta_data['Attributes'][i]['AttributeName'])
        # If attribute is not in list of vertex attributes, do the schema change to add it
        if isinstance(attr_name, str):
            if not attr_type:
                raise Exception(
                    "attr_type must be defined if attr_name is of type string")
            if attr_name != None and attr_name not in attributes:
                tasks.append("ALTER {} {} ADD ATTRIBUTE ({} {});\n".format(
                    schema_type, t, attr_name, attr_type))
        elif isinstance(attr_name, dict):
            for aname in attr_name:
                if aname != None and aname not in attributes:
                    tasks.append("ALTER {} {} ADD ATTRIBUTE ({} {});\n".format(
                        schema_type, t, aname, attr_name[aname]
                    ))
    # If attribute already exists for schema type t, nothing to do
    if not tasks:
        return "Attribute already exists"
    # Drop all jobs on the graph
    # self.conn.gsql("USE GRAPH {}\n".format(self.conn.graphname) + "DROP JOB *")
    # Create schema change job
    job_name = "add_{}_attr_{}".format(schema_type, random_string(6))
    if not (global_change):
        job = "USE GRAPH {}\n".format(conn.graphname) + "CREATE SCHEMA_CHANGE JOB {} {{\n".format(
            job_name) + ''.join(tasks) + "}}\nRUN SCHEMA_CHANGE JOB {}".format(job_name)
    else:
        job = "USE GRAPH {}\n".format(conn.graphname) + "CREATE GLOBAL SCHEMA_CHANGE JOB {} {{\n".format(
            job_name) + ''.join(tasks) + "}}\nRUN GLOBAL SCHEMA_CHANGE JOB {}".format(job_name)
    # Submit the job
    print("Changing schema to save results...", flush=True)
    resp = conn.gsql(job)
    status = resp.splitlines()[-1]
    if "Failed" in status:
        raise ConnectionError(resp)
    else:
        print(status, flush=True)
    return 'Schema change succeeded.'
