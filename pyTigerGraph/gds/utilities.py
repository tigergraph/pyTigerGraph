"""GDS Utilities
Utilities for the Graph Data Science functions.
"""
import os
import random
import re
import string
from os.path import join as pjoin
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from ..pyTigerGraph import TigerGraphConnection

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


def is_query_installed(conn: "TigerGraphConnection", query_name: str) -> bool:
    #If the query already installed return true
    target = "GET /query/{}/{}".format(conn.graphname, query_name)
    queries = conn.getInstalledQueries()
    return target in queries


def install_query_file(
    conn: "TigerGraphConnection",
    file_path: str, 
    replace: dict = None, 
    distributed: bool = False, 
    force: bool = False) -> str:
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
        query_name = query_name.replace("{QUERYSUFFIX}", replace["{QUERYSUFFIX}"])
    # If query is already installed, skip unless force install.
    if is_query_installed(conn, query_name):
        if force:
            #TODO: Drop query.
            raise NotImplementedError
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
        #TODO: Add Distributed keyword.
        raise NotImplementedError
    query = (
        "USE GRAPH {}\n".format(conn.graphname)
        + query
        + "\nInstall Query {}\n".format(query_name)
    )
    print(
        "Installing and optimizing queries. It might take a minute if this is the first time you use this loader."
    )
    resp = conn.gsql(query)
    status = resp.splitlines()[-1]
    if "Failed" in status:
        raise ConnectionError(status)
    else:
        print(status)
    return query_name
