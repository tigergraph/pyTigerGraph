import json
import os
from os.path import exists

from pyTigerGraph import AsyncTigerGraphConnection


async def make_connection(graphname: str = None):
    server_config = {
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
        "jwtToken": ""
    }

    path = os.path.dirname(os.path.realpath(__file__))
    fname = os.path.join(path, "testserver.json")
    if exists(fname):
        with open(fname, "r") as config_file:
            config = json.load(config_file)
        server_config.update(config)

    conn = AsyncTigerGraphConnection(
        host=server_config["host"],
        graphname=graphname if graphname else server_config["graphname"],
        username=server_config["username"],
        password=server_config["password"],
        tgCloud=server_config["tgCloud"],
        restppPort=server_config["restppPort"],
        gsPort=server_config["gsPort"],
        gsqlVersion=server_config["gsqlVersion"],
        useCert=server_config["userCert"],
        certPath=server_config["certPath"],
        sslPort=server_config["sslPort"],
        gcp=server_config["gcp"],
        jwtToken=server_config["jwtToken"]
    )
    if server_config.get("getToken", False):
        await conn.getToken(await conn.createSecret())

    return conn
