import requests
import json
import os
from os.path import exists
from requests.auth import HTTPBasicAuth

import unittest
from unittest.mock import patch, Mock, MagicMock

from pyTigerGraph import TigerGraphConnection
from tests.pyTigerGraphUnitTest import make_connection

class TestJWTTokenAuth(unittest.TestCase):
    # @classmethod
    # def setUpClass(cls):
    #     server_config = {
    #         "host": "http://127.0.0.1",
    #         "graphname": "jwttoken",
    #         "username": "tigergraph",
    #         "password": "tigergraph",
    #         "gsqlSecret": "",
    #         "restppPort": "9000",
    #         "gsPort": "14240",
    #         "gsqlVersion": "",
    #         "userCert": None,
    #         "certPath": None,
    #         "sslPort": "443",
    #         "tgCloud": False,
    #         "gcp": False,
    #         "jwtToken": ""
    #     }

    #     path = os.path.dirname(os.path.realpath(__file__))
    #     fname = os.path.join(path, "testserver.json")

    #     if exists(fname):
    #         with open(fname, "r") as config_file:
    #             config = json.load(config_file)
    #         server_config.update(config)

    #     cls.conn = TigerGraphConnection(
    #         host=server_config["host"],
    #         graphname=server_config["graphname"],
    #         username=server_config["username"],
    #         password=server_config["password"],
    #         tgCloud=server_config["tgCloud"],
    #         restppPort=server_config["restppPort"],
    #         gsPort=server_config["gsPort"],
    #         gsqlVersion=server_config["gsqlVersion"],
    #         useCert=server_config["userCert"],
    #         certPath=server_config["certPath"],
    #         sslPort=server_config["sslPort"],
    #         gcp=server_config["gcp"],
    #         jwtToken=server_config["jwtToken"]
    #     )
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")


    def test_jwtauth(self):
        # self.conn.gsql("DROP GRAPH jwttoken")
        # self.conn.gsql("CREATE GRAPH tests()")
        # print (self.conn.graphname)
        # self.conn.gsql("USE GRAPH mygraph")
        token = self.conn.getToken(self.conn.createSecret())

        # print (token)

        authheader = {'Authorization': "Bearer " + token[0]}
        self.conn.authHeader = authheader
        print (f"authheader from init conn: {self.conn.authHeader}")
        dbversion = self.conn.getVer()
        print (f"dbversion from init conn: {dbversion}")

        if "3.9" in str(dbversion):
            self._test_jwtauth_3_9()
        elif "4.1" in str(dbversion):
            self._test_jwtauth_4_1_success()
            self._test_jwtauth_4_1_fail()
        else:
            pass
        

    def _requestJWTToken(self):
        # Define the URL
        url = f"{self.conn.host}:{self.conn.gsPort}/gsqlserver/requestjwttoken"
        # Define the data payload
        payload = json.dumps({"lifetime": "1000000000"})
        # Define the headers for the request
        headers = {
            'Content-Type': 'application/json'
        }
        # Make the POST request with basic authentication
        response = requests.post(url, data=payload, headers=headers, auth=(self.conn.username, self.conn.password))
        return response.json()['token']
    

    def _test_jwtauth_3_9(self):
        with self.assertRaises(RuntimeError) as context:
            TigerGraphConnection(
                host=self.conn.host,
                jwtToken="fake.JWT.Token"
            )

        # Verify the exception message
        self.assertIn("switch to API token or username/password.", str(context.exception))


    def _test_jwtauth_4_1_success(self):
        jwt_token = self._requestJWTToken()

        newconn = TigerGraphConnection(
            host=self.conn.host,
            jwtToken=jwt_token
        )

        authheader = newconn.authHeader
        print (f"authheader from new conn: {authheader}")

        # restpp on port 9000
        dbversion = newconn.getVer()
        print (f"dbversion from new conn: {dbversion}")
        self.assertIn("4.1", str(dbversion))

        # gsql on port 14240
        res = newconn._get(f"{self.conn.host}:{self.conn.gsPort}/gsqlserver/gsql/simpleauth", authMode="token", resKey=None)
        self.assertIn("privileges", res)


    def _test_jwtauth_4_1_fail(self):
        with self.assertRaises(RuntimeError) as context:
            TigerGraphConnection(
                host=self.conn.host,
                jwtToken="invalid.JWT.Token"
            )

        # Verify the exception message
        self.assertIn("Please generate new JWT token", str(context.exception))


if __name__ == '__main__':
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(TestJWTTokenAuth("test_jwtauth"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True) 
    runner.run(suite)