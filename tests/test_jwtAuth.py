import requests
import json
from requests.auth import HTTPBasicAuth

import unittest
from unittest.mock import patch, Mock, MagicMock

from pyTigerGraph import TigerGraphConnection
from tests.pyTigerGraphUnitTest import make_connection

class TestJWTTokenAuth(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection()
   
    def requestJWTToken(self):
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
    
    def test_jwtauth(self):
        dbversion = self.conn.getVer()
        if "3.9" in dbversion:
            self.test_jwtauth_3_9()
        elif "4.1" in dbversion:
            self.test_jwtauth_4_1_success()
            self.test_jwtauth_4_1_fail()
        else:
            pass

    def test_jwtauth_3_9(self):
        with self.assertRaises(RuntimeError) as context:
            TigerGraphConnection(
                host=self.conn.host,
                jwtToken="fake.JWT.Token"
            )

        # Verify the exception message
        self.assertIn("Please generate new JWT token or switch to API token or username/password.", str(context.exception))

    def test_jwtauth_4_1_success(self):
        jwt_token = self.requestJWTToken()

        conn = TigerGraphConnection(
            host=self.conn.host,
            jwtToken=jwt_token
        )

        # restpp on port 9000
        dbversion = conn.getVer()
        self.assertIn("4.1", dbversion)

        # gsql on port 14240
        res = conn._get(f"http://{self.conn.host}:{self.conn.gsPort}/gsqlserver/gsql/simpleauth", authMode="token", resKey=None)
        self.assertIn("privileges", res)

    def test_jwtauth_4_1_fail(self):
        # jwt_token = self.requestJWTToken()

        with self.assertRaises(RuntimeError) as context:
            conn = TigerGraphConnection(
                host=self.conn.host,
                jwtToken="invalid.JWT.Token"
            )

            # restpp on port 9000
            conn.getVer()

            # gsql on port 14240
            # conn._get(f"http://{self.conn.host}:{self.conn.gsPort}/gsqlserver/gsql/simpleauth", authMode="token", resKey=None)

        # Verify the exception message
        self.assertIn("The JWT token might be invalid or expired or DB version doesn't support JWT token.", str(context.exception))


if __name__ == '__main__':
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(TestJWTTokenAuth("test_jwtauth"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)