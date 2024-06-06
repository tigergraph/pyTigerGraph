import requests
from requests.auth import HTTPBasicAuth

import unittest
from unittest.mock import patch, Mock, MagicMock

from pyTigerGraph import TigerGraphConnection
from tests.pyTigerGraphUnitTest import make_connection

class TestJWTTokenAuth(unittest.TestCase):
    # @classmethod
    # def setUpClass(cls):
    #     cls.conn = make_connection()

    @classmethod
    def setUpClass(cls):
        cls.conn = MagicMock()
        cls.conn.host = "http://localhost"
        cls.conn.gsPort = 14240
        cls.conn.username = "tigergraph"
        cls.conn.password = "tigergraph"

    def test_jwtauth(self):
        # version = self.conn.getVer()

        # if "4.1" in version: 
        #     self.test_jwtauth_4_1()
        # elif "3.9" in version:
        #     self.test_jwtauth_3_9()
        # else:
        #     pass ## todo: don't have a good way to test on 3.10.0, since there is no endpoint to request and configure jwt token 
        with patch.object(self.conn, 'getVer', return_value="4.1"):
            self.test_jwtauth_4_1()

        with patch.object(self.conn, 'getVer', return_value="3.9"):
            self.test_jwtauth_3_9()

    def requestJWTToken(self):
        # Define the URL
        url = f"{self.conn.host}:{self.conn.gsPort}gsqlserver/requestjwttoken"

        # Define the data payload
        payload = {"lifetime": "1000000000"}

        # Make the POST request with basic authentication
        response = requests.post(url, json=payload, auth=HTTPBasicAuth(self.conn.username, self.conn.password))
        return response.json()["jwt"]

    @patch('pyTigerGraph.TigerGraphConnection.getVer', return_value="3.9")
    def test_jwtauth_3_9(self, mock_getVer):
        with self.assertRaises(RuntimeError) as context:
            TigerGraphConnection(
                host=self.conn.host,
                jwtToken="fake.JWT.Token"
            )

        self.assertIn("The DB version using doesn't support JWT token for RestPP.", str(context.exception))

    @patch('pyTigerGraph.TigerGraphConnection.getVer', return_value="4.1")
    @patch('requests.post')
    @patch('pyTigerGraph.TigerGraphConnection._get')
    def test_jwtauth_4_1(self, mock_get, mock_post, mock_getVer):

        # Mock the response for requestJWTToken
        jwt_resposne = {"jwt": "valid.JWT.Token"}
        # mock_post.return_value = Mock(status_code=200, json=lambda: {"jwt": "valid.JWT.Token"})

        # Mock the response for _get (RestPP endpoint)
        mock_get.return_value = {"privileges": "some_privileges"} 

        # Test JWT token on 4.1
        # jwt_token = self.requestJWTToken()
        jwt_token = "valid.JWT.Token"

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

if __name__ == '__main__':
    unittest.main()