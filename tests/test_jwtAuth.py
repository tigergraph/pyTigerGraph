import unittest
import requests
import json

from pyTigerGraphUnitTest import make_connection
from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.common.exception import TigerGraphException


class TestJWTTokenAuth(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="tests")

    def test_jwtauth(self):
        dbversion = self.conn.getVer()

        if "3.9" in str(dbversion):
            self._test_jwtauth_3_9()
        elif "4.1" in str(dbversion):
            self._test_jwtauth_4_1_success()
            self._test_jwtauth_4_1_fail()
        else:
            pass

    def _requestJWTToken(self):
        # in >=4.1 API all tokens are JWT tokens
        if self.conn._version_greater_than_4_0():
            return self.conn.getToken(self.conn.createSecret())[0]

        # Define the URL
        url = f"{self.conn.host}:{self.conn.gsPort}/gsqlserver/requestjwttoken"
        # Define the data payload
        payload = json.dumps({"lifetime": "1000000000"})
        # Define the headers for the request
        headers = {
            'Content-Type': 'application/json'
        }
        # Make the POST request with basic authentication
        response = requests.post(url, data=payload, headers=headers, auth=(
            self.conn.username, self.conn.password))
        return response.json()['token']

    def _test_jwtauth_3_9(self):
        with self.assertRaises(TigerGraphException) as context:
            TigerGraphConnection(
                host=self.conn.host,
                jwtToken="fake.JWT.Token"
            )

        # Verify the exception message
        self.assertIn("switch to API token or username/password.",
                      str(context.exception))

    def _test_jwtauth_4_1_success(self):
        jwt_token = self._requestJWTToken()

        newconn = TigerGraphConnection(
            host=self.conn.host,
            jwtToken=jwt_token
        )

        authheader = newconn.authHeader
        print(f"authheader from new conn: {authheader}")

        # restpp on port 9000
        dbversion = newconn.getVer()
        print(f"dbversion from new conn: {dbversion}")
        self.assertIn("4.1", str(dbversion))

        # gsql on port 14240
        if self.conn._version_greater_than_4_0():
            res = newconn._get(
                f"{newconn.gsUrl}/gsql/v1/auth/simple", authMode="token", resKey=None)
            res = res['results']
        else:
            res = newconn._get(
                f"{self.conn.host}:{self.conn.gsPort}/gsqlserver/gsql/simpleauth", authMode="token", resKey=None)
        self.assertIn("privileges", res)

    def _test_jwtauth_4_1_fail(self):
        with self.assertRaises(TigerGraphException) as context:
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
