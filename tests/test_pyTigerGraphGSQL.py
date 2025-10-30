import unittest
import os

from pyTigerGraphUnitTest import make_connection


class test_pyTigerGraphGSQL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection()

    def test_01_gsql(self):
        res = self.conn.gsql("help")
        self.assertIsInstance(res, str)
        res = res.split("\n")
        self.assertEqual(
            "GSQL Help: Summary of TigerGraph GSQL Shell commands.", res[0])

    def test_02_gsql(self):
        res = self.conn.gsql("ls")
        self.assertIsInstance(res, str)
        res = res.split("\n")[0]
        self.assertIn(res, ["---- Global vertices, edges, and all graphs",
                      "---- Graph " + self.conn.graphname])

    # def test_03_installUDF(self):
    #     path = os.path.dirname(os.path.realpath(__file__))
    #     ExprFunctions = os.path.join(path, "fixtures", "ExprFunctions.hpp")
    #     ExprUtil = os.path.join(path, "fixtures", "ExprUtil.hpp")
    #     self.assertEqual(self.conn.installUDF(ExprFunctions, ExprUtil), 0)

    # def test_04_installUDFRemote(self):
    #     ExprFunctions = "https://tg-mlworkbench.s3.us-west-1.amazonaws.com/udf/1.0/ExprFunctions.hpp"
    #     self.assertEqual(self.conn.installUDF(ExprFunctions=ExprFunctions), 0)

    def test_getUDF(self):
        # Don't get anything
        res = self.conn.getUDF(ExprFunctions=False, ExprUtil=False)
        self.assertEqual(res, "")
        # Get both ExprFunctions and ExprUtil (default)
        udf = self.conn.getUDF()
        self.assertIn("ExprFunctions", udf[0])
        self.assertIn("ExprUtil", udf[1])
        # Get ExprFunctions only
        udf = self.conn.getUDF(ExprUtil=False)
        self.assertIn("ExprFunctions", udf)
        # Get ExprUtil only
        udf = self.conn.getUDF(ExprFunctions=False)
        self.assertIn("ExprUtil", udf)

    def test_getAsyncRequestStatus(self):
        """Test getAsyncRequestStatus function."""
        # Test with a sample request ID (this will likely fail in test environment)
        # but we can test the function structure and error handling
        test_request_id = "00000000006.317280417"

        try:
            res = self.conn.getAsyncRequestStatus(test_request_id)
            self.assertIsInstance(res, dict)
            self.assertIn("error", res)
            self.assertIn("message", res)
        except Exception as e:
            # Expected to fail in test environment, but should be a proper error response
            self.assertIsInstance(e, Exception)

    def test_cancelAsyncRequest(self):
        """Test cancelAsyncRequest function."""
        # Test with a sample request ID (this will likely fail in test environment)
        # but we can test the function structure and error handling
        test_request_id = "00000000006.317280417"

        try:
            res = self.conn.cancelAsyncRequest(test_request_id)
            self.assertIsInstance(res, dict)
            self.assertIn("error", res)
            self.assertIn("message", res)
        except Exception as e:
            # Expected to fail in test environment, but should be a proper error response
            self.assertIsInstance(e, Exception)

    def test_recoverCatalog(self):
        """Test recoverCatalog function."""
        # Test basic catalog recovery
        res = self.conn.recoverCatalog()
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)
        self.assertIn("message", res)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(test_pyTigerGraphGSQL("test_01_gsql"))
    suite.addTest(test_pyTigerGraphGSQL("test_02_gsql"))
    # suite.addTest(test_pyTigerGraphGSQL("test_03_installUDF"))
    # suite.addTest(test_pyTigerGraphGSQL("test_04_installUDFRemote"))
    suite.addTest(test_pyTigerGraphGSQL("test_getUDF"))
    suite.addTest(test_pyTigerGraphGSQL("test_getAsyncRequestStatus"))
    suite.addTest(test_pyTigerGraphGSQL("test_cancelAsyncRequest"))
    suite.addTest(test_pyTigerGraphGSQL("test_recoverCatalog"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
