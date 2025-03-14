import unittest
import os

from pyTigerGraphUnitTestAsync import make_connection

from pyTigerGraph.common.exception import TigerGraphException


class test_pyTigerGraphGSQLAsync(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.conn = await make_connection()

    async def test_01_gsql(self):
        res = await self.conn.gsql("help")
        self.assertIsInstance(res, str)
        res = res.split("\n")
        self.assertEqual(
            "GSQL Help: Summary of TigerGraph GSQL Shell commands.", res[0])

    async def test_02_gsql(self):
        res = await self.conn.gsql("ls")
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

    async def test_getUDF(self):
        # Don't get anything
        res = await self.conn.getUDF(ExprFunctions=False, ExprUtil=False)
        self.assertEqual(res, "")
        # Get both ExprFunctions and ExprUtil (default)
        udf = await self.conn.getUDF()
        self.assertIn("init_kafka_producer", udf[0])
        self.assertIn("class KafkaProducer", udf[1])
        # Get ExprFunctions only
        udf = await self.conn.getUDF(ExprUtil=False)
        self.assertIn("init_kafka_producer", udf)
        # Get ExprUtil only
        udf = await self.conn.getUDF(ExprFunctions=False)
        self.assertIn("class KafkaProducer", udf)


if __name__ == "__main__":
    # suite = unittest.TestSuite()
    # suite.addTest(test_pyTigerGraphGSQL("test_01_gsql"))
    # suite.addTest(test_pyTigerGraphGSQL("test_02_gsql"))
    # # suite.addTest(test_pyTigerGraphGSQL("test_03_installUDF"))
    # # suite.addTest(test_pyTigerGraphGSQL("test_04_installUDFRemote"))
    # suite.addTest(test_pyTigerGraphGSQL("test_getUDF"))

    # runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    # runner.run(suite)

    unittest.main()
