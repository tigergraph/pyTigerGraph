import unittest

from pyTigerGraphUnitTest import make_connection


class test_pyTigerGraphGSQL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection()

    def test_01_gsql(self):
        res = self.conn.gsql("help")
        self.assertIsInstance(res, str)
        res = res.split("\n")
        self.assertEqual("GSQL Help: Summary of TigerGraph GSQL Shell commands.", res[0])

    def test_02_gsql(self):
        res = self.conn.gsql("ls")
        self.assertIsInstance(res, str)
        res = res.split("\n")[0]
        self.assertIn(res,["---- Global vertices, edges, and all graphs", "---- Graph " + self.conn.graphname])

    # def test_03_installUDF(self):
    #     path = os.path.dirname(os.path.realpath(__file__))
    #     ExprFunctions = os.path.join(path, "fixtures", "ExprFunctions.hpp")
    #     ExprUtil = os.path.join(path, "fixtures", "ExprUtil.hpp")
    #     self.conn.installUDF(ExprFunctions, ExprUtil)
    #     f = Featurizer(self.conn)
    #     self.assertEqual(f.installAlgorithm("tg_fastRP"), "tg_fastRP")

    # def test_04_installUDFRemote(self):
    #     ExprFunctions = "https://tg-mlworkbench.s3.us-west-1.amazonaws.com/udf/1.0/ExprFunctions.hpp"
    #     self.conn.installUDF(ExprFunctions=ExprFunctions)
    #     loader = self.conn.gds.vertexLoader(
    #         attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
    #         batch_size=16,
    #         shuffle=True,
    #         filter_by="train_mask",
    #         loader_id=None,
    #         buffer_size=4,
    #     )
    #     self.assertTrue(is_query_installed(self.conn, loader.query_name))


if __name__ == "__main__":
    unittest.main()
