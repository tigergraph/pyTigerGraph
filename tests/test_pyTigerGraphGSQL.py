import os
import unittest

from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.gds.featurizer import Featurizer
from pyTigerGraph.gds.utilities import is_query_installed

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphGSQL(pyTigerGraphUnitTest):
    # conn = None

    def test_01_gsql(self):
        res = self.conn.gsql("help")
        self.assertIsInstance(res, str)
        res = res.split("\n")
        self.assertEqual("GSQL Help: Summary of TigerGraph GSQL Shell commands.", res[0])

    def test_02_gsql(self):
        res = self.conn.gsql("ls")
        self.assertIsInstance(res, str)
        res = res.split("\n")
        self.assertEqual("---- Graph " + self.conn.graphname, res[0])

    def test_01_installUDF(self):
        conn = TigerGraphConnection(
            host="http://localhost",
            username="tigergraph",
            password="tigergraph",
            graphname="Cora")

        path = os.path.dirname(os.path.realpath(__file__))
        ExprFunctions = os.path.join(path, "fixtures", "ExprFunctions.hpp")
        ExprUtil = os.path.join(path, "fixtures", "ExprUtil.hpp")
        conn.installUDF(ExprFunctions, ExprUtil)
        f = Featurizer(conn)
        self.assertEqual(f.installAlgorithm("tg_fastRP"), "tg_fastRP")

    def test_04_installUDFRemote(self):
        conn = TigerGraphConnection(
            host="http://localhost",
            username="tigergraph",
            password="tigergraph",
            graphname="Cora"
        )

        ExprFunctions = "https://tg-mlworkbench.s3.us-west-1.amazonaws.com/udf/1.0/ExprFunctions.hpp"
        conn.installUDF(ExprFunctions=ExprFunctions)
        loader = conn.gds.vertexLoader(
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(is_query_installed(conn, loader.query_name))


if __name__ == "__main__":
    unittest.main()
