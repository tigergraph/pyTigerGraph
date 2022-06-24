import unittest

from pyTigerGraph import TigerGraphConnection
from pyTigerGraphUnitTest import pyTigerGraphUnitTest
from pyTigerGraph.gds import featurizer
from pyTigerGraph.gds.featurizer import Featurizer 
import os

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
        conn = TigerGraphConnection(host="http://localhost", 
                                username="tigergraph", 
                                password="tigergraph", 
                                graphname="Cora")
    
        path = os.path.dirname(os.path.realpath(__file__))
        ExprFunctions = os.path.join(path,"ExprFunctions.hpp")
        ExprUtil = os.path.join(path,"ExprUtil.hpp")
        conn.installUDF(ExprFunctions,ExprUtil)
        f = Featurizer(conn)
        self.assertEqual(f.installAlgorithm("tg_fastRP"),"tg_fastRP")



if __name__ == '__main__':
    unittest.main()