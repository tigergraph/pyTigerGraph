import unittest
from multiprocessing import Pool

from pyTigerGraphUnitTest import make_connection
from pyTigerGraph.schema import Graph, Vertex, Edge



class TestHomogeneousOGM(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def test_init(self):
        g = Graph(self.conn)
        attrs = g.vertex_types["Paper"].attributes
        self.assertIn(attrs.keys(), "x")

    def test_type(self):
        g = Graph(self.conn)
        attrs = g.vertex_types["Paper"].attributes
        self.assertEqual(str(attrs["y"]), "<class 'int'>")


class TestHeterogeneousOGM(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="hetero")

    def test_init(self):
        g = Graph(self.conn)
        self.assertEqual(len(g.vertex_types.keys()), 3)
    
    def test_type(self):
        g = Graph(self.conn)
        attrs = g.vertex_types["v0"].attributes
        self.assertEqual(str(attrs["train_mask"]), "<class 'bool'>")

if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestHeterogeneousOGM("test_init"))
    suite.addTest(TestHomogeneousOGM("test_type"))
    suite.addTest(TestHeterogeneousOGM("test_init"))
    suite.addTest(TestHeterogeneousOGM("test_type"))
    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
