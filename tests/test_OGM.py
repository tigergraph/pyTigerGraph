import unittest
from multiprocessing import Pool

from pyTigerGraphUnitTest import make_connection
from pyTigerGraph.schema import Graph, Vertex, Edge
from typing import List, Dict
from datetime import datetime
from dataclasses import dataclass



class TestHomogeneousOGM(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def test_init(self):
        g = Graph(self.conn)
        attrs = g.vertex_types["Paper"].attributes
        self.assertIn("x", attrs.keys())

    def test_type(self):
        g = Graph(self.conn)
        attrs = g.vertex_types["Paper"].attributes
        self.assertEqual(str(attrs["y"]), "<class 'int'>")

    def test_add_vertex_type(self):
        g = Graph(self.conn)
        @dataclass
        class AccountHolder(Vertex):
            name: str
            address: str
            accounts: List[str]
            dob: datetime
            some_map: Dict[str, int]
            some_double: "DOUBLE"

        g.add_vertex_type(AccountHolder)

        g.commit_changes()

        self.assertIn("name", g.vertex_types["AccountHolder"].attributes.keys())

    def test_add_edge_type(self):
        g = Graph(self.conn)

        @dataclass
        class AccountHolder(Vertex):
            name: str
            address: str
            accounts: List[str]
            dob: datetime
            some_map: Dict[str, int]
            some_double: "DOUBLE"

        @dataclass
        class HOLDS_ACCOUNT(Edge):
            opened_on: datetime
            from_vertex: AccountHolder
            to_vertex: g.vertex_types["Account"]
            is_directed: bool = True
            reverse_edge: str = "ACCOUNT_HELD_BY"

        g.add_edge_type(HOLDS_ACCOUNT)

        g.commit_changes()

        self.assertIn("opened_on", g.edge_types["HOLDS_ACCOUNT"].attributes.keys())


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
