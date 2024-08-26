import unittest
from multiprocessing import Pool

from .pyTigerGraphUnitTestAsync import make_connection
from pyTigerGraph.pytgasync.schema import AsyncGraph, Vertex, Edge
from typing import List, Dict, Union
from datetime import datetime
from dataclasses import dataclass


class TestHomogeneousOGM(unittest.IsolatedAsyncioTestCase):
    @classmethod
    async def asyncSetUp(self):
        self.conn = await make_connection(graphname="Cora")

    async def test_init(self):
        g = await AsyncGraph.create(self.conn)
        attrs = g.vertex_types["Paper"].attributes
        self.assertIn("x", attrs.keys())

    async def test_type(self):
        g = await AsyncGraph.create(self.conn)
        attrs = g.vertex_types["Paper"].attributes
        self.assertEqual(str(attrs["y"]), "<class 'int'>")

    async def test_add_vertex_type(self):
        g = await AsyncGraph.create(self.conn)

        @dataclass
        class AccountHolder(Vertex):
            name: str
            address: str
            accounts: List[str]
            dob: datetime
            some_map: Dict[str, int]
            some_double: "DOUBLE"
            primary_id: str = "name"
            primary_id_as_attribute: bool = True

        g.add_vertex_type(AccountHolder)

        await g.commit_changes()

        self.assertIn(
            "name", g.vertex_types["AccountHolder"].attributes.keys())

    async def test_add_edge_type(self):
        g = await AsyncGraph.create(self.conn)

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
            to_vertex: g.vertex_types["Paper"]
            is_directed: bool = True
            reverse_edge: str = "ACCOUNT_HELD_BY"

        g.add_edge_type(HOLDS_ACCOUNT)

        await g.commit_changes()

        self.assertIn(
            "opened_on", g.edge_types["HOLDS_ACCOUNT"].attributes.keys())

    async def test_add_multi_target_edge_type(self):
        g = await AsyncGraph.create(self.conn)

        @dataclass
        class AccountHolder(Vertex):
            name: str
            address: str
            accounts: List[str]
            dob: datetime
            some_map: Dict[str, int]
            some_double: "DOUBLE"

        @dataclass
        class SOME_EDGE_NAME(Edge):
            some_attr: str
            from_vertex: Union[AccountHolder, g.vertex_types["Paper"]]
            to_vertex: g.vertex_types["Paper"]
            is_directed: bool = True
            reverse_edge: bool = True

        g.add_edge_type(SOME_EDGE_NAME)

        await g.commit_changes()

        self.assertIn(
            "some_attr", g.edge_types["SOME_EDGE_NAME"].attributes.keys())

    async def test_drop_edge_type(self):
        g = await AsyncGraph.create(self.conn)

        g.remove_edge_type(g.edge_types["HOLDS_ACCOUNT"])

        await g.commit_changes()

        self.assertNotIn("HOLDS_ACOUNT", g.edge_types)

    async def test_drop_multi_target_edge_type(self):
        g = await AsyncGraph.create(self.conn)

        g.remove_edge_type(g.edge_types["SOME_EDGE_NAME"])

        await g.commit_changes()

        self.assertNotIn("SOME_EDGE_NAME", g.edge_types)

    async def test_drop_vertex_type(self):
        g = await AsyncGraph.create(self.conn)

        g.remove_vertex_type(g.vertex_types["AccountHolder"])

        await g.commit_changes()

        self.assertNotIn("AccountHolder", g.vertex_types)

    async def test_add_vertex_attribute_default_value(self):
        g = await AsyncGraph.create(self.conn)

        g.vertex_types["Paper"].add_attribute(
            "ThisIsATest", str, "test_default")

        await g.commit_changes()

        self.assertIn("ThisIsATest", g.vertex_types["Paper"].attributes.keys())
        sample = await self.conn.getVertices("Paper", limit=1)
        sample = sample[0]["attributes"]["ThisIsATest"]

        self.assertEqual("'test_default'", sample)

    async def test_drop_vertex_attribute(self):
        g = await AsyncGraph.create(self.conn)

        g.vertex_types["Paper"].remove_attribute("ThisIsATest")

        await g.commit_changes()

        self.assertNotIn(
            "ThisIsATest", g.vertex_types["Paper"].attributes.keys())


class TestHeterogeneousOGM(unittest.IsolatedAsyncioTestCase):
    @classmethod
    async def asyncSetUp(self):
        self.conn = await make_connection(graphname="hetero")

    async def test_init(self):
        g = await AsyncGraph.create(self.conn)
        self.assertEqual(len(g.vertex_types.keys()), 3)

    async def test_type(self):
        g = await AsyncGraph.create(self.conn)
        attrs = g.vertex_types["v0"].attributes
        self.assertEqual(str(attrs["train_mask"]), "<class 'bool'>")

    async def test_outgoing_edge_types(self):
        g = await AsyncGraph.create(self.conn)
        outgoing_edge_types = g.vertex_types["v0"].outgoing_edge_types
        self.assertEqual(outgoing_edge_types, {"v0v0": g.edge_types["v0v0"]})

    async def test_incoming_edge_types(self):
        g = await AsyncGraph.create(self.conn)
        incoming_edge_types = g.vertex_types["v0"].incoming_edge_types
        self.assertEqual(incoming_edge_types, {"v0v0": g.edge_types["v0v0"],
                                               "v2v0": g.edge_types["v2v0"]})


class TestCreateGraph(unittest.IsolatedAsyncioTestCase):
    @classmethod
    async def asyncSetUp(self):
        self.conn = await make_connection()

    async def test_create(self):
        self.conn.graphname = "Cora2"
        g = await AsyncGraph.create()

        @dataclass
        class Paper(Vertex):
            id: int
            y: int
            x: List[int]
            primary_id: str = "id"
            primary_id_as_attribute: bool = True

        @dataclass
        class CITES(Edge):
            from_vertex: Paper
            to_vertex: Paper
            is_directed: bool = True
            reverse_edge: str = "R_CITES"

        g.add_vertex_type(Paper)
        g.add_edge_type(CITES)

        await g.commit_changes(self.conn)

        self.assertIn("id", g.vertex_types["Paper"].attributes.keys())

        await self.conn.gsql("DROP GRAPH Cora2")


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestHeterogeneousOGM("test_init"))
    suite.addTest(TestHomogeneousOGM("test_type"))
    suite.addTest(TestHomogeneousOGM("test_add_vertex_type"))
    suite.addTest(TestHomogeneousOGM("test_add_edge_type"))
    suite.addTest(TestHomogeneousOGM("test_drop_edge_type"))
    suite.addTest(TestHomogeneousOGM("test_drop_vertex_type"))
    # suite.addTest(TestHomogeneousOGM("test_add_vertex_attribute_default_value"))
    # suite.addTest(TestHomogeneousOGM("test_drop_vertex_attribute"))
    suite.addTest(TestHeterogeneousOGM("test_init"))
    suite.addTest(TestHeterogeneousOGM("test_type"))
    suite.addTest(TestHeterogeneousOGM("test_outgoing_edge_types"))
    suite.addTest(TestHeterogeneousOGM("test_incoming_edge_types"))
    suite.addTest(TestCreateGraph("test_create"))
    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
