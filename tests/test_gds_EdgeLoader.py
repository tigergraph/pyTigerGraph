import unittest

from pandas import DataFrame
from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.gds.utilities import is_query_installed
from pyTigerGraph.gds.dataloaders import EdgeLoader


class TestGDSEdgeLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=False,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
            kafka_address="34.82.171.137:9092",
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 11)

    def test_iterate(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
            kafka_address="34.82.171.137:9092",
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            num_batches += 1
        self.assertEqual(num_batches, 11)

    def test_whole_edgelist(self):
        loader = EdgeLoader(
            graph=self.conn,
            num_batches=1,
            shuffle=True,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
            kafka_address="34.82.171.137:9092",
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)

    def test_iterate_attr(self):
        loader = EdgeLoader(
            graph=self.conn,
            attributes=["time","is_train"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
            kafka_address="34.82.171.137:9092",
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("time", data)
            self.assertIn("is_train", data)
            num_batches += 1
        self.assertEqual(num_batches, 11)

    # TODO: test filter_by


class TestGDSEdgeLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=False,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 11)

    def test_iterate(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            num_batches += 1
        self.assertEqual(num_batches, 11)

    def test_whole_edgelist(self):
        loader = EdgeLoader(
            graph=self.conn,
            num_batches=1,
            shuffle=True,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)

    def test_iterate_attr(self):
        loader = EdgeLoader(
            graph=self.conn,
            attributes=["time","is_train"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            loader_id=None,
            buffer_size=4
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("time", data)
            self.assertIn("is_train", data)
            num_batches += 1
        self.assertEqual(num_batches, 11)

    # TODO: test filter_by

class TestGDSHeteroEdgeLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="hetero")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=False,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 6)

    def test_iterate_as_homo(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=False,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            num_batches += 1
        self.assertEqual(num_batches, 6)

    def test_iterate_hetero(self):
        loader = EdgeLoader(
            graph=self.conn,
            attributes={"v0v0": ["is_train", "is_val"],
                        "v2v0": ["is_train", "is_val"]},
            batch_size=200,
            shuffle=False,
            filter_by=None,
            loader_id=None,
            buffer_size=4
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertEqual(len(data), 2)
            self.assertIsInstance(data["v0v0"], DataFrame)
            self.assertIsInstance(data["v2v0"], DataFrame)
            self.assertIn("is_val", data["v0v0"])
            self.assertIn("is_train", data["v0v0"])
            self.assertIn("is_val", data["v2v0"])
            self.assertIn("is_train", data["v2v0"])
            num_batches += 1
        self.assertEqual(num_batches, 9)

if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSEdgeLoader("test_init"))
    suite.addTest(TestGDSEdgeLoader("test_iterate"))
    suite.addTest(TestGDSEdgeLoader("test_whole_edgelist"))
    suite.addTest(TestGDSEdgeLoader("test_iterate_attr"))
    suite.addTest(TestGDSEdgeLoaderREST("test_init"))
    suite.addTest(TestGDSEdgeLoaderREST("test_iterate"))
    suite.addTest(TestGDSEdgeLoaderREST("test_whole_edgelist"))
    suite.addTest(TestGDSEdgeLoaderREST("test_iterate_attr"))
    suite.addTest(TestGDSHeteroEdgeLoaderREST("test_init"))
    suite.addTest(TestGDSHeteroEdgeLoaderREST("test_iterate_as_homo"))
    suite.addTest(TestGDSHeteroEdgeLoaderREST("test_iterate_hetero"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
