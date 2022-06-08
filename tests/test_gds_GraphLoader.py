import unittest

from pandas import DataFrame
from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.gds.dataloaders import GraphLoader
from pyTigerGraph.gds.utilities import is_query_installed
from torch_geometric.data import Data as pygData
from torch_geometric.data import HeteroData as pygHeteroData


class TestGDSGraphLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            output_format="dataframe",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
            kafka_address="34.82.171.137:9092",
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 11)

    def test_iterate_pyg(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
            kafka_address="34.82.171.137:9092",
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygData)
            self.assertIn("x", data)
            self.assertIn("y", data)
            self.assertIn("train_mask", data)
            self.assertIn("val_mask", data)
            self.assertIn("test_mask", data)
            num_batches += 1
        self.assertEqual(num_batches, 11)

    def test_iterate_df(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            output_format="dataframe",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
            kafka_address="34.82.171.137:9092",
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data[0], DataFrame)
            self.assertIsInstance(data[1], DataFrame)
            self.assertIn("x", data[0].columns)
            self.assertIn("y", data[0].columns)
            self.assertIn("train_mask", data[0].columns)
            self.assertIn("val_mask", data[0].columns)
            self.assertIn("test_mask", data[0].columns)
            num_batches += 1
        self.assertEqual(num_batches, 11)

    def test_edge_attr(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            e_in_feats=["time"],
            e_extra_feats=["is_train"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
            kafka_address="34.82.171.137:9092",
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygData)
            self.assertIn("x", data)
            self.assertIn("y", data)
            self.assertIn("train_mask", data)
            self.assertIn("val_mask", data)
            self.assertIn("test_mask", data)
            self.assertIn("edge_feat", data)
            self.assertIn("is_train", data)
            num_batches += 1
        self.assertEqual(num_batches, 11)


class TestGDSGraphLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            output_format="dataframe",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 11)

    def test_iterate_pyg(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygData)
            self.assertIn("x", data)
            self.assertIn("y", data)
            self.assertIn("train_mask", data)
            self.assertIn("val_mask", data)
            self.assertIn("test_mask", data)
            num_batches += 1
        self.assertEqual(num_batches, 11)

    def test_iterate_df(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            output_format="dataframe",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data[0], DataFrame)
            self.assertIsInstance(data[1], DataFrame)
            self.assertIn("x", data[0].columns)
            self.assertIn("y", data[0].columns)
            self.assertIn("train_mask", data[0].columns)
            self.assertIn("val_mask", data[0].columns)
            self.assertIn("test_mask", data[0].columns)
            num_batches += 1
        self.assertEqual(num_batches, 11)

    def test_edge_attr(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            e_in_feats=["time"],
            e_extra_feats=["is_train"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygData)
            self.assertIn("x", data)
            self.assertIn("y", data)
            self.assertIn("train_mask", data)
            self.assertIn("val_mask", data)
            self.assertIn("test_mask", data)
            self.assertIn("edge_feat", data)
            self.assertIn("is_train", data)
            num_batches += 1
        self.assertEqual(num_batches, 11)


class TestGDSHeteroGraphLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="hetero")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"],
                        "v1": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=1024,
            shuffle=False,
            filter_by=None,
            output_format="dataframe",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 6)

    def test_iterate_pyg(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"],
                        "v1": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygHeteroData)
            self.assertIn("x", data["v0"])
            self.assertIn("y", data["v0"])
            self.assertIn("train_mask", data["v0"])
            self.assertIn("val_mask", data["v0"])
            self.assertIn("test_mask", data["v0"])
            self.assertIn("x", data["v1"])
            num_batches += 1
        self.assertEqual(num_batches, 6)

    def test_iterate_df(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"],
                        "v1": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=1024,
            shuffle=False,
            filter_by=None,
            output_format="dataframe",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data[0]["v0"], DataFrame)
            self.assertIsInstance(data[0]["v1"], DataFrame)
            self.assertIsInstance(data[1]["v0v0"], DataFrame)
            self.assertIsInstance(data[1]["v1v1"], DataFrame)
            self.assertIn("x", data[0]["v0"].columns)
            self.assertIn("y", data[0]["v0"].columns)
            self.assertIn("train_mask", data[0]["v0"].columns)
            self.assertIn("val_mask", data[0]["v0"].columns)
            self.assertIn("test_mask", data[0]["v0"].columns)
            self.assertIn("x", data[0]["v1"].columns)
            num_batches += 1
        self.assertEqual(num_batches, 6)

    def test_edge_attr(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"],
                        "v1": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            e_extra_feats={"v0v0": ["is_train", "is_val"],
                           "v1v1": ["is_train", "is_val"]},
            batch_size=1024,
            shuffle=False,
            filter_by=None,
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4
        )
        num_batches = 0
        for data in loader:
            print(num_batches, data)
            self.assertIsInstance(data, pygHeteroData)
            self.assertIn("x", data["v0"])
            self.assertIn("y", data["v0"])
            self.assertIn("train_mask", data["v0"])
            self.assertIn("val_mask", data["v0"])
            self.assertIn("test_mask", data["v0"])
            self.assertIn("x", data["v1"])
            self.assertIn("is_train", data["v0v0"])
            self.assertIn("is_train", data["v1v1"])
            self.assertIn("is_val", data["v0v0"])
            self.assertIn("is_val", data["v1v1"])
            num_batches += 1
        self.assertEqual(num_batches, 2)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSGraphLoader("test_init"))
    suite.addTest(TestGDSGraphLoader("test_iterate_pyg"))
    suite.addTest(TestGDSGraphLoader("test_iterate_df"))
    suite.addTest(TestGDSGraphLoader("test_edge_attr"))
    suite.addTest(TestGDSGraphLoaderREST("test_init"))
    suite.addTest(TestGDSGraphLoaderREST("test_iterate_pyg"))
    suite.addTest(TestGDSGraphLoaderREST("test_iterate_df"))
    suite.addTest(TestGDSGraphLoaderREST("test_edge_attr"))
    suite.addTest(TestGDSHeteroGraphLoaderREST("test_init"))
    suite.addTest(TestGDSHeteroGraphLoaderREST("test_iterate_pyg"))
    suite.addTest(TestGDSHeteroGraphLoaderREST("test_iterate_df"))
    suite.addTest(TestGDSHeteroGraphLoaderREST("test_edge_attr"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
