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

    def test_sasl_plaintext(self):
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
            kafka_address="34.127.11.236:9092",
            kafka_security_protocol="SASL_PLAINTEXT",
            kafka_sasl_mechanism="PLAIN",
            kafka_sasl_plain_username="bill",
            kafka_sasl_plain_password="bill"
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

    def test_sasl_ssl(self):
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
            kafka_address="pkc-6ojv2.us-west4.gcp.confluent.cloud:9092",
            kafka_replica_factor=3,
            kafka_max_msg_size=8388608,
            kafka_security_protocol="SASL_SSL",
            kafka_sasl_mechanism="PLAIN",
            kafka_sasl_plain_username="YIQM66T3BZZLSXBJ",
            kafka_sasl_plain_password="UgRdpSS34e2kYe8jZ9m7py4LgjkjxsGrePiaaMv/YCHRIjRmTJMpodS/0og8SYe8",
            kafka_producer_ca_location="/home/tigergraph/mlworkbench/ssl/cert.pem"
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

    def test_string_attr(self):
        conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora2")
        loader = GraphLoader(
            graph=conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask", "name"],
            num_batches=1,
            shuffle=False,
            filter_by="train_mask",
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        data = loader.data
        # print(data)
        # print(data.name)
        self.assertIsInstance(data, pygData)
        self.assertIn("x", data)
        self.assertIn("y", data)
        self.assertIn("train_mask", data)
        self.assertIn("val_mask", data)
        self.assertIn("test_mask", data)
        self.assertIn("name", data)


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
            # print(num_batches, data)
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
    suite.addTest(TestGDSGraphLoader("test_sasl_plaintext"))
    suite.addTest(TestGDSGraphLoader("test_sasl_ssl"))
    suite.addTest(TestGDSGraphLoaderREST("test_init"))
    suite.addTest(TestGDSGraphLoaderREST("test_iterate_pyg"))
    suite.addTest(TestGDSGraphLoaderREST("test_iterate_df"))
    suite.addTest(TestGDSGraphLoaderREST("test_edge_attr"))
    suite.addTest(TestGDSGraphLoaderREST("test_string_attr"))
    suite.addTest(TestGDSHeteroGraphLoaderREST("test_init"))
    suite.addTest(TestGDSHeteroGraphLoaderREST("test_iterate_pyg"))
    suite.addTest(TestGDSHeteroGraphLoaderREST("test_iterate_df"))
    suite.addTest(TestGDSHeteroGraphLoaderREST("test_edge_attr"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
