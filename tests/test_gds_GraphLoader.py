import unittest

from pandas import DataFrame
from pyTigerGraphUnitTest import make_connection
from torch_geometric.data import Data as pygData
from torch_geometric.data import HeteroData as pygHeteroData

from pyTigerGraph.gds.dataloaders import GraphLoader
from pyTigerGraph.gds.utilities import is_query_installed


class TestGDSGraphLoaderKafka(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def test_init(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            output_format="dataframe",
            kafka_address="kafka:9092",
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_iterate_pyg(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            output_format="PyG",
            kafka_address="kafka:9092",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygData)
            self.assertIn("x", data)
            self.assertIn("y", data)
            self.assertIn("train_mask", data)
            self.assertIn("val_mask", data)
            self.assertIn("test_mask", data)
            batch_sizes.append(data["edge_index"].shape[1])
            num_batches += 1
        self.assertEqual(num_batches, 11)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 1024)
        self.assertLessEqual(batch_sizes[-1], 1024)

    def test_iterate_df(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            output_format="dataframe",
            kafka_address="kafka:9092",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data, flush=True)
            self.assertIsInstance(data[0], DataFrame)
            self.assertIn("x", data[0].columns)
            self.assertIn("y", data[0].columns)
            self.assertIn("train_mask", data[0].columns)
            self.assertIn("val_mask", data[0].columns)
            self.assertIn("test_mask", data[0].columns)
            self.assertIsInstance(data[1], DataFrame)
            self.assertIn("source", data[1])
            self.assertIn("target", data[1])
            batch_sizes.append(data[1].shape[0])
            num_batches += 1
        self.assertEqual(num_batches, 11)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 1024)
        self.assertLessEqual(batch_sizes[-1], 1024)

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
            output_format="PyG",
            kafka_address="kafka:9092",
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
        cls.conn = make_connection(graphname="Cora")

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
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_iterate_pyg(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            output_format="PyG",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygData)
            self.assertIn("x", data)
            self.assertIn("y", data)
            self.assertIn("train_mask", data)
            self.assertIn("val_mask", data)
            self.assertIn("test_mask", data)
            num_batches += 1
            batch_sizes.append(data["edge_index"].shape[1])
        self.assertEqual(num_batches, 11)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 1024)
        self.assertLessEqual(batch_sizes[-1], 1024)

    def test_iterate_df(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            output_format="dataframe",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data[0], DataFrame)
            self.assertIsInstance(data[1], DataFrame)
            self.assertIn("x", data[0].columns)
            self.assertIn("y", data[0].columns)
            self.assertIn("train_mask", data[0].columns)
            self.assertIn("val_mask", data[0].columns)
            self.assertIn("test_mask", data[0].columns)
            self.assertIn("source", data[1])
            self.assertIn("target", data[1])
            batch_sizes.append(data[1].shape[0])
            num_batches += 1
        self.assertEqual(num_batches, 11)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 1024)
        self.assertLessEqual(batch_sizes[-1], 1024)

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
            output_format="PyG",
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

    def test_list_string_attr(self):
        conn = make_connection(graphname="Social")

        loader = GraphLoader(
            graph=conn,
            v_in_feats=["age"],
            v_extra_feats=["state"],
            e_extra_feats=["duration"],
            num_batches=1,
            shuffle=False,
            output_format="PyG",
        )
        data = loader.data
        # print(data)
        # print(data["state"])
        # print(data["duration"])
        self.assertIsInstance(data, pygData)
        self.assertEqual(data["x"].shape[0], 7)
        self.assertEqual(len(data["state"]), 7)
        self.assertEqual(data["edge_index"].shape[1], 14)
        self.assertEqual(len(data["duration"]), 14)

    def test_iterate_spektral(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            output_format="spektral",
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            # self.assertIsInstance(data, spData)
            self.assertIn("x", data)
            self.assertIn("y", data)
            self.assertIn("train_mask", data)
            self.assertIn("val_mask", data)
            self.assertIn("test_mask", data)
            num_batches += 1
        self.assertEqual(num_batches, 11)


class TestGDSHeteroGraphLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="hetero")

    def test_init(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"],
                        "v1": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=1024,
            shuffle=False,
            output_format="dataframe",
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_iterate_pyg(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"],
                        "v1": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=300,
            shuffle=True,
            output_format="PyG",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygHeteroData)
            self.assertTrue("v0" in data.node_types or "v1" in data.node_types)
            if "v0" in data.node_types:
                self.assertIn("x", data["v0"])
                self.assertIn("y", data["v0"])
                self.assertIn("train_mask", data["v0"])
                self.assertIn("val_mask", data["v0"])
                self.assertIn("test_mask", data["v0"])
            if "v1" in data.node_types:
                self.assertIn("x", data["v1"])
            self.assertTrue(('v0', 'v0v0', 'v0') in data.edge_types or ('v1', 'v1v1', 'v1') in data.edge_types)
            batchsize = 0
            if ('v0', 'v0v0', 'v0') in data.edge_types:
                batchsize += data["v0", "v0v0", "v0"].edge_index.shape[1]
            if ('v1', 'v1v1', 'v1') in data.edge_types:
                batchsize += data["v1", "v1v1", "v1"].edge_index.shape[1]
            batch_sizes.append(batchsize)
            num_batches += 1
        self.assertEqual(num_batches, 6)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 300)
        self.assertLessEqual(batch_sizes[-1], 300)

    def test_iterate_df(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"],
                        "v1": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=300,
            shuffle=False,
            output_format="dataframe",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            self.assertTrue("v0" in data[0] or "v1" in data[0])
            if "v0" in data[0]:
                self.assertIsInstance(data[0]["v0"], DataFrame)
                self.assertIn("x", data[0]["v0"].columns)
                self.assertIn("y", data[0]["v0"].columns)
                self.assertIn("train_mask", data[0]["v0"].columns)
                self.assertIn("val_mask", data[0]["v0"].columns)
                self.assertIn("test_mask", data[0]["v0"].columns)
            if "v1" in data[0]:
                self.assertIsInstance(data[0]["v1"], DataFrame)
                self.assertIn("x", data[0]["v1"].columns)
            self.assertTrue("v0v0" in data[1] or "v1v1" in data[1])
            batchsize = 0
            if "v0v0" in data[1]:
                self.assertIsInstance(data[1]["v0v0"], DataFrame)
                batchsize += data[1]["v0v0"].shape[0]
                self.assertEqual(data[1]["v0v0"].shape[1], 2)
            if "v1v1" in data[1]:
                self.assertIsInstance(data[1]["v1v1"], DataFrame)
                batchsize += data[1]["v1v1"].shape[0]
                self.assertEqual(data[1]["v1v1"].shape[1], 2)
            batch_sizes.append(batchsize)
            num_batches += 1
        self.assertEqual(num_batches, 6)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 300)
        self.assertLessEqual(batch_sizes[-1], 300)

    def test_edge_attr(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"],
                        "v1": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            e_extra_feats={"v0v0": ["is_train", "is_val"],
                           "v1v1": ["is_train", "is_val"]},
            batch_size=300,
            shuffle=False,
            output_format="PyG",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygHeteroData)
            self.assertTrue("v0" in data.node_types or "v1" in data.node_types)
            if "v0" in data.node_types:
                self.assertIn("x", data["v0"])
                self.assertIn("y", data["v0"])
                self.assertIn("train_mask", data["v0"])
                self.assertIn("val_mask", data["v0"])
                self.assertIn("test_mask", data["v0"])
            if "v1" in data.node_types:
                self.assertIn("x", data["v1"])
            self.assertTrue(('v0', 'v0v0', 'v0') in data.edge_types or ('v1', 'v1v1', 'v1') in data.edge_types)
            batchsize = 0
            if ('v0', 'v0v0', 'v0') in data.edge_types:
                self.assertIn("is_train", data["v0", "v0v0", "v0"])
                self.assertIn("is_val", data["v0", "v0v0", "v0"])
                batchsize += data["v0", "v0v0", "v0"].edge_index.shape[1]
            if ('v1', 'v1v1', 'v1') in data.edge_types:
                self.assertIn("is_train", data["v1", "v1v1", "v1"])
                self.assertIn("is_val", data["v1", "v1v1", "v1"])
                batchsize += data["v1", "v1v1", "v1"].edge_index.shape[1]
            batch_sizes.append(batchsize)
            num_batches += 1
        self.assertEqual(num_batches, 6)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 300)
        self.assertLessEqual(batch_sizes[-1], 300)


class TestGDSHeteroGraphLoaderKafka(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="hetero")

    def test_init(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"],
                        "v1": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=1024,
            shuffle=False,
            output_format="dataframe",
            kafka_address="kafka:9092",
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_iterate_pyg(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"],
                        "v1": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=300,
            shuffle=True,
            output_format="PyG",
            kafka_address="kafka:9092",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygHeteroData)
            self.assertTrue("v0" in data.node_types or "v1" in data.node_types)
            if "v0" in data.node_types:
                self.assertIn("x", data["v0"])
                self.assertIn("y", data["v0"])
                self.assertIn("train_mask", data["v0"])
                self.assertIn("val_mask", data["v0"])
                self.assertIn("test_mask", data["v0"])
            if "v1" in data.node_types:
                self.assertIn("x", data["v1"])
            self.assertTrue(('v0', 'v0v0', 'v0') in data.edge_types or ('v1', 'v1v1', 'v1') in data.edge_types)
            batchsize = 0
            if ('v0', 'v0v0', 'v0') in data.edge_types:
                batchsize += data["v0", "v0v0", "v0"].edge_index.shape[1]
            if ('v1', 'v1v1', 'v1') in data.edge_types:
                batchsize += data["v1", "v1v1", "v1"].edge_index.shape[1]
            batch_sizes.append(batchsize)
            num_batches += 1
        self.assertEqual(num_batches, 6)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 300)
        self.assertLessEqual(batch_sizes[-1], 300)

    def test_iterate_df(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"],
                        "v1": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=300,
            shuffle=False,
            output_format="dataframe",
            kafka_address="kafka:9092",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            self.assertTrue("v0" in data[0] or "v1" in data[0])
            if "v0" in data[0]:
                self.assertIsInstance(data[0]["v0"], DataFrame)
                self.assertIn("x", data[0]["v0"].columns)
                self.assertIn("y", data[0]["v0"].columns)
                self.assertIn("train_mask", data[0]["v0"].columns)
                self.assertIn("val_mask", data[0]["v0"].columns)
                self.assertIn("test_mask", data[0]["v0"].columns)
            if "v1" in data[0]:
                self.assertIsInstance(data[0]["v1"], DataFrame)
                self.assertIn("x", data[0]["v1"].columns)
            self.assertTrue("v0v0" in data[1] or "v1v1" in data[1])
            batchsize = 0
            if "v0v0" in data[1]:
                self.assertIsInstance(data[1]["v0v0"], DataFrame)
                batchsize += data[1]["v0v0"].shape[0]
                self.assertEqual(data[1]["v0v0"].shape[1], 2)
            if "v1v1" in data[1]:
                self.assertIsInstance(data[1]["v1v1"], DataFrame)
                batchsize += data[1]["v1v1"].shape[0]
                self.assertEqual(data[1]["v1v1"].shape[1], 2)
            batch_sizes.append(batchsize)
            num_batches += 1
        self.assertEqual(num_batches, 6)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 300)
        self.assertLessEqual(batch_sizes[-1], 300)

    def test_edge_attr(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"],
                        "v1": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            e_extra_feats={"v0v0": ["is_train", "is_val"],
                           "v1v1": ["is_train", "is_val"]},
            batch_size=300,
            shuffle=False,
            output_format="PyG",
            kafka_address="kafka:9092",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygHeteroData)
            self.assertTrue("v0" in data.node_types or "v1" in data.node_types)
            if "v0" in data.node_types:
                self.assertIn("x", data["v0"])
                self.assertIn("y", data["v0"])
                self.assertIn("train_mask", data["v0"])
                self.assertIn("val_mask", data["v0"])
                self.assertIn("test_mask", data["v0"])
            if "v1" in data.node_types:
                self.assertIn("x", data["v1"])
            self.assertTrue(('v0', 'v0v0', 'v0') in data.edge_types or ('v1', 'v1v1', 'v1') in data.edge_types)
            batchsize = 0
            if ('v0', 'v0v0', 'v0') in data.edge_types:
                self.assertIn("is_train", data["v0", "v0v0", "v0"])
                self.assertIn("is_val", data["v0", "v0v0", "v0"])
                batchsize += data["v0", "v0v0", "v0"].edge_index.shape[1]
            if ('v1', 'v1v1', 'v1') in data.edge_types:
                self.assertIn("is_train", data["v1", "v1v1", "v1"])
                self.assertIn("is_val", data["v1", "v1v1", "v1"])
                batchsize += data["v1", "v1v1", "v1"].edge_index.shape[1]
            batch_sizes.append(batchsize)
            num_batches += 1
        self.assertEqual(num_batches, 6)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 300)
        self.assertLessEqual(batch_sizes[-1], 300)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSGraphLoaderKafka("test_init"))
    suite.addTest(TestGDSGraphLoaderKafka("test_iterate_pyg"))
    suite.addTest(TestGDSGraphLoaderKafka("test_iterate_df"))
    suite.addTest(TestGDSGraphLoaderKafka("test_edge_attr"))
    # suite.addTest(TestGDSGraphLoader("test_sasl_plaintext"))
    # suite.addTest(TestGDSGraphLoader("test_sasl_ssl"))
    suite.addTest(TestGDSGraphLoaderREST("test_init"))
    suite.addTest(TestGDSGraphLoaderREST("test_iterate_pyg"))
    suite.addTest(TestGDSGraphLoaderREST("test_iterate_df"))
    suite.addTest(TestGDSGraphLoaderREST("test_edge_attr"))
    suite.addTest(TestGDSGraphLoaderREST("test_list_string_attr"))
    suite.addTest(TestGDSHeteroGraphLoaderREST("test_init"))
    suite.addTest(TestGDSHeteroGraphLoaderREST("test_iterate_pyg"))
    suite.addTest(TestGDSHeteroGraphLoaderREST("test_iterate_df"))
    suite.addTest(TestGDSHeteroGraphLoaderREST("test_edge_attr"))
    suite.addTest(TestGDSHeteroGraphLoaderKafka("test_init"))
    suite.addTest(TestGDSHeteroGraphLoaderKafka("test_iterate_pyg"))
    suite.addTest(TestGDSHeteroGraphLoaderKafka("test_iterate_df"))
    suite.addTest(TestGDSHeteroGraphLoaderKafka("test_edge_attr"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
