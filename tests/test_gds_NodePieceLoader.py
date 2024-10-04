from lib2to3.pytree import Node
import unittest
from pyTigerGraphUnitTest import make_connection

from pandas import DataFrame
from pyTigerGraph.gds.dataloaders import NodePieceLoader
from pyTigerGraph.gds.utilities import is_query_installed


class TestGDSNodePieceLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def test_init(self):
        loader = NodePieceLoader(
            graph=self.conn,
            v_feats=["x", "y", "train_mask", "val_mask", "test_mask"],
            compute_anchors=True,
            anchor_percentage=0.5,
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            loader_id=None,
            buffer_size=4,
            kafka_address="kafka:9092",
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 9)

    def test_iterate(self):
        loader = NodePieceLoader(
            graph=self.conn,
            v_feats=["x", "y", "train_mask", "val_mask", "test_mask"],
            compute_anchors=True,
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            anchor_percentage=0.5,
            loader_id=None,
            buffer_size=4,
            kafka_address="kafka:9092",
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("x", data.columns)
            self.assertIn("y", data.columns)
            self.assertIn("relational_context", data.columns)
            self.assertIn("anchors", data.columns)
            self.assertIn("train_mask", data.columns)
            self.assertIn("val_mask", data.columns)
            self.assertIn("test_mask", data.columns)
            num_batches += 1
        self.assertEqual(num_batches, 9)

    def test_all_vertices(self):
        loader = NodePieceLoader(
            graph=self.conn,
            v_feats=["x", "y", "train_mask", "val_mask", "test_mask"],
            compute_anchors=True,
            shuffle=True,
            filter_by="train_mask",
            anchor_percentage=0.5,
            loader_id=None,
            buffer_size=4,
            kafka_address="kafka:9092",
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)
        self.assertIn("x", data.columns)
        self.assertIn("y", data.columns)
        self.assertIn("relational_context", data.columns)
        self.assertIn("anchors", data.columns)
        self.assertIn("train_mask", data.columns)
        self.assertIn("val_mask", data.columns)
        self.assertIn("test_mask", data.columns)

    def test_sasl_plaintext(self):
        loader = NodePieceLoader(
            graph=self.conn,
            v_feats=["x", "y", "train_mask", "val_mask", "test_mask"],
            compute_anchors=True,
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            anchor_percentage=0.5,
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
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("x", data.columns)
            self.assertIn("y", data.columns)
            self.assertIn("relational_context", data.columns)
            self.assertIn("anchors", data.columns)
            self.assertIn("train_mask", data.columns)
            self.assertIn("val_mask", data.columns)
            self.assertIn("test_mask", data.columns)
            num_batches += 1
        self.assertEqual(num_batches, 9)

    def test_sasl_ssl(self):
        loader = NodePieceLoader(
            graph=self.conn,
            v_feats=["x", "y", "train_mask", "val_mask", "test_mask"],
            compute_anchors=True,
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            anchor_percentage=0.5,
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
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("x", data.columns)
            self.assertIn("y", data.columns)
            self.assertIn("relational_context", data.columns)
            self.assertIn("anchors", data.columns)
            self.assertIn("train_mask", data.columns)
            self.assertIn("val_mask", data.columns)
            self.assertIn("test_mask", data.columns)
            num_batches += 1
        self.assertEqual(num_batches, 9)


class TestGDSNodePieceLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def test_init(self):
        loader = NodePieceLoader(
            graph=self.conn,
            v_feats=["x", "y", "train_mask", "val_mask", "test_mask"],
            compute_anchors=True,
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            anchor_percentage=0.5,
            loader_id=None,
            buffer_size=4
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 9)

    def test_iterate(self):
        loader = NodePieceLoader(
            graph=self.conn,
            v_feats=["x", "y", "train_mask", "val_mask", "test_mask"],
            compute_anchors=True,
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            anchor_percentage=0.5,
            loader_id=None,
            buffer_size=4
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("x", data.columns)
            self.assertIn("y", data.columns)
            self.assertIn("relational_context", data.columns)
            self.assertIn("anchors", data.columns)
            self.assertIn("train_mask", data.columns)
            self.assertIn("val_mask", data.columns)
            self.assertIn("test_mask", data.columns)
            num_batches += 1
        self.assertEqual(num_batches, 9)

    def test_all_vertices(self):
        loader = NodePieceLoader(
            graph=self.conn,
            v_feats=["x", "y", "train_mask", "val_mask", "test_mask"],
            compute_anchors=True,
            shuffle=True,
            filter_by="train_mask",
            anchor_percentage=0.5,
            loader_id=None,
            buffer_size=4
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)
        self.assertIn("x", data.columns)
        self.assertIn("y", data.columns)
        self.assertIn("relational_context", data.columns)
        self.assertIn("anchors", data.columns)
        self.assertIn("train_mask", data.columns)
        self.assertIn("val_mask", data.columns)
        self.assertIn("test_mask", data.columns)


class TestGDSHeteroNodePieceLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="hetero")

    def test_init(self):
        loader = NodePieceLoader(
            graph=self.conn,
            compute_anchors=True,
            anchor_percentage=0.5,
            v_feats={"v0": ["x", "y"],
                     "v1": ["x"]},
            batch_size=20,
            shuffle=True,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 10)

    def test_iterate(self):
        loader = NodePieceLoader(
            compute_anchors=True,
            anchor_percentage=0.5,
            graph=self.conn,
            v_feats={"v0": ["x", "y"],
                     "v1": ["x"]},
            batch_size=20,
            shuffle=True,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data["v0"], DataFrame)
            self.assertIsInstance(data["v1"], DataFrame)
            self.assertIn("x", data["v0"].columns)
            self.assertIn("relational_context", data["v0"].columns)
            self.assertIn("anchors", data["v0"].columns)
            self.assertIn("y", data["v0"].columns)
            self.assertIn("x", data["v1"].columns)
            self.assertIn("relational_context", data["v1"].columns)
            self.assertIn("anchors", data["v1"].columns)
            num_batches += 1
        self.assertEqual(num_batches, 10)

    def test_all_vertices(self):
        loader = NodePieceLoader(
            graph=self.conn,
            compute_anchors=True,
            anchor_percentage=0.5,
            v_feats={"v0": ["x", "y"],
                     "v1": ["x"]},
            num_batches=1,
            shuffle=False,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data["v0"], DataFrame)
        self.assertTupleEqual(data["v0"].shape, (76, 6))
        self.assertIsInstance(data["v1"], DataFrame)
        self.assertIn("x", data["v0"].columns)
        self.assertIn("y", data["v0"].columns)
        self.assertIn("x", data["v1"].columns)
        self.assertIn("anchors", data["v0"].columns)
        self.assertIn("relational_context", data["v0"].columns)
        self.assertIn("anchors", data["v1"].columns)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSNodePieceLoader("test_init"))
    suite.addTest(TestGDSNodePieceLoader("test_iterate"))
    suite.addTest(TestGDSNodePieceLoader("test_all_vertices"))
    # suite.addTest(TestGDSNodePieceLoader("test_sasl_plaintext"))
    # suite.addTest(TestGDSNodePieceLoader("test_sasl_ssl"))
    suite.addTest(TestGDSNodePieceLoaderREST("test_init"))
    suite.addTest(TestGDSNodePieceLoaderREST("test_iterate"))
    suite.addTest(TestGDSNodePieceLoaderREST("test_all_vertices"))
    suite.addTest(TestGDSHeteroNodePieceLoaderREST("test_init"))
    suite.addTest(TestGDSHeteroNodePieceLoaderREST("test_iterate"))
    suite.addTest(TestGDSHeteroNodePieceLoaderREST("test_all_vertices"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
