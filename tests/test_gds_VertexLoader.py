import unittest

from pandas import DataFrame
from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.gds.dataloaders import VertexLoader
from pyTigerGraph.gds.utilities import is_query_installed


class TestGDSVertexLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            loader_id=None,
            buffer_size=4,
            kafka_address="34.82.171.137:9092",
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 9)

    def test_iterate(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            loader_id=None,
            buffer_size=4,
            kafka_address="34.82.171.137:9092",
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("x", data.columns)
            self.assertIn("y", data.columns)
            self.assertIn("train_mask", data.columns)
            self.assertIn("val_mask", data.columns)
            self.assertIn("test_mask", data.columns)
            num_batches += 1
        self.assertEqual(num_batches, 9)

    def test_all_vertices(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            num_batches=1,
            shuffle=False,
            filter_by="train_mask",
            loader_id=None,
            buffer_size=4,
            kafka_address="34.82.171.137:9092",
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)
        self.assertIn("x", data.columns)
        self.assertIn("y", data.columns)
        self.assertIn("train_mask", data.columns)
        self.assertIn("val_mask", data.columns)
        self.assertIn("test_mask", data.columns)

    def test_sasl_plaintext(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
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
            self.assertIn("train_mask", data.columns)
            self.assertIn("val_mask", data.columns)
            self.assertIn("test_mask", data.columns)
            num_batches += 1
        self.assertEqual(num_batches, 9)

    def test_sasl_ssl(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
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
            self.assertIn("train_mask", data.columns)
            self.assertIn("val_mask", data.columns)
            self.assertIn("test_mask", data.columns)
            num_batches += 1
        self.assertEqual(num_batches, 9)

class TestGDSVertexLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 9)

    def test_iterate(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            loader_id=None,
            buffer_size=4,
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("x", data.columns)
            self.assertIn("y", data.columns)
            self.assertIn("train_mask", data.columns)
            self.assertIn("val_mask", data.columns)
            self.assertIn("test_mask", data.columns)
            num_batches += 1
        self.assertEqual(num_batches, 9)

    def test_all_vertices(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            num_batches=1,
            shuffle=False,
            filter_by="train_mask",
            loader_id=None,
            buffer_size=4,
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)
        self.assertIn("x", data.columns)
        self.assertIn("y", data.columns)
        self.assertIn("train_mask", data.columns)
        self.assertIn("val_mask", data.columns)
        self.assertIn("test_mask", data.columns)

    def test_string_attr(self):
        conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora2")
        loader = VertexLoader(
            graph=conn,
            attributes=["y", "train_mask", "val_mask", "test_mask", "name"],
            num_batches=1,
            shuffle=False,
            filter_by="train_mask",
            loader_id=None,
            buffer_size=4,
        )
        data = loader.data
        # print(data.head())
        self.assertIsInstance(data, DataFrame)
        self.assertIn("y", data.columns)
        self.assertIn("train_mask", data.columns)
        self.assertIn("val_mask", data.columns)
        self.assertIn("test_mask", data.columns)
        self.assertIn("name", data.columns)


class TestGDSHeteroVertexLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="hetero")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes={"v0": ["x", "y"],
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
        loader = VertexLoader(
            graph=self.conn,
            attributes={"v0": ["x", "y"],
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
            self.assertIn("y", data["v0"].columns)
            self.assertIn("x", data["v1"].columns)
            num_batches += 1
        self.assertEqual(num_batches, 10)

    def test_all_vertices(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes={"v0": ["x", "y"],
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
        self.assertTupleEqual(data["v0"].shape, (76, 3))
        self.assertIsInstance(data["v1"], DataFrame)
        self.assertTupleEqual(data["v1"].shape, (110, 2))
        self.assertIn("x", data["v0"].columns)
        self.assertIn("y", data["v0"].columns)
        self.assertIn("x", data["v1"].columns)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSVertexLoader("test_init"))
    suite.addTest(TestGDSVertexLoader("test_iterate"))
    suite.addTest(TestGDSVertexLoader("test_all_vertices"))
    suite.addTest(TestGDSVertexLoader("test_sasl_plaintext"))
    suite.addTest(TestGDSVertexLoader("test_sasl_ssl"))
    suite.addTest(TestGDSVertexLoaderREST("test_init"))
    suite.addTest(TestGDSVertexLoaderREST("test_iterate"))
    suite.addTest(TestGDSVertexLoaderREST("test_all_vertices"))
    suite.addTest(TestGDSVertexLoaderREST("test_string_attr"))
    suite.addTest(TestGDSHeteroVertexLoaderREST("test_init"))
    suite.addTest(TestGDSHeteroVertexLoaderREST("test_iterate"))
    suite.addTest(TestGDSHeteroVertexLoaderREST("test_all_vertices"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
