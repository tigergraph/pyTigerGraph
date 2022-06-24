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

    def test_sasl_plaintext(self):
        loader = EdgeLoader(
            graph=self.conn,
            attributes=["time","is_train"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
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
            self.assertIn("time", data)
            self.assertIn("is_train", data)
            num_batches += 1
        self.assertEqual(num_batches, 11)

    def test_sasl_ssl(self):
        loader = EdgeLoader(
            graph=self.conn,
            attributes=["time","is_train"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
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
            self.assertIn("time", data)
            self.assertIn("is_train", data)
            num_batches += 1
        self.assertEqual(num_batches, 11)

    def test_sasl_ssl_kafka_config(self):
        self.conn.gds.configureKafka(kafka_address="pkc-6ojv2.us-west4.gcp.confluent.cloud:9092",
            kafka_replica_factor=3,
            kafka_max_msg_size=8388608,
            kafka_security_protocol="SASL_SSL",
            kafka_sasl_mechanism="PLAIN",
            kafka_sasl_plain_username="YIQM66T3BZZLSXBJ",
            kafka_sasl_plain_password="UgRdpSS34e2kYe8jZ9m7py4LgjkjxsGrePiaaMv/YCHRIjRmTJMpodS/0og8SYe8",
            kafka_producer_ca_location="/home/tigergraph/mlworkbench/ssl/cert.pem"
        )
        loader = self.conn.gds.edgeLoader(attributes=["time","is_train"],
                                        batch_size=1024,
                                        shuffle=True,
                                        filter_by=None,
                                        loader_id=None,
                                        buffer_size=4)
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
    suite.addTest(TestGDSEdgeLoader("test_sasl_plaintext"))
    suite.addTest(TestGDSEdgeLoader("test_sasl_ssl"))
    suite.addTest(TestGDSEdgeLoader("test_sasl_ssl_kafka_config"))
    suite.addTest(TestGDSEdgeLoaderREST("test_init"))
    suite.addTest(TestGDSEdgeLoaderREST("test_iterate"))
    suite.addTest(TestGDSEdgeLoaderREST("test_whole_edgelist"))
    suite.addTest(TestGDSEdgeLoaderREST("test_iterate_attr"))
    suite.addTest(TestGDSHeteroEdgeLoaderREST("test_init"))
    suite.addTest(TestGDSHeteroEdgeLoaderREST("test_iterate_as_homo"))
    suite.addTest(TestGDSHeteroEdgeLoaderREST("test_iterate_hetero"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
