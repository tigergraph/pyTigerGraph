import unittest

from pyTigerGraphUnitTest import make_connection

from pyTigerGraph.gds.utilities import is_query_installed


class TestGDSDataLoaders(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def test_neighborLoader(self):
        loader = self.conn.gds.neighborLoader(
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            filter_by="train_mask",
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 9)

    def test_neighborLoader_multiple_filters(self):
        loaders = self.conn.gds.neighborLoader(
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            filter_by=["train_mask", "val_mask", "test_mask"],
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(is_query_installed(self.conn, loaders[0].query_name))
        self.assertEqual(len(loaders), 3)

    def test_graphLoader(self):
        loader = self.conn.gds.graphLoader(
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

    def test_vertexLoader(self):
        loader = self.conn.gds.vertexLoader(
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 9)

    def test_edgeLoader(self):
        loader = self.conn.gds.edgeLoader(
            batch_size=1024,
            shuffle=False,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 11)

    def test_edgeNeighborLoader(self):
        loader = self.conn.gds.edgeNeighborLoader(
            v_in_feats=["x"],
            e_extra_feats=["is_train"],
            batch_size=1024,
            num_neighbors=10,
            num_hops=2,
            shuffle=False,
            filter_by=None,
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 11)

    def test_configureKafka(self):
        self.conn.gds.configureKafka(kafka_address="kafka:9092")
        loader = self.conn.gds.neighborLoader(
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            filter_by="train_mask",
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        self.assertEqual(loader.kafka_address_consumer, "kafka:9092")
        self.assertEqual(loader.kafka_address_producer, "kafka:9092")

    def test_configureKafka_sasl_plaintext(self):
        self.conn.gds.configureKafka(
            kafka_address="34.127.11.236:9092",
            kafka_security_protocol="SASL_PLAINTEXT",
            kafka_sasl_mechanism="PLAIN",
            kafka_sasl_plain_username="bill",
            kafka_sasl_plain_password="bill",
        )
        loader = self.conn.gds.neighborLoader(
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            filter_by="train_mask",
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        self.assertEqual(loader.kafka_address_consumer, "34.127.11.236:9092")
        self.assertEqual(
            loader._payload["security_protocol"], "SASL_PLAINTEXT")
        self.assertEqual(loader._payload["sasl_mechanism"], "PLAIN")
        self.assertEqual(loader._payload["sasl_username"], "bill")
        self.assertEqual(loader._payload["sasl_password"], "bill")

    def test_configureKafka_sasl_ssl(self):
        self.conn.gds.configureKafka(
            kafka_address="pkc-6ojv2.us-west4.gcp.confluent.cloud:9092",
            kafka_replica_factor=3,
            kafka_max_msg_size=8388608,
            kafka_security_protocol="SASL_SSL",
            kafka_sasl_mechanism="PLAIN",
            kafka_sasl_plain_username="YIQM66T3BZZLSXBJ",
            kafka_sasl_plain_password="UgRdpSS34e2kYe8jZ9m7py4LgjkjxsGrePiaaMv/YCHRIjRmTJMpodS/0og8SYe8",
            kafka_producer_ca_location="/home/tigergraph/mlworkbench/ssl/cert.pem",
        )
        loader = self.conn.gds.edgeLoader(
            attributes=["time", "is_train"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
        )
        self.assertEqual(
            loader.kafka_address_consumer, "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092"
        )
        self.assertEqual(loader._payload["security_protocol"], "SASL_SSL")
        self.assertEqual(loader._payload["sasl_mechanism"], "PLAIN")
        self.assertEqual(loader._payload["sasl_username"], "YIQM66T3BZZLSXBJ")
        self.assertEqual(
            loader._payload["sasl_password"],
            "UgRdpSS34e2kYe8jZ9m7py4LgjkjxsGrePiaaMv/YCHRIjRmTJMpodS/0og8SYe8",
        )
        self.assertEqual(
            loader._payload["ssl_ca_location"],
            "/home/tigergraph/mlworkbench/ssl/cert.pem",
        )


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSDataLoaders("test_neighborLoader"))
    suite.addTest(TestGDSDataLoaders("test_neighborLoader_multiple_filters"))
    suite.addTest(TestGDSDataLoaders("test_graphLoader"))
    suite.addTest(TestGDSDataLoaders("test_vertexLoader"))
    suite.addTest(TestGDSDataLoaders("test_edgeLoader"))
    suite.addTest(TestGDSDataLoaders("test_edgeNeighborLoader"))
    suite.addTest(TestGDSDataLoaders("test_configureKafka"))
    # suite.addTest(TestGDSDataLoaders("test_configureKafka_sasl_plaintext"))
    # suite.addTest(TestGDSDataLoaders("test_configureKafka_sasl_ssl"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
