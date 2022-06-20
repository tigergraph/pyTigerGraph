import unittest

from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.gds.utilities import is_query_installed
from pyTigerGraph.gds.dataloaders import EdgeNeighborLoader
from torch_geometric.data import Data as pygData
from torch_geometric.data import HeteroData as pygHeteroData


class TestGDSEdgeNeighborLoaderKafka(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        # cls.conn.gsql("drop query all")
    
    def test_sasl_plaintext(self):
        loader = EdgeNeighborLoader(
            graph=self.conn,
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
            self.assertIn("is_seed", data)
            self.assertIn("is_train", data)
            self.assertGreater(data["x"].shape[0], 0)
            self.assertGreater(data["edge_index"].shape[1], 0)
            num_batches += 1
        self.assertEqual(num_batches, 11)

    def test_sasl_ssl(self):
        loader = EdgeNeighborLoader(
            graph=self.conn,
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
            self.assertIn("is_seed", data)
            self.assertIn("is_train", data)
            self.assertGreater(data["x"].shape[0], 0)
            self.assertGreater(data["edge_index"].shape[1], 0)
            num_batches += 1
        self.assertEqual(num_batches, 11)


class TestGDSEdgeNeighborLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = EdgeNeighborLoader(
            graph=self.conn,
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

    def test_iterate_pyg(self):
        loader = EdgeNeighborLoader(
            graph=self.conn,
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
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygData)
            self.assertIn("x", data)
            self.assertIn("is_seed", data)
            self.assertIn("is_train", data)
            self.assertGreater(data["x"].shape[0], 0)
            self.assertGreater(data["edge_index"].shape[1], 0)
            num_batches += 1
        self.assertEqual(num_batches, 11)
 

if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSEdgeNeighborLoaderKafka("test_sasl_plaintext"))
    suite.addTest(TestGDSEdgeNeighborLoaderKafka("test_sasl_ssl"))
    suite.addTest(TestGDSEdgeNeighborLoaderREST("test_init"))
    suite.addTest(TestGDSEdgeNeighborLoaderREST("test_iterate_pyg"))
    
    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
