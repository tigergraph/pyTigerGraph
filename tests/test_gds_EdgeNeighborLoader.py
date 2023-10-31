import unittest

from pyTigerGraphUnitTest import make_connection
from torch_geometric.data import Data as pygData
from torch_geometric.data import HeteroData as pygHeteroData

from pyTigerGraph.gds.dataloaders import EdgeNeighborLoader
from pyTigerGraph.gds.utilities import is_query_installed


class TestGDSEdgeNeighborLoaderKafka(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

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
            kafka_address="kafka:9092",
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_iterate_pyg(self):
        loader = EdgeNeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            e_extra_feats=["is_train"],
            batch_size=1024,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            filter_by=None,
            output_format="PyG",
            kafka_address="kafka:9092",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygData)
            self.assertIn("x", data)
            self.assertIn("is_seed", data)
            self.assertIn("is_train", data)
            self.assertGreater(data["x"].shape[0], 0)
            self.assertGreater(data["edge_index"].shape[1], 0)
            num_batches += 1
            batch_sizes.append(int(data["is_seed"].sum()))
        self.assertEqual(num_batches, 11)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 1024)
        self.assertLessEqual(batch_sizes[-1], 1024)

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
            kafka_producer_ca_location="/home/tigergraph/mlworkbench/ssl/cert.pem",
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
        cls.conn = make_connection(graphname="Cora")

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
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_iterate_pyg(self):
        loader = EdgeNeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            e_extra_feats=["is_train"],
            batch_size=1024,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            filter_by=None,
            output_format="PyG",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygData)
            self.assertIn("x", data)
            self.assertIn("is_seed", data)
            self.assertIn("is_train", data)
            self.assertGreater(data["x"].shape[0], 0)
            self.assertGreater(data["edge_index"].shape[1], 0)
            num_batches += 1
            batch_sizes.append(int(data["is_seed"].sum()))
        self.assertEqual(num_batches, 11)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 1024)
        self.assertLessEqual(batch_sizes[-1], 1024)

    def test_iterate_spektral(self):
        loader = EdgeNeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            e_extra_feats=["is_train"],
            batch_size=1024,
            num_neighbors=10,
            num_hops=2,
            shuffle=False,
            filter_by=None,
            output_format="spektral",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            # self.assertIsInstance(data, spData)
            self.assertIn("x", data)
            self.assertIn("is_seed", data)
            self.assertIn("is_train", data)
            self.assertGreater(data["x"].shape[0], 0)
            self.assertGreater(data["A"].shape[1], 0)
            num_batches += 1
        self.assertEqual(num_batches, 11)


class TestGDSHeteroEdgeNeighborLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="hetero")

    def test_init(self):
        loader = EdgeNeighborLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x", "y"], "v2": ["x"]},
            e_extra_feats={"v2v0":["is_train"], "v0v0":[], "v2v2":[]},
            e_seed_types=["v2v0"],
            batch_size=100,
            num_neighbors=5,
            num_hops=2,
            shuffle=True,
            filter_by=None,
            output_format="PyG",
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_iterate_pyg(self):
        loader = EdgeNeighborLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x", "y"], "v2": ["x"]},
            e_extra_feats={"v2v0":["is_train"], "v0v0":[], "v2v2":[]},
            e_seed_types=["v2v0"],
            batch_size=100,
            num_neighbors=5,
            num_hops=2,
            shuffle=True,
            filter_by=None,
            output_format="PyG",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygHeteroData)
            self.assertGreater(data["v0"]["x"].shape[0], 0)
            self.assertGreater(data["v2"]["x"].shape[0], 0)
            self.assertTrue(
                data['v2', 'v2v0', 'v0']["edge_index"].shape[1] > 0
                and data['v2', 'v2v0', 'v0']["edge_index"].shape[1] <= 943
            )
            self.assertEqual(
                data['v2', 'v2v0', 'v0']["edge_index"].shape[1],
                data['v2', 'v2v0', 'v0']["is_train"].shape[0]
            )
            if ('v0', 'v0v0', 'v0') in data.edge_types:
                self.assertTrue(
                    data['v0', 'v0v0', 'v0']["edge_index"].shape[1] > 0
                    and data['v0', 'v0v0', 'v0']["edge_index"].shape[1] <= 710
            )
            if ('v2', 'v2v2', 'v2') in data.edge_types:
                self.assertTrue(
                    data['v2', 'v2v2', 'v2']["edge_index"].shape[1] > 0
                    and data['v2', 'v2v2', 'v2']["edge_index"].shape[1] <= 966
                )
            num_batches += 1
            batch_sizes.append(int(data['v2', 'v2v0', 'v0']["is_seed"].sum()))
        self.assertEqual(num_batches, 10)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 100)
        self.assertLessEqual(batch_sizes[-1], 100)


class TestGDSHeteroEdgeNeighborLoaderKafka(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="hetero")

    def test_init(self):
        loader = EdgeNeighborLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x", "y"], "v2": ["x"]},
            e_extra_feats={"v2v0":["is_train"], "v0v0":[], "v2v2":[]},
            e_seed_types=["v2v0"],
            batch_size=100,
            num_neighbors=5,
            num_hops=2,
            shuffle=True,
            filter_by=None,
            output_format="PyG",
            kafka_address="kafka:9092"
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_iterate_pyg(self):
        loader = EdgeNeighborLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x", "y"], "v2": ["x"]},
            e_extra_feats={"v2v0":["is_train"], "v0v0":[], "v2v2":[]},
            e_seed_types=["v2v0"],
            batch_size=100,
            num_neighbors=5,
            num_hops=2,
            shuffle=True,
            filter_by=None,
            output_format="PyG",
            kafka_address="kafka:9092"
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygHeteroData)
            self.assertGreater(data["v0"]["x"].shape[0], 0)
            self.assertGreater(data["v2"]["x"].shape[0], 0)
            self.assertTrue(
                data['v2', 'v2v0', 'v0']["edge_index"].shape[1] > 0
                and data['v2', 'v2v0', 'v0']["edge_index"].shape[1] <= 943
            )
            self.assertEqual(
                data['v2', 'v2v0', 'v0']["edge_index"].shape[1],
                data['v2', 'v2v0', 'v0']["is_train"].shape[0]
            )
            if ('v0', 'v0v0', 'v0') in data.edge_types:
                self.assertTrue(
                    data['v0', 'v0v0', 'v0']["edge_index"].shape[1] > 0
                    and data['v0', 'v0v0', 'v0']["edge_index"].shape[1] <= 710
            )
            if ('v2', 'v2v2', 'v2') in data.edge_types:
                self.assertTrue(
                    data['v2', 'v2v2', 'v2']["edge_index"].shape[1] > 0
                    and data['v2', 'v2v2', 'v2']["edge_index"].shape[1] <= 966
                )
            num_batches += 1
            batch_sizes.append(int(data['v2', 'v2v0', 'v0']["is_seed"].sum()))
        self.assertEqual(num_batches, 10)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 100)
        self.assertLessEqual(batch_sizes[-1], 100)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSEdgeNeighborLoaderKafka("test_init"))
    suite.addTest(TestGDSEdgeNeighborLoaderKafka("test_iterate_pyg"))
    # suite.addTest(TestGDSEdgeNeighborLoaderKafka("test_sasl_ssl"))
    suite.addTest(TestGDSEdgeNeighborLoaderREST("test_init"))
    suite.addTest(TestGDSEdgeNeighborLoaderREST("test_iterate_pyg"))
    # suite.addTest(TestGDSEdgeNeighborLoaderREST("test_iterate_spektral"))
    suite.addTest(TestGDSHeteroEdgeNeighborLoaderREST("test_init"))
    suite.addTest(TestGDSHeteroEdgeNeighborLoaderREST("test_iterate_pyg"))
    suite.addTest(TestGDSHeteroEdgeNeighborLoaderKafka("test_init"))
    suite.addTest(TestGDSHeteroEdgeNeighborLoaderKafka("test_iterate_pyg"))
    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
