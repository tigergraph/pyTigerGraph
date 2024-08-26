import unittest
from multiprocessing import Pool

from pyTigerGraphUnitTest import make_connection
from torch_geometric.data import Data as pygData
from torch_geometric.data import HeteroData as pygHeteroData

from pyTigerGraph.gds.dataloaders import NeighborLoader
from pyTigerGraph.gds.utilities import is_query_installed


class TestGDSNeighborLoaderKafka(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def test_init(self):
        loader = NeighborLoader(
            graph=self.conn,
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
            kafka_address="kafka:9092",
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 9)

    def test_iterate_pyg(self):
        loader = NeighborLoader(
            graph=self.conn,
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
            kafka_address="kafka:9092",
        )
        for epoch in range(2):
            with self.subTest(i=epoch):
                num_batches = 0
                for data in loader:
                    # print(num_batches, data)
                    self.assertIsInstance(data, pygData)
                    self.assertIn("x", data)
                    self.assertIn("y", data)
                    self.assertIn("train_mask", data)
                    self.assertIn("val_mask", data)
                    self.assertIn("test_mask", data)
                    self.assertIn("is_seed", data)
                    self.assertGreater(data["x"].shape[0], 0)
                    self.assertGreater(data["edge_index"].shape[1], 0)
                    num_batches += 1
                self.assertEqual(num_batches, 9)

    def test_iterate_stop_pyg(self):
        loader = NeighborLoader(
            graph=self.conn,
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
            kafka_address="kafka:9092",
        )
        for epoch in range(2):
            num_batches = 0
            broken = False
            for data in loader:
                # print(num_batches, data)
                self.assertIsInstance(data, pygData)
                self.assertIn("x", data)
                self.assertIn("y", data)
                self.assertIn("train_mask", data)
                self.assertIn("val_mask", data)
                self.assertIn("test_mask", data)
                self.assertIn("is_seed", data)
                self.assertGreater(data["x"].shape[0], 0)
                self.assertGreater(data["edge_index"].shape[1], 0)
                num_batches += 1
                if num_batches == 5:
                    loader.stop(remove_topics=True)
                    broken = True
                    break
            if broken:
                break
        rq_id = self.conn.getRunningQueries()["results"]
        self.assertEqual(len(rq_id), 0)

    def test_whole_graph_pyg(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            num_batches=1,
            num_neighbors=10,
            num_hops=2,
            shuffle=False,
            filter_by="train_mask",
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
            kafka_address="kafka:9092",
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, pygData)
        self.assertIn("x", data)
        self.assertIn("y", data)
        self.assertIn("train_mask", data)
        self.assertIn("val_mask", data)
        self.assertIn("test_mask", data)
        self.assertIn("is_seed", data)

    def test_edge_attr(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            e_in_feats=["time"],
            e_extra_feats=["is_train"],
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            filter_by="train_mask",
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
            kafka_address="kafka:9092",
        )
        for epoch in range(2):
            with self.subTest(i=epoch):
                num_batches = 0
                for data in loader:
                    # print(num_batches, data)
                    self.assertIsInstance(data, pygData)
                    self.assertIn("x", data)
                    self.assertIn("y", data)
                    self.assertIn("train_mask", data)
                    self.assertIn("val_mask", data)
                    self.assertIn("test_mask", data)
                    self.assertIn("is_seed", data)
                    self.assertIn("edge_feat", data)
                    self.assertIn("is_train", data)
                    num_batches += 1
                self.assertEqual(num_batches, 9)

    def test_sasl_plaintext(self):
        loader = NeighborLoader(
            graph=self.conn,
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
            kafka_address="34.127.11.236:9092",
            kafka_security_protocol="SASL_PLAINTEXT",
            kafka_sasl_mechanism="PLAIN",
            kafka_sasl_plain_username="bill",
            kafka_sasl_plain_password="bill",
        )
        for epoch in range(2):
            with self.subTest(i=epoch):
                num_batches = 0
                for data in loader:
                    # print(num_batches, data)
                    self.assertIsInstance(data, pygData)
                    self.assertIn("x", data)
                    self.assertIn("y", data)
                    self.assertIn("train_mask", data)
                    self.assertIn("val_mask", data)
                    self.assertIn("test_mask", data)
                    self.assertIn("is_seed", data)
                    self.assertGreater(data["x"].shape[0], 0)
                    self.assertGreater(data["edge_index"].shape[1], 0)
                    num_batches += 1
                self.assertEqual(num_batches, 9)

    def test_sasl_ssl(self):
        loader = NeighborLoader(
            graph=self.conn,
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
            kafka_address="pkc-6ojv2.us-west4.gcp.confluent.cloud:9092",
            kafka_replica_factor=3,
            kafka_max_msg_size=8388608,
            kafka_security_protocol="SASL_SSL",
            kafka_sasl_mechanism="PLAIN",
            kafka_sasl_plain_username="YIQM66T3BZZLSXBJ",
            kafka_sasl_plain_password="UgRdpSS34e2kYe8jZ9m7py4LgjkjxsGrePiaaMv/YCHRIjRmTJMpodS/0og8SYe8",
            kafka_producer_ca_location="/home/tigergraph/mlworkbench/ssl/cert.pem",
        )
        for epoch in range(2):
            with self.subTest(i=epoch):
                num_batches = 0
                for data in loader:
                    # print(num_batches, data)
                    self.assertIsInstance(data, pygData)
                    self.assertIn("x", data)
                    self.assertIn("y", data)
                    self.assertIn("train_mask", data)
                    self.assertIn("val_mask", data)
                    self.assertIn("test_mask", data)
                    self.assertIn("is_seed", data)
                    self.assertGreater(data["x"].shape[0], 0)
                    self.assertGreater(data["edge_index"].shape[1], 0)
                    num_batches += 1
                self.assertEqual(num_batches, 9)

    @staticmethod
    def run_loader(params: dict) -> int:
        neighbor_loader = NeighborLoader(
            graph=params["conn"],
            batch_size=8,
            num_neighbors=10,
            num_hops=2,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            output_format="PyG",
            shuffle=False,
            filter_by="train_mask",
            timeout=300000,
            kafka_address="kafka:9092",
            kafka_num_partitions=2,
            kafka_auto_offset_reset="earliest",
            kafka_group_id="test_group",
            kafka_auto_del_topic=False,
            kafka_skip_produce=params["kafka_skip_produce"]
        )
        num_batches = 0
        num_seeds = 0
        for data in neighbor_loader:
            # print(num_batches, data, data["is_seed"].sum().item(), flush=True)
            num_batches += 1
            num_seeds += data["is_seed"].sum().item()
        return num_batches, num_seeds

    def test_distributed_loaders(self):
        params = [
            {"conn": self.conn, "kafka_skip_produce": False},
            {"conn": self.conn, "kafka_skip_produce": True}
        ]
        with Pool(processes=2) as pool:
            res = pool.map(self.run_loader, params)
        self.assertEqual(res[0][0]+res[1][0], 18)
        self.assertEqual(res[0][1]+res[1][1], 140)


class TestGDSNeighborLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def test_init(self):
        loader = NeighborLoader(
            graph=self.conn,
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

    def test_iterate_pyg(self):
        loader = NeighborLoader(
            graph=self.conn,
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
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygData)
            self.assertIn("x", data)
            self.assertIn("y", data)
            self.assertIn("train_mask", data)
            self.assertIn("val_mask", data)
            self.assertIn("test_mask", data)
            self.assertIn("is_seed", data)
            self.assertGreater(data["x"].shape[0], 0)
            self.assertGreater(data["edge_index"].shape[1], 0)
            num_batches += 1
        self.assertEqual(num_batches, 9)

    def test_whole_graph_pyg(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            num_batches=1,
            num_neighbors=10,
            num_hops=2,
            shuffle=False,
            filter_by="train_mask",
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, pygData)
        self.assertIn("x", data)
        self.assertIn("y", data)
        self.assertIn("train_mask", data)
        self.assertIn("val_mask", data)
        self.assertIn("test_mask", data)
        self.assertIn("is_seed", data)

    def test_edge_attr(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            e_in_feats=["time"],
            e_extra_feats=["is_train"],
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
        for epoch in range(2):
            with self.subTest(i=epoch):
                num_batches = 0
                for data in loader:
                    # print(num_batches, data)
                    self.assertIsInstance(data, pygData)
                    self.assertIn("x", data)
                    self.assertIn("y", data)
                    self.assertIn("train_mask", data)
                    self.assertIn("val_mask", data)
                    self.assertIn("test_mask", data)
                    self.assertIn("is_seed", data)
                    self.assertIn("edge_feat", data)
                    self.assertIn("is_train", data)
                    num_batches += 1
                self.assertEqual(num_batches, 9)

    def test_fetch(self):
        loader = NeighborLoader(
            graph=self.conn,
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
        data = loader.fetch(
            [
                {"primary_id": "100", "type": "Paper"},
                {"primary_id": "55", "type": "Paper"},
            ]
        )
        self.assertIn("primary_id", data)
        self.assertGreater(data["x"].shape[0], 2)
        self.assertGreater(data["edge_index"].shape[1], 0)
        self.assertIn("100", data["primary_id"])
        self.assertIn("55", data["primary_id"])
        for i, d in enumerate(data["primary_id"]):
            if d == "100" or d == "55":
                self.assertTrue(data["is_seed"][i].item())
            else:
                self.assertFalse(data["is_seed"][i].item())

    def test_fetch_delimiter(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            delimiter="$|",
            filter_by="train_mask",
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        data = loader.fetch(
            [
                {"primary_id": "100", "type": "Paper"},
                {"primary_id": "55", "type": "Paper"},
            ]
        )
        self.assertIn("primary_id", data)
        self.assertGreater(data["x"].shape[0], 2)
        self.assertGreater(data["edge_index"].shape[1], 0)
        self.assertIn("100", data["primary_id"])
        self.assertIn("55", data["primary_id"])
        for i, d in enumerate(data["primary_id"]):
            if d == "100" or d == "55":
                self.assertTrue(data["is_seed"][i].item())
            else:
                self.assertFalse(data["is_seed"][i].item())

    def test_iterate_spektral(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            filter_by="train_mask",
            output_format="spektral",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4
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
            self.assertIn("is_seed", data)
            num_batches += 1
        self.assertEqual(num_batches, 9)

    def test_whole_graph_spektral(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            num_batches=1,
            num_neighbors=10,
            num_hops=2,
            shuffle=False,
            filter_by="train_mask",
            output_format="spektral",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        data = loader.data
        # print(data)
        # self.assertIsInstance(data, spData)
        self.assertIn("x", data)
        self.assertIn("y", data)
        self.assertIn("train_mask", data)
        self.assertIn("val_mask", data)
        self.assertIn("test_mask", data)
        self.assertIn("is_seed", data)

    def test_reinstall_query(self):
        loader = NeighborLoader(
            graph=self.conn,
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
        query_name = loader.query_name
        self.assertEqual(loader.reinstall_query(), query_name)


class TestGDSHeteroNeighborLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="hetero")

    def test_init(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertEqual(loader.num_batches, 18)

    def test_whole_graph_df(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            num_batches=1,
            num_neighbors=10,
            num_hops=2,
            shuffle=False,
            output_format="dataframe",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        data = loader.data
        self.assertTupleEqual(data[0]["v0"].shape, (76, 7))
        self.assertTupleEqual(data[0]["v1"].shape, (110, 3))
        self.assertTupleEqual(data[0]["v2"].shape, (100, 3))
        self.assertTrue(
            data[1]["v0v0"].shape[0] > 0 and data[1]["v0v0"].shape[0] <= 710
        )
        self.assertTrue(
            data[1]["v1v1"].shape[0] > 0 and data[1]["v1v1"].shape[0] <= 1044
        )
        self.assertTrue(
            data[1]["v1v2"].shape[0] > 0 and data[1]["v1v2"].shape[0] <= 1038
        )
        self.assertTrue(
            data[1]["v2v0"].shape[0] > 0 and data[1]["v2v0"].shape[0] <= 943
        )
        self.assertTrue(
            data[1]["v2v1"].shape[0] > 0 and data[1]["v2v1"].shape[0] <= 959
        )
        self.assertTrue(
            data[1]["v2v2"].shape[0] > 0 and data[1]["v2v2"].shape[0] <= 966
        )

    def test_whole_graph_pyg(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            num_batches=1,
            num_neighbors=10,
            num_hops=2,
            shuffle=False,
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        data = loader.data
        # print(data)
        self.assertTupleEqual(data["v0"]["x"].shape, (76, 77))
        self.assertEqual(data["v0"]["y"].shape[0], 76)
        self.assertEqual(data["v0"]["train_mask"].shape[0], 76)
        self.assertEqual(data["v0"]["test_mask"].shape[0], 76)
        self.assertEqual(data["v0"]["val_mask"].shape[0], 76)
        self.assertEqual(data["v0"]["is_seed"].shape[0], 76)
        self.assertTupleEqual(data["v1"]["x"].shape, (110, 57))
        self.assertEqual(data["v1"]["is_seed"].shape[0], 110)
        self.assertTupleEqual(data["v2"]["x"].shape, (100, 48))
        self.assertEqual(data["v2"]["is_seed"].shape[0], 100)
        self.assertTrue(
            data["v0v0"]["edge_index"].shape[1] > 0
            and data["v0v0"]["edge_index"].shape[1] <= 710
        )
        self.assertTrue(
            data["v1v1"]["edge_index"].shape[1] > 0
            and data["v1v1"]["edge_index"].shape[1] <= 1044
        )
        self.assertTrue(
            data["v1v2"]["edge_index"].shape[1] > 0
            and data["v1v2"]["edge_index"].shape[1] <= 1038
        )
        self.assertTrue(
            data["v2v0"]["edge_index"].shape[1] > 0
            and data["v2v0"]["edge_index"].shape[1] <= 943
        )
        self.assertTrue(
            data["v2v1"]["edge_index"].shape[1] > 0
            and data["v2v1"]["edge_index"].shape[1] <= 959
        )
        self.assertTrue(
            data["v2v2"]["edge_index"].shape[1] > 0
            and data["v2v2"]["edge_index"].shape[1] <= 966
        )

    def test_iterate_pyg(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=False,
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygHeteroData)
            self.assertGreater(data["v0"]["x"].shape[0], 0)
            self.assertEqual(data["v0"]["x"].shape[0],
                             data["v0"]["y"].shape[0])
            self.assertEqual(
                data["v0"]["x"].shape[0], data["v0"]["train_mask"].shape[0]
            )
            self.assertEqual(data["v0"]["x"].shape[0],
                             data["v0"]["test_mask"].shape[0])
            self.assertEqual(data["v0"]["x"].shape[0],
                             data["v0"]["is_seed"].shape[0])
            self.assertEqual(data["v0"]["x"].shape[0],
                             data["v0"]["val_mask"].shape[0])
            self.assertGreater(data["v1"]["x"].shape[0], 0)
            self.assertEqual(data["v1"]["x"].shape[0],
                             data["v1"]["is_seed"].shape[0])
            self.assertGreater(data["v2"]["x"].shape[0], 0)
            self.assertEqual(data["v2"]["x"].shape[0],
                             data["v2"]["is_seed"].shape[0])
            self.assertTrue(
                data["v0v0"]["edge_index"].shape[1] > 0
                and data["v0v0"]["edge_index"].shape[1] <= 710
            )
            self.assertTrue(
                data["v1v1"]["edge_index"].shape[1] > 0
                and data["v1v1"]["edge_index"].shape[1] <= 1044
            )
            self.assertTrue(
                data["v1v2"]["edge_index"].shape[1] > 0
                and data["v1v2"]["edge_index"].shape[1] <= 1038
            )
            self.assertTrue(
                data["v2v0"]["edge_index"].shape[1] > 0
                and data["v2v0"]["edge_index"].shape[1] <= 943
            )
            self.assertTrue(
                data["v2v1"]["edge_index"].shape[1] > 0
                and data["v2v1"]["edge_index"].shape[1] <= 959
            )
            self.assertTrue(
                data["v2v2"]["edge_index"].shape[1] > 0
                and data["v2v2"]["edge_index"].shape[1] <= 966
            )
            num_batches += 1
        self.assertEqual(num_batches, 18)

    def test_iterate_pyg_multichar_delimiter(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=False,
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
            delimiter="|$"
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygHeteroData)
            self.assertGreater(data["v0"]["x"].shape[0], 0)
            self.assertEqual(data["v0"]["x"].shape[0],
                             data["v0"]["y"].shape[0])
            self.assertEqual(
                data["v0"]["x"].shape[0], data["v0"]["train_mask"].shape[0]
            )
            self.assertEqual(data["v0"]["x"].shape[0],
                             data["v0"]["test_mask"].shape[0])
            self.assertEqual(data["v0"]["x"].shape[0],
                             data["v0"]["is_seed"].shape[0])
            self.assertEqual(data["v0"]["x"].shape[0],
                             data["v0"]["val_mask"].shape[0])
            self.assertGreater(data["v1"]["x"].shape[0], 0)
            self.assertEqual(data["v1"]["x"].shape[0],
                             data["v1"]["is_seed"].shape[0])
            self.assertGreater(data["v2"]["x"].shape[0], 0)
            self.assertEqual(data["v2"]["x"].shape[0],
                             data["v2"]["is_seed"].shape[0])
            self.assertTrue(
                data["v0v0"]["edge_index"].shape[1] > 0
                and data["v0v0"]["edge_index"].shape[1] <= 710
            )
            self.assertTrue(
                data["v1v1"]["edge_index"].shape[1] > 0
                and data["v1v1"]["edge_index"].shape[1] <= 1044
            )
            self.assertTrue(
                data["v1v2"]["edge_index"].shape[1] > 0
                and data["v1v2"]["edge_index"].shape[1] <= 1038
            )
            self.assertTrue(
                data["v2v0"]["edge_index"].shape[1] > 0
                and data["v2v0"]["edge_index"].shape[1] <= 943
            )
            self.assertTrue(
                data["v2v1"]["edge_index"].shape[1] > 0
                and data["v2v1"]["edge_index"].shape[1] <= 959
            )
            self.assertTrue(
                data["v2v2"]["edge_index"].shape[1] > 0
                and data["v2v2"]["edge_index"].shape[1] <= 966
            )
            num_batches += 1
        self.assertEqual(num_batches, 18)

    def test_fetch(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=False,
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        data = loader.fetch(
            [{"primary_id": "10", "type": "v0"}, {
                "primary_id": "55", "type": "v0"}]
        )
        self.assertIn("primary_id", data["v0"])
        self.assertGreater(data["v0"]["x"].shape[0], 2)
        self.assertGreater(data["v0v0"]["edge_index"].shape[1], 0)
        self.assertIn("10", data["v0"]["primary_id"])
        self.assertIn("55", data["v0"]["primary_id"])
        for i, d in enumerate(data["v0"]["primary_id"]):
            if d == "10" or d == "55":
                self.assertTrue(data["v0"]["is_seed"][i].item())
            else:
                self.assertFalse(data["v0"]["is_seed"][i].item())

    def test_fetch_delimiter(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=False,
            output_format="PyG",
            delimiter="$|",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        data = loader.fetch(
            [{"primary_id": "10", "type": "v0"}, {
                "primary_id": "55", "type": "v0"}]
        )
        self.assertIn("primary_id", data["v0"])
        self.assertGreater(data["v0"]["x"].shape[0], 2)
        self.assertGreater(data["v0v0"]["edge_index"].shape[1], 0)
        self.assertIn("10", data["v0"]["primary_id"])
        self.assertIn("55", data["v0"]["primary_id"])
        for i, d in enumerate(data["v0"]["primary_id"]):
            if d == "10" or d == "55":
                self.assertTrue(data["v0"]["is_seed"][i].item())
            else:
                self.assertFalse(data["v0"]["is_seed"][i].item())

    def test_metadata(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=False,
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )

        test = (["v0", "v1", "v2"],
                [("v0", "v0v0", "v0"),
                 ("v1", "v1v1", "v1"),
                 ("v1", "v1v2", "v2"),
                 ("v2", "v2v0", "v0"),
                 ("v2", "v2v1", "v1"),
                 ("v2", "v2v2", "v2")])

        metadata = loader.metadata()
        self.assertEqual(test, metadata)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSNeighborLoaderKafka("test_init"))
    suite.addTest(TestGDSNeighborLoaderKafka("test_iterate_pyg"))
    suite.addTest(TestGDSNeighborLoaderKafka("test_iterate_stop_pyg"))
    suite.addTest(TestGDSNeighborLoaderKafka("test_whole_graph_pyg"))
    suite.addTest(TestGDSNeighborLoaderKafka("test_edge_attr"))
    suite.addTest(TestGDSNeighborLoaderKafka("test_distributed_loaders"))
    # suite.addTest(TestGDSNeighborLoaderKafka("test_sasl_plaintext"))
    # suite.addTest(TestGDSNeighborLoaderKafka("test_sasl_ssl"))
    suite.addTest(TestGDSNeighborLoaderREST("test_init"))
    suite.addTest(TestGDSNeighborLoaderREST("test_iterate_pyg"))
    suite.addTest(TestGDSNeighborLoaderREST("test_whole_graph_pyg"))
    suite.addTest(TestGDSNeighborLoaderREST("test_edge_attr"))
    suite.addTest(TestGDSNeighborLoaderREST("test_fetch"))
    suite.addTest(TestGDSNeighborLoaderREST("test_fetch_delimiter"))
    suite.addTest(TestGDSNeighborLoaderREST("test_reinstall_query"))
    suite.addTest(TestGDSHeteroNeighborLoaderREST("test_init"))
    suite.addTest(TestGDSHeteroNeighborLoaderREST("test_whole_graph_df"))
    suite.addTest(TestGDSHeteroNeighborLoaderREST("test_whole_graph_pyg"))
    suite.addTest(TestGDSHeteroNeighborLoaderREST("test_iterate_pyg"))
    suite.addTest(TestGDSHeteroNeighborLoaderREST(
        "test_iterate_pyg_multichar_delimiter"))
    suite.addTest(TestGDSHeteroNeighborLoaderREST("test_fetch"))
    suite.addTest(TestGDSHeteroNeighborLoaderREST("test_fetch_delimiter"))
    suite.addTest(TestGDSHeteroNeighborLoaderREST("test_metadata"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
