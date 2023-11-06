import unittest

from pyTigerGraphUnitTest import make_connection
from torch_geometric.data import Data as pygData
from torch_geometric.data import HeteroData as pygHeteroData

from pyTigerGraph.gds.dataloaders import HGTLoader
from pyTigerGraph.gds.utilities import is_query_installed


class TestGDSHGTLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="hetero")

    def test_init(self):
        loader = HGTLoader(
            graph=self.conn,
            num_neighbors={"v0": 3, "v1": 5, "v2": 10},
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=16,
            num_hops=2,
            shuffle=True,
            output_format="PyG",
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_whole_graph_df(self):
        loader = HGTLoader(
            graph=self.conn,
            num_neighbors={"v0": 3, "v1": 5, "v2": 10},
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            num_batches=1,
            num_hops=2,
            shuffle=False,
            output_format="dataframe",
        )
        data = loader.data
        self.assertTupleEqual(data[0]["v0"].shape, (152, 7))
        self.assertTupleEqual(data[0]["v1"].shape, (220, 3))
        self.assertTupleEqual(data[0]["v2"].shape, (200, 3))
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
        loader = HGTLoader(
            graph=self.conn,
            num_neighbors={"v0": 3, "v1": 5, "v2": 10},
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            num_batches=1,
            num_hops=2,
            shuffle=False,
            output_format="PyG",
        )
        data = loader.data
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
        loader = HGTLoader(
            graph=self.conn,
            num_neighbors={"v0": 2, "v1": 2, "v2": 2},
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            v_seed_types=["v2"],
            batch_size=16,
            num_hops=2,
            shuffle=False,
            output_format="PyG",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygHeteroData)
            self.assertGreater(data["v2"]["x"].shape[0], 0)
            self.assertEqual(data["v2"]["x"].shape[0], data["v2"]["is_seed"].shape[0])
            batch_sizes.append(int(data["v2"]["is_seed"].sum()))
            self.assertGreater(data["v1"]["x"].shape[0], 0)
            self.assertEqual(data["v1"]["x"].shape[0], data["v1"]["is_seed"].shape[0])
            self.assertGreater(data["v0"]["x"].shape[0], 0)
            self.assertEqual(data["v0"]["x"].shape[0], data["v0"]["y"].shape[0])
            self.assertEqual(
                data["v0"]["x"].shape[0], data["v0"]["train_mask"].shape[0]
            )
            self.assertEqual(data["v0"]["x"].shape[0], data["v0"]["test_mask"].shape[0])
            self.assertEqual(data["v0"]["x"].shape[0], data["v0"]["is_seed"].shape[0])
            self.assertEqual(data["v0"]["x"].shape[0], data["v0"]["val_mask"].shape[0])
            self.assertTrue(
                data['v2', 'v2v0', 'v0']["edge_index"].shape[1] > 0
                and data['v2', 'v2v0', 'v0']["edge_index"].shape[1] <= 943
            )
            self.assertTrue(
                data['v2', 'v2v1', 'v1']["edge_index"].shape[1] > 0
                and data['v2', 'v2v1', 'v1']["edge_index"].shape[1] <= 959
            )
            self.assertTrue(
                data['v2', 'v2v2', 'v2']["edge_index"].shape[1] > 0
                and data['v2', 'v2v2', 'v2']["edge_index"].shape[1] <= 966
            )
            self.assertTrue(
                data['v0', 'v0v0', 'v0']["edge_index"].shape[1] > 0
                and data['v0', 'v0v0', 'v0']["edge_index"].shape[1] <= 710
            )
            self.assertTrue(
                data['v1', 'v1v1', 'v1']["edge_index"].shape[1] > 0
                and data['v1', 'v1v1', 'v1']["edge_index"].shape[1] <= 1044
            )
            self.assertTrue(
                data['v1', 'v1v2', 'v2']["edge_index"].shape[1] > 0
                and data['v1', 'v1v2', 'v2']["edge_index"].shape[1] <= 1038
            )
            num_batches += 1
        self.assertEqual(num_batches, 7)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 16)
        self.assertLessEqual(batch_sizes[-1], 16)

    def test_fetch(self):
        loader = HGTLoader(
            graph=self.conn,
            num_neighbors={"v0": 2, "v1": 2, "v2": 2},
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=16,
            num_hops=2,
            shuffle=False,
            output_format="PyG",
        )
        data = loader.fetch(
            [{"primary_id": "10", "type": "v0"}, {"primary_id": "55", "type": "v0"}]
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


class TestGDSHGTLoaderKafka(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="hetero")

    def test_init(self):
        loader = HGTLoader(
            graph=self.conn,
            num_neighbors={"v0": 3, "v1": 5, "v2": 10},
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            batch_size=16,
            num_hops=2,
            shuffle=True,
            output_format="PyG",
            kafka_address="kafka:9092"
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_whole_graph_df(self):
        loader = HGTLoader(
            graph=self.conn,
            num_neighbors={"v0": 3, "v1": 5, "v2": 10},
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            num_batches=1,
            num_hops=2,
            shuffle=False,
            output_format="dataframe",
            kafka_address="kafka:9092"
        )
        data = loader.data
        self.assertTupleEqual(data[0]["v0"].shape, (152, 7))
        self.assertTupleEqual(data[0]["v1"].shape, (220, 3))
        self.assertTupleEqual(data[0]["v2"].shape, (200, 3))
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
        loader = HGTLoader(
            graph=self.conn,
            num_neighbors={"v0": 3, "v1": 5, "v2": 10},
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            num_batches=1,
            num_hops=2,
            shuffle=False,
            output_format="PyG",
            kafka_address="kafka:9092"
        )
        data = loader.data
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
        loader = HGTLoader(
            graph=self.conn,
            num_neighbors={"v0": 2, "v1": 2, "v2": 2},
            v_in_feats={"v0": ["x"], "v1": ["x"], "v2": ["x"]},
            v_out_labels={"v0": ["y"]},
            v_extra_feats={"v0": ["train_mask", "val_mask", "test_mask"]},
            v_seed_types=["v2"],
            batch_size=16,
            num_hops=2,
            shuffle=False,
            output_format="PyG",
            kafka_address="kafka:9092"
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygHeteroData)
            self.assertGreater(data["v2"]["x"].shape[0], 0)
            self.assertEqual(data["v2"]["x"].shape[0], data["v2"]["is_seed"].shape[0])
            batch_sizes.append(int(data["v2"]["is_seed"].sum()))
            self.assertGreater(data["v1"]["x"].shape[0], 0)
            self.assertEqual(data["v1"]["x"].shape[0], data["v1"]["is_seed"].shape[0])
            self.assertGreater(data["v0"]["x"].shape[0], 0)
            self.assertEqual(data["v0"]["x"].shape[0], data["v0"]["y"].shape[0])
            self.assertEqual(
                data["v0"]["x"].shape[0], data["v0"]["train_mask"].shape[0]
            )
            self.assertEqual(data["v0"]["x"].shape[0], data["v0"]["test_mask"].shape[0])
            self.assertEqual(data["v0"]["x"].shape[0], data["v0"]["is_seed"].shape[0])
            self.assertEqual(data["v0"]["x"].shape[0], data["v0"]["val_mask"].shape[0])
            self.assertTrue(
                data['v2', 'v2v0', 'v0']["edge_index"].shape[1] > 0
                and data['v2', 'v2v0', 'v0']["edge_index"].shape[1] <= 943
            )
            self.assertTrue(
                data['v2', 'v2v1', 'v1']["edge_index"].shape[1] > 0
                and data['v2', 'v2v1', 'v1']["edge_index"].shape[1] <= 959
            )
            self.assertTrue(
                data['v2', 'v2v2', 'v2']["edge_index"].shape[1] > 0
                and data['v2', 'v2v2', 'v2']["edge_index"].shape[1] <= 966
            )
            self.assertTrue(
                data['v0', 'v0v0', 'v0']["edge_index"].shape[1] > 0
                and data['v0', 'v0v0', 'v0']["edge_index"].shape[1] <= 710
            )
            self.assertTrue(
                data['v1', 'v1v1', 'v1']["edge_index"].shape[1] > 0
                and data['v1', 'v1v1', 'v1']["edge_index"].shape[1] <= 1044
            )
            self.assertTrue(
                data['v1', 'v1v2', 'v2']["edge_index"].shape[1] > 0
                and data['v1', 'v1v2', 'v2']["edge_index"].shape[1] <= 1038
            )
            num_batches += 1
        self.assertEqual(num_batches, 7)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 16)
        self.assertLessEqual(batch_sizes[-1], 16)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSHGTLoaderREST("test_init"))
    suite.addTest(TestGDSHGTLoaderREST("test_whole_graph_df"))
    suite.addTest(TestGDSHGTLoaderREST("test_whole_graph_pyg"))
    suite.addTest(TestGDSHGTLoaderREST("test_iterate_pyg"))
    suite.addTest(TestGDSHGTLoaderREST("test_fetch"))
    suite.addTest(TestGDSHGTLoaderKafka("test_init"))
    suite.addTest(TestGDSHGTLoaderKafka("test_whole_graph_df"))
    suite.addTest(TestGDSHGTLoaderKafka("test_whole_graph_pyg"))
    suite.addTest(TestGDSHGTLoaderKafka("test_iterate_pyg"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
