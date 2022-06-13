import unittest

from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.gds.utilities import is_query_installed
from pyTigerGraph.gds.dataloaders import EdgeNeighborLoader
from torch_geometric.data import Data as pygData
from torch_geometric.data import HeteroData as pygHeteroData


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
    suite.addTest(TestGDSEdgeNeighborLoaderREST("test_init"))
    suite.addTest(TestGDSEdgeNeighborLoaderREST("test_iterate_pyg"))
 
    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
