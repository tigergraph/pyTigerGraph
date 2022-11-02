import unittest

from pyTigerGraph.datasets import Datasets
from os.path import exists

class TestDatasets(unittest.TestCase):
    def test_get_dataset_url(self):
        dataset = Datasets()

        dataset.name = "Cora"
        self.assertEqual(
            dataset.get_dataset_url(),
            "https://tigergraph-public-data.s3.us-west-1.amazonaws.com/Cora.tar.gz",
        )

        dataset.name = "SomethingNotThere"
        self.assertIsNone(dataset.get_dataset_url())

    def test_download_extract(self):
        dataset = Datasets()
        dataset.name = "Cora"
        dataset.dataset_url = dataset.get_dataset_url()
        dataset.download_extract()
        self.assertTrue(exists("./tmp/Cora/create_graph.gsql"))
        self.assertTrue(exists("./tmp/Cora/create_load_job.gsql"))
        self.assertTrue(exists("./tmp/Cora/create_schema.gsql"))
        self.assertTrue(exists("./tmp/Cora/run_load_job.json"))
        self.assertTrue(exists("./tmp/Cora/edges.csv"))
        self.assertTrue(exists("./tmp/Cora/nodes.csv"))

    def test_clean_up(self):
        dataset = Datasets()
        dataset.name = "Cora"
        dataset.clean_up()
        self.assertFalse(exists("./tmp/Cora"))


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestDatasets("test_get_dataset_url"))
    suite.addTest(TestDatasets("test_download_extract"))
    suite.addTest(TestDatasets("test_clean_up"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
