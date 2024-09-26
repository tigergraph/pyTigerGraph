import unittest
from io import StringIO
from os.path import exists
from textwrap import dedent
from unittest.mock import patch

from pyTigerGraph.pytgasync.datasets import AsyncDatasets


class TestDatasetsAsync(unittest.IsolatedAsyncioTestCase):
    async def test_get_dataset_url(self):
        dataset = await AsyncDatasets.create()

        dataset.name = "Cora"
        self.assertEqual(
            await dataset.get_dataset_url(),
            "https://tigergraph-public-data.s3.us-west-1.amazonaws.com/Cora.tar.gz",
        )

        dataset.name = "SomethingNotThere"
        self.assertIsNone(await dataset.get_dataset_url())

    async def test_download_extract(self):
        dataset = await AsyncDatasets.create()
        dataset.name = "Cora"
        dataset.dataset_url = await dataset.get_dataset_url()
        await dataset.download_extract()
        self.assertTrue(exists("./tmp/Cora/create_graph.gsql"))
        self.assertTrue(exists("./tmp/Cora/create_load_job.gsql"))
        self.assertTrue(exists("./tmp/Cora/create_schema.gsql"))
        self.assertTrue(exists("./tmp/Cora/run_load_job.json"))
        self.assertTrue(exists("./tmp/Cora/edges.csv"))
        self.assertTrue(exists("./tmp/Cora/nodes.csv"))

    async def test_clean_up(self):
        dataset = await AsyncDatasets.create()
        dataset.name = "Cora"
        dataset.clean_up()
        self.assertFalse(exists("./tmp/Cora"))

    @patch("sys.stdout", new_callable=StringIO)
    async def test_list(self, mock_stdout):
        dataset = await AsyncDatasets.create()
        truth = """\
            Available datasets:
            - Cora
            - CoraV2
            - Ethereum
            - ldbc_snb
            - LastFM
            - imdb
            - movie
            - social
            """
        self.assertIn(dedent(truth), mock_stdout.getvalue())


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestDatasetsAsync("test_get_dataset_url"))
    suite.addTest(TestDatasetsAsync("test_download_extract"))
    suite.addTest(TestDatasetsAsync("test_clean_up"))
    suite.addTest(TestDatasetsAsync("test_list"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
