import unittest

from pandas import DataFrame
from pyTigerGraphUnitTest import make_connection

from pyTigerGraph.gds.dataloaders import EdgeLoader
from pyTigerGraph.gds.utilities import is_query_installed


class TestGDSEdgeLoaderKafka(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def test_init(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=False,
            kafka_address="kafka:9092",
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_iterate(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=True,
            kafka_address="kafka:9092",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("source", data)
            self.assertIn("target", data)
            num_batches += 1
            self.assertEqual(data.shape[1], 2)
            batch_sizes.append(data.shape[0])
        self.assertEqual(num_batches, 11)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 1024)
        self.assertLessEqual(batch_sizes[-1], 1024)

    def test_whole_edgelist(self):
        loader = EdgeLoader(
            graph=self.conn,
            num_batches=1,
            shuffle=False,
            kafka_address="kafka:9092",
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)
        self.assertIn("source", data)
        self.assertIn("target", data)
        self.assertEqual(data.shape[0], 10556)
        self.assertEqual(data.shape[1], 2)

    def test_iterate_attr(self):
        loader = EdgeLoader(
            graph=self.conn,
            attributes=["time", "is_train"],
            batch_size=1024,
            shuffle=True,
            kafka_address="kafka:9092",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("time", data)
            self.assertIn("is_train", data)
            num_batches += 1
            self.assertEqual(data.shape[1], 4)
            batch_sizes.append(data.shape[0])
        self.assertEqual(num_batches, 11)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 1024)
        self.assertLessEqual(batch_sizes[-1], 1024)

    def test_iterate_attr_multichar_delimiter(self):
        loader = EdgeLoader(
            graph=self.conn,
            attributes=["time", "is_train"],
            batch_size=1024,
            shuffle=True,
            kafka_address="kafka:9092",
            delimiter="|$"
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("time", data)
            self.assertIn("is_train", data)
            num_batches += 1
            self.assertEqual(data.shape[1], 4)
            batch_sizes.append(data.shape[0])
        self.assertEqual(num_batches, 11)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 1024)
        self.assertLessEqual(batch_sizes[-1], 1024)

    def test_sasl_plaintext(self):
        loader = EdgeLoader(
            graph=self.conn,
            attributes=["time", "is_train"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
            kafka_address="34.127.11.236:9092",
            kafka_security_protocol="SASL_PLAINTEXT",
            kafka_sasl_mechanism="PLAIN",
            kafka_sasl_plain_username="bill",
            kafka_sasl_plain_password="bill",
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
            attributes=["time", "is_train"],
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
            kafka_producer_ca_location="/home/tigergraph/mlworkbench/ssl/cert.pem",
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


class TestGDSHeteroEdgeLoaderKafka(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="hetero")

    def test_init(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=False,
            kafka_address="kafka:9092"
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_iterate_as_homo(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=False,
            kafka_address="kafka:9092"
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("source", data)
            self.assertIn("target", data)
            num_batches += 1
            self.assertEqual(data.shape[1], 2)
            batch_sizes.append(data.shape[0])
        self.assertEqual(num_batches, 6)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 1024)
        self.assertLessEqual(batch_sizes[-1], 1024)

    def test_iterate_hetero(self):
        loader = EdgeLoader(
            graph=self.conn,
            attributes={"v0v0": ["is_train", "is_val"], "v2v0": ["is_train", "is_val"]},
            batch_size=200,
            shuffle=True,
            kafka_address="kafka:9092"
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            batchsize = 0
            if "v0v0" in data:
                self.assertIsInstance(data["v0v0"], DataFrame)
                self.assertIn("is_val", data["v0v0"])
                self.assertIn("is_train", data["v0v0"])
                batchsize += data["v0v0"].shape[0]
                self.assertEqual(data["v0v0"].shape[1], 4)
            if "v2v0" in data:
                self.assertIsInstance(data["v2v0"], DataFrame)
                self.assertIn("is_val", data["v2v0"])
                self.assertIn("is_train", data["v2v0"])
                batchsize += data["v2v0"].shape[0]
                self.assertEqual(data["v2v0"].shape[1], 4)
            self.assertGreater(len(data), 0)
            num_batches += 1
            batch_sizes.append(batchsize)
        self.assertEqual(num_batches, 9)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 200)
        self.assertLessEqual(batch_sizes[-1], 200)

    def test_iterate_hetero_multichar_delimiter(self):
        loader = EdgeLoader(
            graph=self.conn,
            attributes={"v0v0": ["is_train", "is_val"], "v2v0": ["is_train", "is_val"]},
            batch_size=200,
            shuffle=True,
            delimiter="|$",
            kafka_address="kafka:9092"
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            batchsize = 0
            if "v0v0" in data:
                self.assertIsInstance(data["v0v0"], DataFrame)
                self.assertIn("is_val", data["v0v0"])
                self.assertIn("is_train", data["v0v0"])
                batchsize += data["v0v0"].shape[0]
                self.assertEqual(data["v0v0"].shape[1], 4)
            if "v2v0" in data:
                self.assertIsInstance(data["v2v0"], DataFrame)
                self.assertIn("is_val", data["v2v0"])
                self.assertIn("is_train", data["v2v0"])
                batchsize += data["v2v0"].shape[0]
                self.assertEqual(data["v2v0"].shape[1], 4)
            self.assertGreater(len(data), 0)
            num_batches += 1
            batch_sizes.append(batchsize)
        self.assertEqual(num_batches, 9)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 200)
        self.assertLessEqual(batch_sizes[-1], 200)


class TestGDSEdgeLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def test_init(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=False,
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_iterate(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=True,
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("source", data)
            self.assertIn("target", data)
            num_batches += 1
            self.assertEqual(data.shape[1], 2)
            batch_sizes.append(data.shape[0])
        self.assertEqual(num_batches, 11)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 1024)
        self.assertLessEqual(batch_sizes[-1], 1024)

    def test_whole_edgelist(self):
        loader = EdgeLoader(
            graph=self.conn,
            num_batches=1,
            shuffle=False,
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)
        self.assertIn("source", data)
        self.assertIn("target", data)
        self.assertEqual(data.shape[0], 10556)
        self.assertEqual(data.shape[1], 2)
        

    def test_iterate_attr(self):
        loader = EdgeLoader(
            graph=self.conn,
            attributes=["time", "is_train"],
            batch_size=1024,
            shuffle=True,
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("time", data)
            self.assertIn("is_train", data)
            num_batches += 1
            self.assertEqual(data.shape[1], 4)
            batch_sizes.append(data.shape[0])
        self.assertEqual(num_batches, 11)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 1024)
        self.assertLessEqual(batch_sizes[-1], 1024)

    def test_iterate_attr_multichar_delimiter(self):
        loader = EdgeLoader(
            graph=self.conn,
            attributes=["time", "is_train"],
            batch_size=1024,
            shuffle=True,
            delimiter="|$"
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("time", data)
            self.assertIn("is_train", data)
            num_batches += 1
            self.assertEqual(data.shape[1], 4)
            batch_sizes.append(data.shape[0])
        self.assertEqual(num_batches, 11)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 1024)
        self.assertLessEqual(batch_sizes[-1], 1024)

    # TODO: test filter_by


class TestGDSHeteroEdgeLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="hetero")

    def test_init(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=False,
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_iterate_as_homo(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=False,
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("source", data)
            self.assertIn("target", data)
            num_batches += 1
            self.assertEqual(data.shape[1], 2)
            batch_sizes.append(data.shape[0])
        self.assertEqual(num_batches, 6)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 1024)
        self.assertLessEqual(batch_sizes[-1], 1024)

    def test_iterate_hetero(self):
        loader = EdgeLoader(
            graph=self.conn,
            attributes={"v0v0": ["is_train", "is_val"], "v2v0": ["is_train", "is_val"]},
            batch_size=200,
            shuffle=True,
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            batchsize = 0
            if "v0v0" in data:
                self.assertIsInstance(data["v0v0"], DataFrame)
                self.assertIn("is_val", data["v0v0"])
                self.assertIn("is_train", data["v0v0"])
                batchsize += data["v0v0"].shape[0]
                self.assertEqual(data["v0v0"].shape[1], 4)
            if "v2v0" in data:
                self.assertIsInstance(data["v2v0"], DataFrame)
                self.assertIn("is_val", data["v2v0"])
                self.assertIn("is_train", data["v2v0"])
                batchsize += data["v2v0"].shape[0]
                self.assertEqual(data["v2v0"].shape[1], 4)
            self.assertGreater(len(data), 0)
            num_batches += 1
            batch_sizes.append(batchsize)
        self.assertEqual(num_batches, 9)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 200)
        self.assertLessEqual(batch_sizes[-1], 200)

    def test_iterate_hetero_multichar_delimiter(self):
        loader = EdgeLoader(
            graph=self.conn,
            attributes={"v0v0": ["is_train", "is_val"], "v2v0": ["is_train", "is_val"]},
            batch_size=200,
            shuffle=True,
            delimiter="|$"
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            batchsize = 0
            if "v0v0" in data:
                self.assertIsInstance(data["v0v0"], DataFrame)
                self.assertIn("is_val", data["v0v0"])
                self.assertIn("is_train", data["v0v0"])
                batchsize += data["v0v0"].shape[0]
                self.assertEqual(data["v0v0"].shape[1], 4)
            if "v2v0" in data:
                self.assertIsInstance(data["v2v0"], DataFrame)
                self.assertIn("is_val", data["v2v0"])
                self.assertIn("is_train", data["v2v0"])
                batchsize += data["v2v0"].shape[0]
                self.assertEqual(data["v2v0"].shape[1], 4)
            self.assertGreater(len(data), 0)
            num_batches += 1
            batch_sizes.append(batchsize)
        self.assertEqual(num_batches, 9)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 200)
        self.assertLessEqual(batch_sizes[-1], 200)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSEdgeLoaderKafka("test_init"))
    suite.addTest(TestGDSEdgeLoaderKafka("test_iterate"))
    suite.addTest(TestGDSEdgeLoaderKafka("test_whole_edgelist"))
    suite.addTest(TestGDSEdgeLoaderKafka("test_iterate_attr"))
    suite.addTest(TestGDSEdgeLoaderKafka("test_iterate_attr_multichar_delimiter"))
    # suite.addTest(TestGDSEdgeLoader("test_sasl_plaintext"))
    # suite.addTest(TestGDSEdgeLoader("test_sasl_ssl"))
    suite.addTest(TestGDSHeteroEdgeLoaderKafka("test_init"))
    suite.addTest(TestGDSHeteroEdgeLoaderKafka("test_iterate_as_homo"))
    suite.addTest(TestGDSHeteroEdgeLoaderKafka("test_iterate_hetero"))
    suite.addTest(TestGDSHeteroEdgeLoaderKafka("test_iterate_hetero_multichar_delimiter"))
    suite.addTest(TestGDSEdgeLoaderREST("test_init"))
    suite.addTest(TestGDSEdgeLoaderREST("test_iterate"))
    suite.addTest(TestGDSEdgeLoaderREST("test_whole_edgelist"))
    suite.addTest(TestGDSEdgeLoaderREST("test_iterate_attr"))
    suite.addTest(TestGDSEdgeLoaderREST("test_iterate_attr_multichar_delimiter"))
    suite.addTest(TestGDSHeteroEdgeLoaderREST("test_init"))
    suite.addTest(TestGDSHeteroEdgeLoaderREST("test_iterate_as_homo"))
    suite.addTest(TestGDSHeteroEdgeLoaderREST("test_iterate_hetero"))
    suite.addTest(TestGDSHeteroEdgeLoaderREST("test_iterate_hetero_multichar_delimiter"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
