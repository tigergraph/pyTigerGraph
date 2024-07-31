import unittest

from pandas import DataFrame
from pyTigerGraphUnitTest import make_connection

from pyTigerGraph.gds.dataloaders import VertexLoader
from pyTigerGraph.gds.utilities import is_query_installed


class TestGDSVertexLoaderKafka(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def test_init(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            kafka_address="kafka:9092",
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_iterate(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            kafka_address="kafka:9092",
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data.shape, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("x", data.columns)
            self.assertIn("y", data.columns)
            self.assertIn("train_mask", data.columns)
            self.assertIn("val_mask", data.columns)
            self.assertIn("test_mask", data.columns)
            self.assertEqual(data.shape[1], 6)
            batch_sizes.append(data.shape[0])
            num_batches += 1
        self.assertEqual(num_batches, 9)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 16)
        self.assertLessEqual(batch_sizes[-1], 16)

    def test_all_vertices(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            num_batches=1,
            shuffle=False,
            filter_by="train_mask",
            kafka_address="kafka:9092",
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)
        self.assertIn("x", data.columns)
        self.assertIn("y", data.columns)
        self.assertIn("train_mask", data.columns)
        self.assertIn("val_mask", data.columns)
        self.assertIn("test_mask", data.columns)
        self.assertEqual(data.shape[0], 140)
        self.assertEqual(data.shape[1], 6)

    def test_all_vertices_multichar_delimiter(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            num_batches=1,
            shuffle=False,
            filter_by="train_mask",
            delimiter="$|",
            kafka_address="kafka:9092",
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)
        self.assertIn("x", data.columns)
        self.assertIn("y", data.columns)
        self.assertIn("train_mask", data.columns)
        self.assertIn("val_mask", data.columns)
        self.assertIn("test_mask", data.columns)
        self.assertEqual(data.shape[0], 140)
        self.assertEqual(data.shape[1], 6)

    def test_sasl_plaintext(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
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
            self.assertIn("x", data.columns)
            self.assertIn("y", data.columns)
            self.assertIn("train_mask", data.columns)
            self.assertIn("val_mask", data.columns)
            self.assertIn("test_mask", data.columns)
            num_batches += 1
        self.assertEqual(num_batches, 9)

    def test_sasl_ssl(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
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
            self.assertIn("x", data.columns)
            self.assertIn("y", data.columns)
            self.assertIn("train_mask", data.columns)
            self.assertIn("val_mask", data.columns)
            self.assertIn("test_mask", data.columns)
            num_batches += 1
        self.assertEqual(num_batches, 9)


class TestGDSHeteroVertexLoaderKafka(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="hetero")

    def test_init(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes={"v0": ["x", "y"],
                        "v1": ["x"]},
            batch_size=20,
            shuffle=True,
            kafka_address="kafka:9092"
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_iterate(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes={"v0": ["x", "y"],
                        "v1": ["x"]},
            batch_size=20,
            shuffle=True,
            kafka_address="kafka:9092"
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            batchsize = 0
            if "v0" in data:
                self.assertIsInstance(data["v0"], DataFrame)
                self.assertIn("x", data["v0"].columns)
                self.assertIn("y", data["v0"].columns)
                batchsize += data["v0"].shape[0]
                self.assertEqual(data["v0"].shape[1], 3)
            if "v1" in data:
                self.assertIsInstance(data["v1"], DataFrame)
                self.assertIn("x", data["v1"].columns)
                batchsize += data["v1"].shape[0]
                self.assertEqual(data["v1"].shape[1], 2)
            self.assertGreater(len(data), 0)
            num_batches += 1
            batch_sizes.append(batchsize)
        self.assertEqual(num_batches, 10)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 20)
        self.assertLessEqual(batch_sizes[-1], 20)

    def test_all_vertices(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes={"v0": ["x", "y"],
                        "v1": ["x"]},
            num_batches=1,
            shuffle=False,
            kafka_address="kafka:9092"
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data["v0"], DataFrame)
        self.assertTupleEqual(data["v0"].shape, (76, 3))
        self.assertIsInstance(data["v1"], DataFrame)
        self.assertTupleEqual(data["v1"].shape, (110, 2))
        self.assertIn("x", data["v0"].columns)
        self.assertIn("y", data["v0"].columns)
        self.assertIn("x", data["v1"].columns)

    def test_all_vertices_multichar_delimiter(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes={"v0": ["x", "y"],
                        "v1": ["x"]},
            num_batches=1,
            shuffle=False,
            delimiter="|$",
            kafka_address="kafka:9092"
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data["v0"], DataFrame)
        self.assertTupleEqual(data["v0"].shape, (76, 3))
        self.assertIsInstance(data["v1"], DataFrame)
        self.assertTupleEqual(data["v1"].shape, (110, 2))
        self.assertIn("x", data["v0"].columns)
        self.assertIn("y", data["v0"].columns)
        self.assertIn("x", data["v1"].columns)


class TestGDSVertexLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def test_init(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask"
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_iterate(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask"
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data.shape, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("x", data.columns)
            self.assertIn("y", data.columns)
            self.assertIn("train_mask", data.columns)
            self.assertIn("val_mask", data.columns)
            self.assertIn("test_mask", data.columns)
            self.assertEqual(data.shape[1], 6)
            batch_sizes.append(data.shape[0])
            num_batches += 1
        self.assertEqual(num_batches, 9)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 16)
        self.assertLessEqual(batch_sizes[-1], 16)

    def test_all_vertices(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            num_batches=1,
            shuffle=False,
            filter_by="train_mask"
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)
        self.assertIn("x", data.columns)
        self.assertIn("y", data.columns)
        self.assertIn("train_mask", data.columns)
        self.assertIn("val_mask", data.columns)
        self.assertIn("test_mask", data.columns)
        self.assertEqual(data.shape[0], 140)
        self.assertEqual(data.shape[1], 6)

    def test_all_vertices_multichar_delimiter(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            num_batches=1,
            shuffle=False,
            filter_by="train_mask",
            delimiter="$|"
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)
        self.assertIn("x", data.columns)
        self.assertIn("y", data.columns)
        self.assertIn("train_mask", data.columns)
        self.assertIn("val_mask", data.columns)
        self.assertIn("test_mask", data.columns)
        self.assertEqual(data.shape[0], 140)
        self.assertEqual(data.shape[1], 6)

    def test_string_attr(self):
        conn = make_connection(graphname="Social")

        loader = VertexLoader(
            graph=conn,
            attributes=["age", "state"],
            num_batches=1,
            shuffle=False
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)
        self.assertEqual(data.shape[0], 7)
        self.assertEqual(data.shape[1], 3)
        self.assertIn("age", data.columns)
        self.assertIn("state", data.columns)


class TestGDSHeteroVertexLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="hetero")

    def test_init(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes={"v0": ["x", "y"],
                        "v1": ["x"]},
            batch_size=20,
            shuffle=True
        )
        self.assertTrue(is_query_installed(self.conn, loader.query_name))
        self.assertIsNone(loader.num_batches)

    def test_iterate(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes={"v0": ["x", "y"],
                        "v1": ["x"]},
            batch_size=20,
            shuffle=True
        )
        num_batches = 0
        batch_sizes = []
        for data in loader:
            # print(num_batches, data)
            batchsize = 0
            if "v0" in data:
                self.assertIsInstance(data["v0"], DataFrame)
                self.assertIn("x", data["v0"].columns)
                self.assertIn("y", data["v0"].columns)
                batchsize += data["v0"].shape[0]
                self.assertEqual(data["v0"].shape[1], 3)
            if "v1" in data:
                self.assertIsInstance(data["v1"], DataFrame)
                self.assertIn("x", data["v1"].columns)
                batchsize += data["v1"].shape[0]
                self.assertEqual(data["v1"].shape[1], 2)
            self.assertGreater(len(data), 0)
            num_batches += 1
            batch_sizes.append(batchsize)
        self.assertEqual(num_batches, 10)
        for i in batch_sizes[:-1]:
            self.assertEqual(i, 20)
        self.assertLessEqual(batch_sizes[-1], 20)

    def test_all_vertices(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes={"v0": ["x", "y"],
                        "v1": ["x"]},
            num_batches=1,
            shuffle=False
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data["v0"], DataFrame)
        self.assertTupleEqual(data["v0"].shape, (76, 3))
        self.assertIsInstance(data["v1"], DataFrame)
        self.assertTupleEqual(data["v1"].shape, (110, 2))
        self.assertIn("x", data["v0"].columns)
        self.assertIn("y", data["v0"].columns)
        self.assertIn("x", data["v1"].columns)

    def test_all_vertices_multichar_delimiter(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes={"v0": ["x", "y"],
                        "v1": ["x"]},
            num_batches=1,
            shuffle=False,
            delimiter="|$"
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data["v0"], DataFrame)
        self.assertTupleEqual(data["v0"].shape, (76, 3))
        self.assertIsInstance(data["v1"], DataFrame)
        self.assertTupleEqual(data["v1"].shape, (110, 2))
        self.assertIn("x", data["v0"].columns)
        self.assertIn("y", data["v0"].columns)
        self.assertIn("x", data["v1"].columns)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSVertexLoaderREST("test_init"))
    suite.addTest(TestGDSVertexLoaderREST("test_iterate"))
    suite.addTest(TestGDSVertexLoaderREST("test_all_vertices"))
    suite.addTest(TestGDSVertexLoaderREST("test_all_vertices_multichar_delimiter"))
    suite.addTest(TestGDSVertexLoaderREST("test_string_attr"))
    suite.addTest(TestGDSHeteroVertexLoaderREST("test_init"))
    suite.addTest(TestGDSHeteroVertexLoaderREST("test_iterate"))
    suite.addTest(TestGDSHeteroVertexLoaderREST("test_all_vertices"))
    suite.addTest(TestGDSHeteroVertexLoaderREST("test_all_vertices_multichar_delimiter"))
    suite.addTest(TestGDSVertexLoaderKafka("test_init"))
    suite.addTest(TestGDSVertexLoaderKafka("test_iterate"))
    suite.addTest(TestGDSVertexLoaderKafka("test_all_vertices"))
    suite.addTest(TestGDSVertexLoaderKafka("test_all_vertices_multichar_delimiter"))
    # suite.addTest(TestGDSVertexLoaderKafka("test_sasl_plaintext"))
    # suite.addTest(TestGDSVertexLoaderKafka("test_sasl_ssl"))
    suite.addTest(TestGDSHeteroVertexLoaderKafka("test_init"))
    suite.addTest(TestGDSHeteroVertexLoaderKafka("test_iterate"))
    suite.addTest(TestGDSHeteroVertexLoaderKafka("test_all_vertices"))
    suite.addTest(TestGDSHeteroVertexLoaderKafka("test_all_vertices_multichar_delimiter"))
    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
