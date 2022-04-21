import unittest

from pyTigerGraph import TigerGraphConnection


class TestGDSRandomVertexSplit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora2")
        # cls.conn.gsql("drop query all")

    def test_no_attr(self):
        with self.assertRaises(ValueError):
            splitter = self.conn.gds.vertexSplitter()

    def test_bad_attr(self):
        with self.assertRaises(ValueError):
            splitter = self.conn.gds.vertexSplitter(train_mask=-0.1)
        with self.assertRaises(ValueError):
            splitter = self.conn.gds.vertexSplitter(train_mask=1.1)
        with self.assertRaises(ValueError):
            splitter = self.conn.gds.vertexSplitter(train_mask=0.6, val_mask=0.7)

    def test_one_attr(self):
        splitter = self.conn.gds.vertexSplitter(train_mask=0.6)
        splitter.run()
        num_vertices = self.conn.getVertexCount("Paper2")
        p1_count = self.conn.getVertexCount("Paper2", where="train_mask!=0")
        self.assertAlmostEqual(p1_count / num_vertices, 0.6, delta=0.05)

    def test_two_attr(self):
        splitter = self.conn.gds.vertexSplitter(train_mask=0.6, val_mask=0.3)
        splitter.run()
        num_vertices = self.conn.getVertexCount("Paper2")
        p1_count = self.conn.getVertexCount("Paper2", where="train_mask!=0")
        p2_count = self.conn.getVertexCount("Paper2", where="val_mask!=0")
        self.assertAlmostEqual(p1_count / num_vertices, 0.6, delta=0.05)
        self.assertAlmostEqual(p2_count / num_vertices, 0.3, delta=0.05)

    def test_three_attr(self):
        splitter = self.conn.gds.vertexSplitter(
            train_mask=0.6, val_mask=0.3, test_mask=0.1
        )
        splitter.run()
        num_vertices = self.conn.getVertexCount("Paper2")
        p1_count = self.conn.getVertexCount("Paper2", where="train_mask!=0")
        p2_count = self.conn.getVertexCount("Paper2", where="val_mask!=0")
        p3_count = self.conn.getVertexCount("Paper2", where="test_mask!=0")
        self.assertAlmostEqual(p1_count / num_vertices, 0.6, delta=0.05)
        self.assertAlmostEqual(p2_count / num_vertices, 0.3, delta=0.05)
        self.assertAlmostEqual(p3_count / num_vertices, 0.1, delta=0.05)

    def test_too_many_attr(self):
        with self.assertRaises(ValueError):
            splitter = self.conn.gds.vertexSplitter(
                train_mask=0.6, val_mask=0.3, test_mask=0.1, extra_mask=0.0
            )

    def test_override_attr(self):
        splitter = self.conn.gds.vertexSplitter(
            train_mask=0.6, val_mask=0.3, test_mask=0.1
        )
        splitter.run(train_mask=0.1, val_mask=0.2, test_mask=0.7)
        num_vertices = self.conn.getVertexCount("Paper2")
        p1_count = self.conn.getVertexCount("Paper2", where="train_mask!=0")
        p2_count = self.conn.getVertexCount("Paper2", where="val_mask!=0")
        p3_count = self.conn.getVertexCount("Paper2", where="test_mask!=0")
        self.assertAlmostEqual(p1_count / num_vertices, 0.1, delta=0.05)
        self.assertAlmostEqual(p2_count / num_vertices, 0.2, delta=0.05)
        self.assertAlmostEqual(p3_count / num_vertices, 0.7, delta=0.05)


if __name__ == "__main__":
    unittest.main(verbosity=2, failfast=True)
