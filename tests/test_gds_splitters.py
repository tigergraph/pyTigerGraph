import json
import unittest

from pyTigerGraphUnitTest import make_connection

from pyTigerGraph import TigerGraphConnection


class TestGDSRandomVertexSplit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def test_no_attr(self):
        with self.assertRaises(ValueError):
            splitter = self.conn.gds.vertexSplitter()

    def test_bad_attr(self):
        with self.assertRaises(ValueError):
            splitter = self.conn.gds.vertexSplitter(train_mask=-0.1)
        with self.assertRaises(ValueError):
            splitter = self.conn.gds.vertexSplitter(train_mask=1.1)
        with self.assertRaises(ValueError):
            splitter = self.conn.gds.vertexSplitter(
                train_mask=0.6, val_mask=0.7)

    def test_one_attr(self):
        splitter = self.conn.gds.vertexSplitter(train_mask=0.6)
        splitter.run()
        num_vertices = self.conn.getVertexCount("Paper")
        p1_count = self.conn.getVertexCount("Paper", where="train_mask!=0")
        self.assertAlmostEqual(p1_count / num_vertices, 0.6, delta=0.05)

    def test_two_attr(self):
        splitter = self.conn.gds.vertexSplitter(train_mask=0.6, val_mask=0.3)
        splitter.run()
        num_vertices = self.conn.getVertexCount("Paper")
        p1_count = self.conn.getVertexCount("Paper", where="train_mask!=0")
        p2_count = self.conn.getVertexCount("Paper", where="val_mask!=0")
        self.assertAlmostEqual(p1_count / num_vertices, 0.6, delta=0.05)
        self.assertAlmostEqual(p2_count / num_vertices, 0.3, delta=0.05)

    def test_three_attr(self):
        splitter = self.conn.gds.vertexSplitter(
            train_mask=0.6, val_mask=0.3, test_mask=0.1
        )
        splitter.run()
        num_vertices = self.conn.getVertexCount("Paper")
        p1_count = self.conn.getVertexCount("Paper", where="train_mask!=0")
        p2_count = self.conn.getVertexCount("Paper", where="val_mask!=0")
        p3_count = self.conn.getVertexCount("Paper", where="test_mask!=0")
        self.assertAlmostEqual(p1_count / num_vertices, 0.6, delta=0.05)
        self.assertAlmostEqual(p2_count / num_vertices, 0.3, delta=0.05)
        self.assertAlmostEqual(p3_count / num_vertices, 0.1, delta=0.05)

    def test_three_attr_add_attribute(self):
        splitter = self.conn.gds.vertexSplitter(
            train_mask_2=0.6, val_mask_2=0.3, test_mask_2=0.1
        )
        splitter.run()
        num_vertices = self.conn.getVertexCount("Paper")
        p1_count = self.conn.getVertexCount("Paper", where="train_mask_2!=0")
        p2_count = self.conn.getVertexCount("Paper", where="val_mask_2!=0")
        p3_count = self.conn.getVertexCount("Paper", where="test_mask_2!=0")
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
        num_vertices = self.conn.getVertexCount("Paper")
        p1_count = self.conn.getVertexCount("Paper", where="train_mask!=0")
        p2_count = self.conn.getVertexCount("Paper", where="val_mask!=0")
        p3_count = self.conn.getVertexCount("Paper", where="test_mask!=0")
        self.assertAlmostEqual(p1_count / num_vertices, 0.1, delta=0.05)
        self.assertAlmostEqual(p2_count / num_vertices, 0.2, delta=0.05)
        self.assertAlmostEqual(p3_count / num_vertices, 0.7, delta=0.05)

    def test_v_types(self):
        splitter = self.conn.gds.vertexSplitter(
            v_types=["Paper"], train_mask=0.3)
        splitter.run()
        num_vertices = self.conn.getVertexCount("Paper")
        p1_count = self.conn.getVertexCount("Paper", where="train_mask!=0")
        self.assertAlmostEqual(p1_count / num_vertices, 0.3, delta=0.05)


def get_edge_count(conn: TigerGraphConnection, attribute: str):
    gsql = """
        USE GRAPH Cora\n
        INTERPRET QUERY () FOR GRAPH Cora {{ 
            SumAccum<INT> @@edges;
            start = {{ANY}};
            res = 
                SELECT s
                FROM start:s -(_>:e)- :t
                WHERE e.{}
                ACCUM @@edges+=1;
            PRINT @@edges as edges; 
            }}
    """.format(
        attribute
    )
    resp = conn.gsql(gsql)
    resp_json = json.loads("\n".join(resp.split("\n")[1:]))
    return resp_json["results"][0]["edges"]


class TestGDSRandomEdgeSplit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def test_no_attr(self):
        with self.assertRaises(ValueError):
            splitter = self.conn.gds.edgeSplitter()

    def test_bad_attr(self):
        with self.assertRaises(ValueError):
            splitter = self.conn.gds.edgeSplitter(train_mask=-0.1)
        with self.assertRaises(ValueError):
            splitter = self.conn.gds.edgeSplitter(train_mask=1.1)
        with self.assertRaises(ValueError):
            splitter = self.conn.gds.edgeSplitter(train_mask=0.6, val_mask=0.7)

    def test_one_attr(self):
        splitter = self.conn.gds.edgeSplitter(is_train=0.6)
        splitter.run()
        num_edges = self.conn.getEdgeCount("Cite")
        p1_count = get_edge_count(self.conn, "is_train")
        self.assertAlmostEqual(p1_count / num_edges, 0.6, delta=0.05)

    def test_two_attr(self):
        splitter = self.conn.gds.edgeSplitter(is_train=0.6, is_val=0.3)
        splitter.run()
        num_edges = self.conn.getEdgeCount("Cite")
        p1_count = get_edge_count(self.conn, "is_train")
        p2_count = get_edge_count(self.conn, "is_val")
        self.assertAlmostEqual(p1_count / num_edges, 0.6, delta=0.05)
        self.assertAlmostEqual(p2_count / num_edges, 0.3, delta=0.05)

    def test_two_attr_add_attribute(self):
        splitter = self.conn.gds.edgeSplitter(is_train_2=0.6, is_val_2=0.3)
        splitter.run()
        num_edges = self.conn.getEdgeCount("Cite")
        p1_count = get_edge_count(self.conn, "is_train_2")
        p2_count = get_edge_count(self.conn, "is_val_2")
        self.assertAlmostEqual(p1_count / num_edges, 0.6, delta=0.05)
        self.assertAlmostEqual(p2_count / num_edges, 0.3, delta=0.05)

    # def test_three_attr(self):
    #     splitter = self.conn.gds.edgeSplitter(
    #         train_mask=0.6, val_mask=0.3, test_mask=0.1
    #     )
    #     splitter.run()
    #     num_edges = self.conn.getEdgeCount("Cite")
    #     p1_count = get_edge_count(self.conn, "train_mask")
    #     p2_count = get_edge_count(self.conn, "val_mask")
    #     p3_count = get_edge_count(self.conn, "test_mask")
    #     self.assertAlmostEqual(p1_count / num_edges, 0.6, delta=0.05)
    #     self.assertAlmostEqual(p2_count / num_edges, 0.3, delta=0.05)
    #     self.assertAlmostEqual(p3_count / num_edges, 0.1, delta=0.05)

    def test_too_many_attr(self):
        with self.assertRaises(ValueError):
            splitter = self.conn.gds.edgeSplitter(
                train_mask=0.6, val_mask=0.3, test_mask=0.1, extra_mask=0.0
            )

    def test_override_attr(self):
        splitter = self.conn.gds.edgeSplitter(
            is_train=0.6, is_val=0.3
        )
        splitter.run(is_train=0.1, is_val=0.2)
        num_edges = self.conn.getEdgeCount("Cite")
        p1_count = get_edge_count(self.conn, "is_train")
        p2_count = get_edge_count(self.conn, "is_val")
        self.assertAlmostEqual(p1_count / num_edges, 0.1, delta=0.05)
        self.assertAlmostEqual(p2_count / num_edges, 0.2, delta=0.05)

    def test_e_types(self):
        splitter = self.conn.gds.edgeSplitter(e_types=["Cite"], is_train=0.3)
        splitter.run()
        num_vertices = self.conn.getEdgeCount("Cite")
        p1_count = get_edge_count(self.conn, "is_train")
        self.assertAlmostEqual(p1_count / num_vertices, 0.3, delta=0.05)


if __name__ == "__main__":
    unittest.main(verbosity=2, failfast=True)
