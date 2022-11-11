import os
import unittest
from io import StringIO
from textwrap import dedent
from unittest import runner
from unittest.mock import patch

from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.gds.featurizer import Featurizer
from pyTigerGraph.gds.utilities import is_query_installed


class test_Featurizer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conn = TigerGraphConnection(host="http://tigergraph", graphname="Cora")
        conn.getToken(conn.createSecret())
        cls.featurizer = Featurizer(conn, algo_version="3.7")

    def test_get_db_version(self):
        major_ver, minor_ver, patch_ver = self.featurizer._get_db_version()
        self.assertIsNotNone(int(major_ver))
        self.assertIsNotNone(int(minor_ver))
        self.assertIsNotNone(int(patch_ver))
        self.assertIsInstance(self.featurizer.algo_ver, str)

    def test_get_algo_dict(self):
        path = os.path.dirname(os.path.realpath(__file__))
        fname = os.path.join(path, "fixtures/manifest.json")
        algo_dict = self.featurizer._get_algo_dict(fname)
        self.assertIsInstance(algo_dict, dict)
        self.assertIn("Centrality", algo_dict)

    @patch('sys.stdout', new_callable=StringIO)
    def test_listAlgorithms(self, mock_stdout):
        self.featurizer.listAlgorithms()
        truth = """\
            Available algorithms per category:
            - Centrality: 10 algorithms
            - Classification: 3 algorithms
            - Community: 6 algorithms
            - Embeddings: 1 algorithms
            - Path: 3 algorithms
            - Topological Link Prediction: 6 algorithms
            - Similarity: 3 algorithms
            Call listAlgorithms() with the category name to see the list of algorithms
            """
        self.assertEqual(mock_stdout.getvalue(), dedent(truth))

    @patch('sys.stdout', new_callable=StringIO)
    def test_listAlgorithms_category(self, mock_stdout):
        self.featurizer.listAlgorithms("Centrality")
        truth = """\
            Available algorithms for Centrality:
              pagerank:
                weighted:
                  01. name: tg_pagerank_wt
                unweighted:
                  02. name: tg_pagerank
              article_rank:
                03. name: tg_article_rank
              betweenness:
                04. name: tg_betweenness_cent
              closeness:
                approximate:
                  05. name: tg_closeness_cent_approx
                exact:
                  06. name: tg_closeness_cent
              degree:
                unweighted:
                  07. name: tg_degree_cent
                weighted:
                  08. name: tg_weighted_degree_cent
              eigenvector:
                09. name: tg_eigenvector_cent
              harmonic:
                10. name: tg_harmonic_cent
            Call runAlgorithm() with the algorithm name to execute it
            """
        self.maxDiff=None
        self.assertEqual(mock_stdout.getvalue(), dedent(truth))

    def test_install_query_file(self):
        query_path = "https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/3.7/algorithms/Centrality/pagerank/global/unweighted/tg_pagerank.gsql"
        resp = self.featurizer._install_query_file(query_path) 
        self.assertEqual(resp, "tg_pagerank")
        self.assertTrue(is_query_installed(self.featurizer.conn, "tg_pagerank"))

    def test_get_algo_details(self):
        path = os.path.dirname(os.path.realpath(__file__))
        fname = os.path.join(path, "fixtures/manifest.json")
        algo_dict = self.featurizer._get_algo_dict(fname)
        res = self.featurizer._get_algo_details(algo_dict["Path"])
        self.assertDictEqual(
            res[0],
            {'tg_bfs': ['https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/3.7/algorithms/Path/bfs/tg_bfs.gsql'], 
             'tg_cycle_detection_count': ['https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/3.7/algorithms/Path/cycle_detection/count/tg_cycle_detection_count.gsql'], 
             'tg_shortest_ss_no_wt': ['https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/3.7/algorithms/Path/shortest_path/unweighted/tg_shortest_ss_no_wt.gsql']})
        self.assertDictEqual(
            res[1],
            {'tg_bfs': "INT", 
             'tg_shortest_ss_no_wt': "INT"})

    def test_get_Params(self):
        _dict = {'v_type': None,
            'e_type': None,
            'max_change': 0.001,
            'max_iter': 25,
            'damping': 0.85,
            'top_k': 100,
            'print_accum': True,
            'result_attr': '', 
            'file_path': '',
            'display_edges': True}
        self.assertEqual(self.featurizer._get_Params("tg_pagerank"), _dict)

    def test01_add_attribute(self):
        self.assertEqual(self.featurizer._add_attribute("VERTEX", "FLOAT", "attr1", global_change=False), 'Schema change succeeded.')

    def test02_add_attribute(self):
        self.assertEqual(self.featurizer._add_attribute("Edge", "BOOL", "attr2", global_change=False), 'Schema change succeeded.')
    
    def test03_add_attribute(self):
        self.assertEqual(self.featurizer._add_attribute("Vertex", "BOOL", "attr1", global_change=False), 'Attribute already exists')

    def test04_add_attribute(self):
        with self.assertRaises(Exception) as context:
            self.featurizer._add_attribute("Something","BOOL","attr3")
        self.assertTrue('schema_type has to be VERTEX or EDGE' in str(context.exception))
    
    def test05_add_attribute(self):
        self.assertEqual(self.featurizer._add_attribute("VERTEX", "BOOL", "attr4", ['Paper'], global_change=False), 'Schema change succeeded.')

    def test01_installAlgorithm(self):
       self.assertEqual(self.featurizer.installAlgorithm("tg_pagerank"), "tg_pagerank")

    def test02_installAlgorithm(self):
        with self.assertRaises(Exception):
            self.featurizer.installAlgorithm("someQuery")
 
    def test01_runAlgorithm(self):
        params = {'v_type': 'Paper',
            'e_type': 'Cite',
            'max_change': 0.001,
            'max_iter': 25,
            'damping': 0.85,
            'top_k': 100,
            'print_accum': True,
            'result_attr': '', 
            'file_path': '',
            'display_edges': True}
        message = "Test value is not none."
        self.assertIsNotNone(self.featurizer.runAlgorithm("tg_pagerank", params=params, feat_name="pagerank", timeout=2147480, global_schema=False), message)

    def test02_runAlgorithm(self):
        with self.assertRaises(ValueError):
            self.featurizer.runAlgorithm("tg_pagerank", timeout=2147480)

    def test03_runAlgorithm(self):
        params = {'v_type': 'Paper', 'e_type': ['Cite'], 'weights': '1,1,2', 'beta': -0.85, 'k': 3, 'reduced_dim': 128,
          'sampling_constant': 1, 'random_seed': 42, 'print_accum': False,'result_attr':"",'file_path' :""}
        with self.assertRaises(Exception):
            self.featurizer.runAlgorithm("tg_fastRP", params=params, feat_name="fastrp_embedding", timeout=1, global_schema=False)

    def test04_runAlgorithm(self):
        params = {'v_type': 'Paper', 'e_type': ['Cite'], 'weights': '1,1,2', 'beta': -0.85, 'k': 3, 'reduced_dim': 128, 
          'sampling_constant': 1, 'random_seed': 42, 'print_accum': False,'result_attr':"",'file_path' :""}
        with self.assertRaises(Exception):
            self.featurizer.runAlgorithm("tg_fastRP", params=params, feat_name="fastrp_embedding", sizeLimit=1, global_schema=False)
    
    def test05_runAlgorithm(self):
        params = {'v_type': 'Paper',
            'e_type': 'Cite',
            'max_change': 0.001,
            'max_iter': 25,
            'damping': 0.85,
            'top_k': 100,
            'print_accum': True,
            'file_path': '',
            'display_edges': True}
        message = "Test value is not none."
        self.assertIsNotNone(self.featurizer.runAlgorithm("tg_pagerank", params=params, timeout=2147480, global_schema=False), message)

    def test06_installCustomAlgorithm(self):
        path = os.path.dirname(os.path.realpath(__file__))
        fname = os.path.join(path, "fixtures/create_query_simple.gsql")
        out = self.featurizer.installAlgorithm("simple_query", query_path=fname)
        self.assertEqual(out, "simple_query")
    
    def test07_runCustomAlgorithm(self):
        out = self.featurizer.runAlgorithm("simple_query", params={}, feat_name="test_feat", feat_type="INT", schema_name=["Paper"], custom_query=True, global_schema=False)
        self.assertEqual(out[0]['"Hello World!"'], "Hello World!")

if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(test_Featurizer("test_get_db_version"))
    suite.addTest(test_Featurizer("test_get_algo_dict"))
    suite.addTest(test_Featurizer("test_listAlgorithms"))
    suite.addTest(test_Featurizer("test_listAlgorithms_category"))
    suite.addTest(test_Featurizer("test_install_query_file"))
    suite.addTest(test_Featurizer("test_get_algo_details"))
    suite.addTest(test_Featurizer("test_get_Params"))
    suite.addTest(test_Featurizer("test01_add_attribute"))
    suite.addTest(test_Featurizer("test02_add_attribute"))
    suite.addTest(test_Featurizer("test03_add_attribute"))
    suite.addTest(test_Featurizer("test04_add_attribute"))
    suite.addTest(test_Featurizer("test05_add_attribute"))
    suite.addTest(test_Featurizer("test01_installAlgorithm"))
    suite.addTest(test_Featurizer("test02_installAlgorithm"))
    suite.addTest(test_Featurizer("test01_runAlgorithm"))
    suite.addTest(test_Featurizer("test02_runAlgorithm"))
    suite.addTest(test_Featurizer("test03_runAlgorithm")) 
    suite.addTest(test_Featurizer("test04_runAlgorithm"))
    suite.addTest(test_Featurizer("test05_runAlgorithm"))
    suite.addTest(test_Featurizer("test06_installCustomAlgorithm"))
    suite.addTest(test_Featurizer("test07_runCustomAlgorithm"))
    
    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)

    
    