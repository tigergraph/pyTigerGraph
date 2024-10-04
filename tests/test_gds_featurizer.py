import os
import unittest
from io import StringIO
from textwrap import dedent
from unittest import runner
from unittest.mock import patch

from pyTigerGraphUnitTest import make_connection

from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.gds.featurizer import Featurizer
from pyTigerGraph.gds.utilities import is_query_installed, add_attribute


class test_Featurizer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conn = make_connection(graphname="Cora")
        cls.featurizer = Featurizer(conn, algo_version="3.8")
        cls.conn = conn

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
            - Classification: 6 algorithms
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
        self.maxDiff = None
        self.assertEqual(mock_stdout.getvalue(), dedent(truth))

    def test_install_query_file(self):
        query_path = "https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/3.7/algorithms/Centrality/pagerank/global/unweighted/tg_pagerank.gsql"
        resp = self.featurizer._install_query_file(query_path)
        self.assertEqual(resp, "tg_pagerank")
        self.assertTrue(is_query_installed(
            self.featurizer.conn, "tg_pagerank"))

    def test_get_algo_details(self):
        path = os.path.dirname(os.path.realpath(__file__))
        fname = os.path.join(path, "fixtures/manifest.json")
        algo_dict = self.featurizer._get_algo_dict(fname)
        res = self.featurizer._get_algo_details(algo_dict["Path"])
        self.assertDictEqual(
            res[0],
            {'tg_bfs': ['https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/3.8/algorithms/Path/bfs/tg_bfs.gsql'],
             'tg_cycle_detection_count': ['https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/3.8/algorithms/Path/cycle_detection/count/tg_cycle_detection_count.gsql'],
             'tg_shortest_ss_no_wt': ['https://raw.githubusercontent.com/tigergraph/gsql-graph-algorithms/3.8/algorithms/Path/shortest_path/unweighted/tg_shortest_ss_no_wt.gsql']})
        self.assertDictEqual(
            res[1],
            {'tg_bfs': "INT",
             'tg_shortest_ss_no_wt': "INT"})

    def test_get_params(self):
        query = """
        Create QUERY myquery(
            SET<STRING> v_type, 
            SET<STRING> e_type, 
            VERTEX source,
            STRING iteration_weights,
            STRING wt_attr ="weight",
            INT max_iter= 10, 
            STRING file_path="", 
            BOOL print_info = FALSE
        )
        """
        true_values = {
            'v_type': None,
            'e_type': None,
            'source': None,
            'iteration_weights': None,
            'wt_attr': "weight",
            'max_iter': 10,
            'file_path': '',
            'print_info': False
        }
        true_types = {
            'v_type': "SET<STRING>",
            'e_type': "SET<STRING>",
            'source': "VERTEX",
            'iteration_weights': "str",
            'wt_attr': "str",
            'max_iter': "int",
            'file_path': "str",
            'print_info': "bool"
        }
        param_values, param_types = self.featurizer._get_params(query)
        self.assertDictEqual(param_values, true_values)
        self.assertDictEqual(param_types, true_types)

    def test_get_params_emtpy(self):
        query = """
        Create query no_param()
        """

        param_values, param_types = self.featurizer._get_params(query)
        self.assertDictEqual(param_values, {})
        self.assertDictEqual(param_types, {})

    def test_getParams(self):
        params = self.featurizer.getParams("tg_pagerank", printout=False)
        truth = {
            "v_type": None,
            "e_type": None,
            "max_change": 0.001,
            "maximum_iteration": 25,
            "damping": 0.85,
            "top_k": 100,
            "print_results": True,
            "result_attribute": "",
            "file_path": "",
            "display_edges": False
        }
        self.assertDictEqual(params, truth)

    @patch('sys.stdout', new_callable=StringIO)
    def test_getParams_print(self, mock_stdout):
        _ = self.featurizer.getParams("tg_pagerank", printout=True)
        truth = """\
            Parameters for tg_pagerank (parameter: type [= default value]):
            - v_type: str
            - e_type: str
            - max_change: float = 0.001
            - maximum_iteration: int = 25
            - damping: float = 0.85
            - top_k: int = 100
            - print_results: bool = True
            - result_attribute: str = ""
            - file_path: str = ""
            - display_edges: bool = False
            """
        self.assertEqual(mock_stdout.getvalue(), dedent(truth))

    def test01_add_attribute(self):
        self.assertEqual(add_attribute(self.conn, "VERTEX", "FLOAT",
                         "attr1", global_change=False), 'Schema change succeeded.')

    def test02_add_attribute(self):
        self.assertEqual(add_attribute(self.conn, "Edge", "BOOL",
                         "attr2", global_change=False), 'Schema change succeeded.')

    def test03_add_attribute(self):
        self.assertEqual(add_attribute(self.conn, "Vertex", "BOOL",
                         "attr1", global_change=False), 'Attribute already exists')

    def test04_add_attribute(self):
        with self.assertRaises(Exception) as context:
            add_attribute(self.conn, "Something", "BOOL", "attr3")
        self.assertTrue(
            'schema_type has to be VERTEX or EDGE' in str(context.exception))

    def test05_add_attribute(self):
        self.assertEqual(add_attribute(self.conn, "VERTEX", "BOOL", "attr4", [
                         'Paper'], global_change=False), 'Schema change succeeded.')

    def test01_installAlgorithm(self):
        self.assertEqual(self.featurizer.installAlgorithm(
            "tg_pagerank"), "tg_pagerank")

    def test02_installAlgorithm(self):
        with self.assertRaises(Exception):
            self.featurizer.installAlgorithm("someQuery")

    def test01_runAlgorithm(self):
        params = {'v_type': 'Paper',
                  'e_type': 'Cite',
                  'max_change': 0.001,
                  'maximum_iteration': 25,
                  'damping': 0.85,
                  'top_k': 100,
                  'print_results': True,
                  'result_attribute': 'pagerank',
                  'file_path': '',
                  'display_edges': True}
        self.assertIsNotNone(self.featurizer.runAlgorithm(
            "tg_pagerank", params=params))

    def test02_runAlgorithm(self):
        with self.assertRaises(ValueError) as error:
            self.featurizer.runAlgorithm("tg_pagerank")
        self.assertIn('Missing mandatory parameters:', str(error.exception))

        with self.assertRaises(ValueError) as error:
            self.featurizer.runAlgorithm(
                "tg_pagerank", params={'v_type': 'Paper'})
        self.assertIn('Missing mandatory parameters:', str(error.exception))

        with self.assertRaises(ValueError) as error:
            self.featurizer.runAlgorithm("tg_pagerank", params={'foo': 'bar'})
        self.assertIn("Unknown parameters: ['foo']", str(error.exception))

    def test03_runAlgorithm(self):
        params = {
            "v_type": ["Paper"],
            "e_type": ["Cite"],
            "output_v_type": ["Paper"],
            "iteration_weights": "1,2,4",
            "beta": -0.1,
            "embedding_dimension": 128,
            "embedding_dim_map": [],
            "default_length": 128,
            "sampling_constant": 3,
            "random_seed": 42,
            "component_attribute": "",
            "result_attribute": "embedding",
            "choose_k": 0}
        self.featurizer.runAlgorithm("tg_fastRP", params=params)

    def test06_installCustomAlgorithm(self):
        path = os.path.dirname(os.path.realpath(__file__))
        fname = os.path.join(path, "fixtures/create_query_simple.gsql")
        out = self.featurizer.installAlgorithm(
            "simple_query", query_path=fname)
        self.assertEqual(out, "simple_query")

    def test07_runCustomAlgorithm(self):
        out = self.featurizer.runAlgorithm(
            "simple_query", params={}, custom_query=True)
        self.assertEqual(out[0]['"Hello World!"'], "Hello World!")

    def test08_runAlgorithm_async_qid(self):
        params = {'v_type': 'Paper',
                  'e_type': 'Cite',
                  'max_change': 0.001,
                  'maximum_iteration': 25,
                  'damping': 0.85,
                  'top_k': 100,
                  'print_results': True,
                  'result_attribute': 'pagerank',
                  'file_path': '',
                  'display_edges': True}
        ret = self.featurizer.runAlgorithm(
            "tg_pagerank", params=params, runAsync=True)
        self.assertIsNotNone(ret.query_id)

    def test09_runAlgorithm_async_wait(self):
        params = {'v_type': 'Paper',
                  'e_type': 'Cite',
                  'max_change': 0.001,
                  'maximum_iteration': 25,
                  'damping': 0.85,
                  'top_k': 100,
                  'print_results': True,
                  'result_attribute': 'pagerank',
                  'file_path': '',
                  'display_edges': True}
        ret = self.featurizer.runAlgorithm(
            "tg_pagerank", params=params, runAsync=True)
        self.assertIsNotNone(ret.wait())

    def test_get_template_queries(self):
        if (self.featurizer.major_ver != "master" and (
            int(self.featurizer.major_ver) < 3 or (
                int(self.featurizer.major_ver) == 3 and int(self.featurizer.minor_ver) < 8)
        )
        ):
            print("Skip test_get_template_queries as the DB version is not supported.")
            return
        self.conn.gsql("IMPORT PACKAGE GDBMS_ALGO")
        self.featurizer._get_template_queries()
        self.assertIn("centrality", self.featurizer.template_queries)
        self.assertIn("article_rank(string v_type, string e_type, float max_change, int maximum_iteration, float damping, int top_k, bool print_results, string result_attribute, string file_path)",
                      self.featurizer.template_queries["centrality"])

    def test_template_query(self):
        if (self.featurizer.major_ver != "master" and (
            int(self.featurizer.major_ver) < 3 or (
                int(self.featurizer.major_ver) == 3 and int(self.featurizer.minor_ver) < 9)
        )
        ):
            print("Skip test_template_query as the DB version is not supported.")
            return
        params = {'v_type': 'Paper',
                  'e_type': 'Cite',
                  'max_change': 0.001,
                  'maximum_iteration': 25,
                  'damping': 0.85,
                  'top_k': 100,
                  'print_results': True,
                  'result_attribute': 'pagerank',
                  'file_path': '',
                  'display_edges': False}

        resp = self.featurizer.runAlgorithm(
            "tg_pagerank", params, templateQuery=True)
        self.assertIn("@@top_scores_heap", resp[0])
        self.assertEqual(len(resp[0]["@@top_scores_heap"]), 100)


if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(test_Featurizer("test_get_db_version"))
    suite.addTest(test_Featurizer("test_get_algo_dict"))
    suite.addTest(test_Featurizer("test_listAlgorithms"))
    suite.addTest(test_Featurizer("test_listAlgorithms_category"))
    suite.addTest(test_Featurizer("test_install_query_file"))
    suite.addTest(test_Featurizer("test_get_algo_details"))
    suite.addTest(test_Featurizer("test_get_params_emtpy"))
    suite.addTest(test_Featurizer("test_get_params"))
    suite.addTest(test_Featurizer("test_getParams"))
    suite.addTest(test_Featurizer("test_getParams_print"))
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
    suite.addTest(test_Featurizer("test06_installCustomAlgorithm"))
    suite.addTest(test_Featurizer("test07_runCustomAlgorithm"))
    suite.addTest(test_Featurizer("test08_runAlgorithm_async_qid"))
    suite.addTest(test_Featurizer("test09_runAlgorithm_async_wait"))
    suite.addTest(test_Featurizer("test_get_template_queries"))
    suite.addTest(test_Featurizer("test_template_query"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
