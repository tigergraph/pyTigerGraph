import unittest

from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.gds import utilities as utils


class TestGDSUtilsQuery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        cls.conn.gsql("drop query simple_query")
        cls.conn.gsql("drop query simple_query_something_special")

    def test_is_query_installed(self):
        self.assertFalse(utils.is_query_installed(self.conn, "simple_query"))

    def test_install_query_file(self):
        resp = utils.install_query_file(
            self.conn,
            "./tests/fixtures/create_query_simple.gsql"
        )
        self.assertEqual(resp, "simple_query")
        self.assertTrue(utils.is_query_installed(self.conn, "simple_query"))

    def test_install_exist_query(self):
        resp = utils.install_query_file(
            self.conn,
            "./tests/fixtures/create_query_simple.gsql"
        )
        self.assertEqual(resp, "simple_query")

    def test_install_query_template(self):
        replace = {
            "{QUERYSUFFIX}": "something_special",
            "{VERTEXATTRS}": "s.id,s.x,s.y",
        }
        resp = utils.install_query_file(
            self.conn,
            "./tests/fixtures/create_query_template.gsql", 
            replace
        )
        self.assertEqual(resp, "simple_query_something_special")
        self.assertTrue(
            utils.is_query_installed(self.conn, "simple_query_something_special")
        )


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSUtilsQuery("test_is_query_installed"))
    suite.addTest(TestGDSUtilsQuery("test_install_query_file"))
    suite.addTest(TestGDSUtilsQuery("test_install_exist_query"))
    suite.addTest(TestGDSUtilsQuery("test_install_query_template"))
    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
