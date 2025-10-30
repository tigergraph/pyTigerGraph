import unittest
from datetime import datetime
from time import sleep

from pyTigerGraphUnitTest import make_connection

from pyTigerGraph.common.exception import TigerGraphException


class test_pyTigerGraphQuery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection()

    def test_01_getQueries(self):
        # TODO Once pyTigerGraphQuery.getQueries() is available
        pass

    def test_02_getInstalledQueries(self):
        res = self.conn.getInstalledQueries()
        self.assertIn("GET /query/tests/query1", res)
        #self.assertNotIn("GET /query/tests/query2_not_installed", res)
        self.assertIn("GET /query/tests/query3_installed", res)

    def test_03_runInstalledQuery(self):
        res = self.conn.runInstalledQuery("query1")
        self.assertIn("ret", res[0])
        self.assertEqual(15, res[0]["ret"])

        params = {
            "p01_int": 1,
            "p02_uint": 1,
            "p03_float": 1.1,
            "p04_double": 1.1,
            "p05_string": "test <>\"'`\\/{}[]()<>!@£$%^&*-_=+;:|,.§±~` árvíztűrő tükörfúrógép 👍",
            "p06_bool": True,
            "p07_vertex": (1, "vertex4"),
            "p08_vertex_vertex4": 1,
            "p09_datetime": datetime.now(),
            # Intentionally bag-like, to see it behaving as set
            "p10_set_int": [1, 2, 3, 2, 3, 3],
            "p11_bag_int": [1, 2, 3, 2, 3, 3],
            "p13_set_vertex": [(1, "vertex4"), (2, "vertex4"), (3, "vertex4")],
            "p14_set_vertex_vertex4": [1, 2, 3]
        }

        res = self.conn.runInstalledQuery("query4_all_param_types", params)
        self.assertIsInstance(res, list)
        self.assertIsInstance(res[4], dict)
        self.assertIn("p05_string", res[4])
        self.assertEqual(params["p05_string"], res[4]["p05_string"])
        self.assertIsInstance(res[11], dict)
        vs = res[11]
        self.assertIn("p13_set_vertex", vs)
        vs = sorted(vs["p13_set_vertex"])
        self.assertIsInstance(vs, list)
        self.assertEqual(["1", "2", "3"], vs)

    def test_04_runInterpretedQuery(self):
        queryText = \
            """INTERPRET QUERY () FOR GRAPH $graphname {
  SumAccum<INT> @@summa;
  start = {vertex4.*};
  res =
    SELECT src
    FROM   start:src
    ACCUM  @@summa += src.a01;
  PRINT @@summa AS ret;
}"""
        res = self.conn.runInterpretedQuery(queryText)
        self.assertIn("ret", res[0])
        self.assertEqual(15, res[0]["ret"])

        queryText = \
            """INTERPRET QUERY () FOR GRAPH @graphname@ {
  SumAccum<INT> @@summa;
  start = {vertex4.*};
  res =
    SELECT src
    FROM   start:src
    ACCUM  @@summa += src.a01;
  PRINT @@summa AS ret;
}"""
        res = self.conn.runInterpretedQuery(queryText)
        self.assertIn("ret", res[0])
        self.assertEqual(15, res[0]["ret"])

    def test_05_runInstalledQueryAsync(self):
        q_id = self.conn.runInstalledQuery("query1", runAsync=True)
        trials = 0
        while trials < 30:
            job = self.conn.checkQueryStatus(q_id)[0]
            if job["status"] == "success":
                break
            sleep(1)
            trials += 1
        res = self.conn.getQueryResult(q_id)
        self.assertIn("ret", res[0])
        self.assertEqual(15, res[0]["ret"])

    def test_06_checkQueryStatus(self):
        q_id = self.conn.runInstalledQuery("query1", runAsync=True)
        print(q_id)
        res = self.conn.checkQueryStatus(q_id)
        self.assertIn("requestid", res[0])
        self.assertEqual(q_id, res[0]["requestid"])

    def test_07_showQuery(self):
        query = self.conn.showQuery("query1").split("\n")[1]
        q1 = """# installed v2"""
        self.assertEqual(q1, query)

    def test_08_getQueryMetadata(self):
        query_md = self.conn.getQueryMetadata("query1")
        self.assertEqual(query_md["output"][0], {"ret": "int"})

    def test_09_getRunningQueries(self):
        rq_id = self.conn.getRunningQueries()["results"]
        self.assertEqual(len(rq_id), 0)

    def test_10_abortQuery(self):
        abort_ret = self.conn.abortQuery("all")
        self.assertEqual(abort_ret["results"], [{'aborted_queries': []}])

    def test_11_queryDescriptions(self):
        version = self.conn.getVer().split('.')
        if version[0] >= "4":  # Query descriptions only supported in Tigergraph versions >= 4.x
            self.conn.dropQueryDescription('query1')
            desc = self.conn.getQueryDescription('query1')
            self.assertEqual(desc, [{'queryName': 'query1', 'parameters': []}])
            self.conn.describeQuery('query1', 'This is a description')
            desc = self.conn.getQueryDescription('query1')
            self.assertEqual(desc[0]['description'], 'This is a description')

            self.conn.dropQueryDescription('query4_all_param_types')
            self.conn.describeQuery('query4_all_param_types', 'this is a query description',
                                    {'p01_int': 'this is a parameter description',
                                     'p02_uint': 'this is a second param desc'})
            desc = self.conn.getQueryDescription('query4_all_param_types')
            self.assertEqual(desc[0]['description'],
                             'this is a query description')
            self.assertEqual(
                desc[0]['parameters'][0]['description'], 'this is a parameter description')
            self.assertEqual(desc[0]['parameters'][1]
                             ['description'], 'this is a second param desc')

        else:
            with self.assertRaises(TigerGraphException) as tge:
                res = self.conn.dropQueryDescription('query1')
            self.assertEqual(
                "This function is only supported on versions of TigerGraph >= 4.0.0.", tge.exception.message)
            with self.assertRaises(TigerGraphException) as tge:
                res = self.conn.describeQuery('query1', 'test')
            self.assertEqual(
                "This function is only supported on versions of TigerGraph >= 4.0.0.", tge.exception.message)
            with self.assertRaises(TigerGraphException) as tge:
                res = self.conn.getQueryDescription('query1')
            self.assertEqual(
                "This function is only supported on versions of TigerGraph >= 4.0.0.", tge.exception.message)

    def test_installQueries(self):
        # First create the queries using gsql
        queries = [
            """
            CREATE QUERY test_install_query() {
                PRINT "Hello World";
            }
            """,
            """
            CREATE QUERY test_install_query1() {
                PRINT "Hello World 1";
            }
            """,
            """
            CREATE QUERY test_install_query2() {
                PRINT "Hello World 2";
            }
            """,
            """
            CREATE QUERY test_install_query_with_flag() {
                PRINT "Hello World";
            }
            """,
            """
            CREATE QUERY test_install_query_with_multiple_flags() {
                PRINT "Hello World";
            }
            """
        ]
        
        # Create all queries first
        for query in queries:
            self.conn.gsql(query)

        # Test installing a single query
        requestId = self.conn.installQueries("test_install_query")
        self.assertIsInstance(requestId, str)
        
        # Check installation status
        status = self.conn.getQueryInstallationStatus(requestId)
        self.assertIn("message", status)
        self.assertIn(status["message"], "success")

        # Test installing multiple queries
        requestId = self.conn.installQueries(["test_install_query1", "test_install_query2"])
        self.assertIsInstance(requestId, str)
        
        # Check installation status
        status = self.conn.getQueryInstallationStatus(requestId)
        self.assertIn("message", status)
        self.assertIn(status["message"], "SUCCESS")

        # Test installing with flags
        requestId = self.conn.installQueries("test_install_query_with_flag", flag="-force")
        self.assertIsInstance(requestId, str)
        
        # Check installation status
        status = self.conn.getQueryInstallationStatus(requestId)
        self.assertIn("message", status)
        self.assertIn(status["message"], "SUCCESS")

        # Test installing with multiple flags
        requestId = self.conn.installQueries("test_install_query_with_multiple_flags", flag=["-force", "-debug"])
        self.assertIsInstance(requestId, str)
        
        # Check installation status
        status = self.conn.getQueryInstallationStatus(requestId)
        self.assertIn("message", status)
        self.assertIn(status["message"], "SUCCESS")

        # Test installing all queries
        requestId = self.conn.installQueries("all")
        self.assertIsInstance(requestId, str)
        
        # Check installation status
        status = self.conn.getQueryInstallationStatus(requestId)
        self.assertIn("message", status)
        self.assertIn(status["message"], "SUCCESS")

        # Test installing all queries with asterisk
        requestId = self.conn.installQueries("*")
        self.assertIsInstance(requestId, str)
        
        # Check installation status
        status = self.conn.getQueryInstallationStatus(requestId)
        self.assertIn("message", status)
        self.assertIn(status["message"], "SUCCESS")

        # Test invalid query name
        with self.assertRaises(ValueError):
            self.conn.installQueries("non_existent_query")

    def test_getQueryContent(self):
        """Test getQueryContent function."""
        # Test getting content of an existing query
        res = self.conn.getQueryContent("query1")
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)

    def test_createQuery(self):
        """Test createQuery function."""
        # Test creating a simple query
        query_text = """
        CREATE QUERY testCreateQuery() FOR GRAPH $graphname {
            PRINT "Hello World";
        }
        """
        res = self.conn.createQuery(query_text)
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)
        self.assertIn("message", res)

    def test_dropQueries(self):
        """Test dropQueries function."""
        # Test dropping a single query
        res = self.conn.dropQueries("testCreateQuery")
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)

        # Test dropping multiple queries (use queries that don't exist to test error handling)
        # This should return an error response but still be a valid dict
        res = self.conn.dropQueries(["testQuery1", "testQuery2"])
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)

        # Test invalid input
        with self.assertRaises(Exception):
            self.conn.dropQueries(123)  # Should be string or list

    def test_listQueryNames(self):
        """Test listQueryNames function."""
        res = self.conn.listQueryNames()
        self.assertIsInstance(res, list)

    def test_checkQuerySemantic(self):
        """Test checkQuerySemantic function."""
        # Test valid query
        valid_query = """
        CREATE QUERY testSemanticQuery() {
            PRINT "Hello World";
        }
        """
        res = self.conn.checkQuerySemantic(valid_query)
        self.assertIsInstance(res, dict)
        self.assertIn("warnings", res)
        self.assertIn("errors", res)

        # Test invalid query
        invalid_query = "INVALID GSQL SYNTAX"
        res = self.conn.checkQuerySemantic(invalid_query)
        self.assertIsInstance(res, dict)
        self.assertIn("warnings", res)
        self.assertIn("errors", res)

    def test_getQueryInfo(self):
        """Test getQueryInfo function."""
        # Test getting info for all queries
        res = self.conn.getQueryInfo()
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)
        self.assertIn("results", res)

        # Test getting info for specific query
        res = self.conn.getQueryInfo(queryName="query1")
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)

        # Test getting info with status filter
        res = self.conn.getQueryInfo(status="VALID")
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)


if __name__ == '__main__':
    unittest.main()
