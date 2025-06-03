import unittest
from datetime import datetime
from time import sleep

from pyTigerGraphUnitTestAsync import make_connection

from pyTigerGraph.common.exception import TigerGraphException


class test_pyTigerGraphQueryAsync(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.conn = await make_connection()

    async def test_01_getQueries(self):
        # TODO Once pyTigerGraphQuery.getQueries() is available
        pass

    async def test_02_getInstalledQueries(self):
        res = await self.conn.getInstalledQueries()
        self.assertIn("GET /query/tests/query1", res)
        #self.assertNotIn("GET /query/tests/query2_not_installed", res)
        self.assertIn("GET /query/tests/query3_installed", res)

    async def test_03_runInstalledQuery(self):
        res = await self.conn.runInstalledQuery("query1")
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

        res = await self.conn.runInstalledQuery("query4_all_param_types", params)
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

    async def test_04_runInterpretedQuery(self):
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
        res = await self.conn.runInterpretedQuery(queryText)
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
        res = await self.conn.runInterpretedQuery(queryText)
        self.assertIn("ret", res[0])
        self.assertEqual(15, res[0]["ret"])

    async def test_05_runInstalledQueryAsync(self):
        q_id = await self.conn.runInstalledQuery("query1", runAsync=True)
        trials = 0
        while trials < 30:
            job = await self.conn.checkQueryStatus(q_id)
            job = job[0]
            if job["status"] == "success":
                break
            sleep(1)
            trials += 1
        res = await self.conn.getQueryResult(q_id)
        self.assertIn("ret", res[0])
        self.assertEqual(15, res[0]["ret"])

    async def test_06_checkQueryStatus(self):
        q_id = await self.conn.runInstalledQuery("query1", runAsync=True)
        print(q_id)
        res = await self.conn.checkQueryStatus(q_id)
        self.assertIn("requestid", res[0])
        self.assertEqual(q_id, res[0]["requestid"])

    async def test_07_showQuery(self):
        query = await self.conn.showQuery("query1")
        query = query.split("\n")[1]
        q1 = """# installed v2"""
        self.assertEqual(q1, query)

    async def test_08_getQueryMetadata(self):
        query_md = await self.conn.getQueryMetadata("query1")
        self.assertEqual(query_md["output"][0], {"ret": "int"})

    async def test_09_getRunningQueries(self):
        rq_id = await self.conn.getRunningQueries()
        rq_id = rq_id["results"]
        self.assertEqual(len(rq_id), 0)

    async def test_10_abortQuery(self):
        abort_ret = await self.conn.abortQuery("all")
        self.assertEqual(abort_ret["results"], [{'aborted_queries': []}])

    async def test_11_queryDescriptions(self):
        version = await self.conn.getVer()
        version = version.split('.')
        if version[0] >= "4":  # Query descriptions only supported in Tigergraph versions >= 4.x
            await self.conn.dropQueryDescription('query1')
            desc = await self.conn.getQueryDescription('query1')
            self.assertEqual(desc, [{'queryName': 'query1', 'parameters': []}])
            await self.conn.describeQuery('query1', 'This is a description')
            desc = await self.conn.getQueryDescription('query1')
            self.assertEqual(desc[0]['description'], 'This is a description')

            await self.conn.dropQueryDescription('query4_all_param_types')
            await self.conn.describeQuery('query4_all_param_types', 'this is a query description',
                                          {'p01_int': 'this is a parameter description',
                                           'p02_uint': 'this is a second param desc'})
            desc = await self.conn.getQueryDescription('query4_all_param_types')
            self.assertEqual(desc[0]['description'],
                             'this is a query description')
            self.assertEqual(
                desc[0]['parameters'][0]['description'], 'this is a parameter description')
            self.assertEqual(desc[0]['parameters'][1]
                             ['description'], 'this is a second param desc')

        else:
            with self.assertRaises(TigerGraphException) as tge:
                res = await self.conn.dropQueryDescription('query1')
            self.assertEqual(
                "This function is only supported on versions of TigerGraph >= 4.0.0.", tge.exception.message)
            with self.assertRaises(TigerGraphException) as tge:
                res = await self.conn.describeQuery('query1', 'test')
            self.assertEqual(
                "This function is only supported on versions of TigerGraph >= 4.0.0.", tge.exception.message)
            with self.assertRaises(TigerGraphException) as tge:
                res = await self.conn.getQueryDescription('query1')
            self.assertEqual(
                "This function is only supported on versions of TigerGraph >= 4.0.0.", tge.exception.message)

    async def test_04_installQueries(self):
        # First create the queries using gsql
        queries = [
            """
            CREATE QUERY test_install_query_async() {
                PRINT "Hello World";
            }
            """,
            """
            CREATE QUERY test_install_query1_async() {
                PRINT "Hello World 1";
            }
            """,
            """
            CREATE QUERY test_install_query2_async() {
                PRINT "Hello World 2";
            }
            """,
            """
            CREATE QUERY test_install_query_with_flag_async() {
                PRINT "Hello World";
            }
            """,
            """
            CREATE QUERY test_install_query_with_multiple_flags_async() {
                PRINT "Hello World";
            }
            """
        ]
        
        # Create all queries first
        for query in queries:
            await self.conn.gsql(query)

        # Test installing a single query
        result = await self.conn.installQueries("test_install_query_async")
        self.assertIn("SUCCESS", result)
        
        # Test installing multiple queries
        result = await self.conn.installQueries(["test_install_query1_async", "test_install_query2_async"])
        self.assertIn("SUCCESS", result)

        # Test installing with flags
        result = await self.conn.installQueries("test_install_query_with_flag_async", flag="-force")
        self.assertIn("SUCCESS", result)

        # Test installing with multiple flags
        result = await self.conn.installQueries("test_install_query_with_multiple_flags_async", flag=["-force", "-debug"])
        self.assertIn("SUCCESS", result)

        # Test installing all queries
        result = await self.conn.installQueries("all")
        self.assertIn("SUCCESS", result)

        # Test installing all queries with asterisk
        result = await self.conn.installQueries("*")
        self.assertIn("SUCCESS", result)

        # Test invalid query name
        with self.assertRaises(ValueError):
            await self.conn.installQueries("non_existent_query")


if __name__ == '__main__':
    unittest.main()
