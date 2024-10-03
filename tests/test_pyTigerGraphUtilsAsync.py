import re
import unittest
from datetime import datetime

from pyTigerGraph.common.util import _safe_char

from pyTigerGraphUnitTestAsync import make_connection

from pyTigerGraph.common.exception import TigerGraphException


class test_pyTigerGraphUtilsAsync(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.conn = await make_connection()

    def test_01_safeChar(self):
        res = _safe_char(" _space")
        self.assertEqual("%20_space", res)
        res = _safe_char("/_slash")
        self.assertEqual("%2F_slash", res)
        res = _safe_char("ñ_LATIN_SMALL_LETTER_N_WITH_TILDE")
        self.assertEqual(res, '%C3%B1_LATIN_SMALL_LETTER_N_WITH_TILDE')
        res = _safe_char(12345)
        self.assertEqual("12345", res)
        res = _safe_char(12.345)
        self.assertEqual("12.345", res)
        now = datetime.now()
        res = _safe_char(now)
        exp = str(now).replace(" ", "%20").replace(":", "%3A")
        self.assertEqual(exp, res)
        res = _safe_char(True)
        self.assertEqual("True", res)

    async def test_02_echo(self):
        res = await self.conn.echo()
        self.assertIsInstance(res, str)
        self.assertEqual("Hello GSQL", res)
        res = await self.conn.echo(True)
        self.assertIsInstance(res, str)
        self.assertEqual("Hello GSQL", res)

    async def test_03_getVersion(self):
        res = await self.conn.getVersion()
        self.assertIsInstance(res, list)
        self.assertGreater(len(res), 0)

    async def test_04_getVer(self):
        res = await self.conn.getVer()
        self.assertIsInstance(res, str)
        m = re.match(r"[0-9]+\.[0-9]+\.[0-9]", res)
        self.assertIsNotNone(m)

    async def test_05_ping(self):
        res = await self.conn.ping()
        self.assertIsInstance(res, dict)
        self.assertEqual(res["message"], "pong")

    async def test_06_getSystemMetrics(self):
        if await self.conn._version_greater_than_4_0():
            res = await self.conn.getSystemMetrics(what="cpu-memory")
            self.assertIn("CPUMemoryMetrics", res)
            res = await self.conn.getSystemMetrics(what="diskspace")
            self.assertIn("DiskMetrics", res)
            res = await self.conn.getSystemMetrics(what="network")
            self.assertIn("NetworkMetrics", res)
            res = await self.conn.getSystemMetrics(what="qps")
            self.assertIn("QPSMetrics", res)

            with self.assertRaises(TigerGraphException) as tge:
                res = await self.conn.getSystemMetrics(what="servicestate")
            self.assertEqual(
                "This 'what' parameter is only supported on versions of TigerGraph < 4.1.0.", tge.exception.message)

            with self.assertRaises(TigerGraphException) as tge:
                res = await self.conn.getSystemMetrics(what="connection")
            self.assertEqual(
                "This 'what' parameter is only supported on versions of TigerGraph < 4.1.0.", tge.exception.message)
        else:
            res = await self.conn.getSystemMetrics(what="mem", latest=10)
            self.assertEqual(len(res), 10)
    ''' Commented out because the queries are not completed yet
    async def test_07_getQueryPerformance(self):
        res = await self.conn.getQueryPerformance()
        self.assertIn("CompletedRequests", str(res))
    '''
    async def test_08_getServiceStatus(self):
        req = {"ServiceDescriptors": [{"ServiceName": "GSQL"}]}
        res = await self.conn.getServiceStatus(req)
        self.assertEqual(res["ServiceStatusEvents"][0]
                         ["ServiceStatus"], "Online")

    async def test_09_rebuildGraph(self):
        res = await self.conn.rebuildGraph()
        self.assertEqual(
            res["message"], "RebuildNow finished, please check details in the folder: /tmp/rebuildnow")


if __name__ == '__main__':
    unittest.main()
