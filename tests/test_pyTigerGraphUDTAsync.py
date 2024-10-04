import unittest

from pyTigerGraphUnitTestAsync import make_connection

from pyTigerGraph.common.exception import TigerGraphException


class test_pyTigerGraphUDT(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.conn = await make_connection()

    async def test_01_getUDTs(self):
        res = await self.conn.getUDTs()
        exp = ["tuple1_all_types", "tuple2_simple"]
        self.assertEqual(exp, res)

    async def test_02_getUDT(self):
        res = await self.conn.getUDT("tuple2_simple")
        self.assertTrue(res[0]['fieldName'] == 'field1')
        self.assertTrue(res[0]['fieldType'] == 'INT')
        self.assertTrue(res[1]['fieldName'] == 'field2')
        self.assertTrue(res[1]['fieldType'] == 'STRING')
        self.assertTrue(res[2]['fieldName'] == 'field3')
        self.assertTrue(res[2]['fieldType'] == 'DATETIME')


if __name__ == '__main__':
    unittest.main()
