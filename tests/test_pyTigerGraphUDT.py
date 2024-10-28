import unittest

from pyTigerGraphUnitTest import make_connection


class test_pyTigerGraphUDT(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection()

    def test_01_getUDTs(self):
        res = self.conn.getUDTs()
        exp = ["tuple1_all_types", "tuple2_simple"]
        self.assertEqual(exp, res)

    def test_02_getUDT(self):
        res = self.conn.getUDT("tuple2_simple")
        self.assertTrue(res[0]['fieldName'] == 'field1')
        self.assertTrue(res[0]['fieldType'] == 'INT')
        self.assertTrue(res[1]['fieldName'] == 'field2')
        self.assertTrue(res[1]['fieldType'] == 'STRING')
        self.assertTrue(res[2]['fieldName'] == 'field3')
        self.assertTrue(res[2]['fieldType'] == 'DATETIME')


if __name__ == '__main__':
    unittest.main()
