import unittest

from pyTigerGraphUnitTest import make_connection

from pyTigerGraph.pyTigerGraphException import TigerGraphException


class test_pyTigerGraphPath(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection()

    def test_01_getSecrets(self):
        res = self.conn.showSecrets()
        self.assertIsInstance(res, dict)
        self.assertEqual(3, len(res))
        self.assertIn("secret1", res)
        self.assertIn("secret2", res)
        self.assertIn("secret2", res)

    def test_02_getSecret(self):
        pass
        # TODO Implement

    def test_03_createSecret(self):
        res = self.conn.createSecret("secret4")
        self.assertIsInstance(res, str)

        res = self.conn.createSecret("secret5", True)
        self.assertIsInstance(res, dict)
        self.assertEqual(1, len(res))
        alias = list(res.keys())[0]
        self.assertEqual("secret5", alias)

        res = self.conn.createSecret(withAlias=True)
        self.assertIsInstance(res, dict)
        self.assertEqual(1, len(res))
        alias = list(res.keys())[0]
        self.assertTrue(alias.startswith("AUTO_GENERATED_ALIAS_"))

        with self.assertRaises(TigerGraphException) as tge:
            self.conn.createSecret("secret1")
        self.assertEqual("The secret with alias secret1 already exists.", tge.exception.message)

    def test_04_dropSecret(self):
        res = self.conn.showSecrets()
        for a in list(res.keys()):
            if a.startswith("AUTO_GENERATED_ALIAS"):
                res = self.conn.dropSecret(a)
                self.assertTrue("Successfully dropped secrets" in res)

        res = self.conn.dropSecret(["secret4", "secret5"])
        self.assertTrue("Failed to drop secrets" not in res)

        res = self.conn.dropSecret("non_existent_secret")
        self.assertTrue("Failed to drop secrets" in res)

        with self.assertRaises(TigerGraphException) as tge:
            res = self.conn.dropSecret("non_existent_secret", False)

    def test_05_getToken(self):
        res = self.conn.createSecret("secret5", True)
        token = self.conn.getToken(res["secret5"])
        self.assertIsInstance(token, tuple)
        self.conn.dropSecret("secret5")

    def test_06_refreshToken(self):
        res = self.conn.createSecret("secret6", True)
        token = self.conn.getToken(res["secret6"])
        refreshed = self.conn.refreshToken(res["secret6"], token[0])
        self.assertIsInstance(refreshed, tuple)
        self.conn.dropSecret("secret6")

    def test_07_deleteToken(self):
        res = self.conn.createSecret("secret7", True)
        token = self.conn.getToken(res["secret7"])
        self.assertTrue(self.conn.deleteToken(res["secret7"], token[0]))
        self.conn.dropSecret("secret7")

if __name__ == '__main__':
    unittest.main()
