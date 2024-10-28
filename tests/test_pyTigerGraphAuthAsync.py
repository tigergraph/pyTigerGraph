import unittest

from pyTigerGraphUnitTestAsync import make_connection

from pyTigerGraph.common.exception import TigerGraphException


class test_pyTigerGraphAuthAsync(unittest.IsolatedAsyncioTestCase):
    @classmethod
    async def asyncSetUp(self):
        self.conn = await make_connection()

    async def test_01_getSecrets(self):
        res = await self.conn.showSecrets()
        self.assertIsInstance(res, dict)
        # self.assertEqual(3, len(res)) # Just in case more secrets than expected
        self.assertIn("secret1", res)
        self.assertIn("secret2", res)
        self.assertIn("secret2", res)

    async def test_02_getSecret(self):
        pass
        # TODO Implement

    async def test_03_createSecret(self):
        res = await self.conn.createSecret("secret4")
        self.assertIsInstance(res, str)

        res = await self.conn.createSecret("secret5", True)
        self.assertIsInstance(res, dict)
        self.assertEqual(1, len(res))
        alias = list(res.keys())[0]
        self.assertEqual("secret5", alias)

        res = await self.conn.createSecret(withAlias=True)
        self.assertIsInstance(res, dict)
        self.assertEqual(1, len(res))
        alias = list(res.keys())[0]
        self.assertTrue(alias.startswith("AUTO_GENERATED_ALIAS_"))

        with self.assertRaises(TigerGraphException) as tge:
            await self.conn.createSecret("secret1")
        self.assertEqual(
            "The secret with alias secret1 already exists.", tge.exception.message)

    async def test_04_dropSecret(self):
        res = await self.conn.showSecrets()
        for a in list(res.keys()):
            if a.startswith("AUTO_GENERATED_ALIAS"):
                res = await self.conn.dropSecret(a)
                self.assertTrue("Successfully dropped secrets" in res)

        res = await self.conn.dropSecret(["secret4", "secret5"])
        self.assertTrue("Failed to drop secrets" not in res)

        res = await self.conn.dropSecret("non_existent_secret")
        self.assertTrue("Failed to drop secrets" in res)

        with self.assertRaises(TigerGraphException) as tge:
            res = await self.conn.dropSecret("non_existent_secret", False)

    async def test_05_getToken(self):
        res = await self.conn.createSecret("secret5", True)
        token = await self.conn.getToken(res["secret5"])
        self.assertIsInstance(token, tuple)
        await self.conn.dropSecret("secret5")
    '''
    async def test_06_refreshToken(self):
        # TG 4.x does not allow refreshing tokens
        await self.conn.getToken(await self.conn.createSecret())
        if await self.conn._version_greater_than_4_0():
            with self.assertRaises(TigerGraphException) as tge:
                await self.conn.refreshToken("secret1")
            self.assertEqual(
                "Refreshing tokens is only supported on versions of TigerGraph <= 4.0.0.", tge.exception.message)
        else:
            await self.conn.dropSecret("secret6", ignoreErrors=True)
            res = await self.conn.createSecret("secret6", True)
            token = await self.conn.getToken(res["secret6"])
            refreshed = await self.conn.refreshToken(res["secret6"], token[0])
            self.assertIsInstance(refreshed, tuple)
            await self.conn.dropSecret("secret6")
    '''
    async def test_07_deleteToken(self):
        await self.conn.dropSecret("secret7", ignoreErrors=True)
        res = await self.conn.createSecret("secret7", True)
        token = await self.conn.getToken(res["secret7"])
        self.assertTrue(await self.conn.deleteToken(res["secret7"], token[0]))
        await self.conn.dropSecret("secret7")


if __name__ == '__main__':
    unittest.main()
