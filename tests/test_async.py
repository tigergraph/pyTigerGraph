import asyncio
import unittest
from pyTigerGraphUnitTestAsync import make_connection


class test_async(unittest.IsolatedAsyncioTestCase):
    @classmethod
    async def asyncSetUp(self):
        self.conn = await make_connection()

    '''
    async def test_task_results(self):
        if not hasattr(self, 'conn'):
            raise AttributeError(
                "Connection was not initialized. Please check the setup.")

        tasks: list[asyncio.Task] = []

        async with asyncio.TaskGroup() as tg:
            for i in range(100):
                if i % 2 == 0:
                    task = tg.create_task(self.conn.getVertexCount("vertex7"))
                    tasks.append(task)
                else:
                    task = tg.create_task(
                        self.conn.getEdgeCount("edge1_undirected"))
                    tasks.append(task)

        for t in tasks:
            result = t.result()
            # print(result)
            self.assertIsInstance(result, int)

        '''
if __name__ == "__main__":
    unittest.main()
