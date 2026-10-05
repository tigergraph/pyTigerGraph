"""Regression tests for using an AsyncTigerGraphConnection across event loops.

The HTTP session and the asyncio locks are bound to the event loop that created
them and cannot move. A connection must therefore cope with two shapes:

  * sequential loops -- asyncio.run() closes its loop on return, so the next
    call runs on a new one (GML-2183);
  * concurrent loops -- two threads each running their own loop against one
    shared connection, which must not tear down each other's session.
"""

import asyncio
import json
import threading
import unittest
import warnings
from http.server import BaseHTTPRequestHandler, HTTPServer

import aiohttp

from pyTigerGraph import AsyncTigerGraphConnection


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"error": False, "results": "pong"}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class _ServerCase(unittest.TestCase):
    """Base case providing a local HTTP server and connection factory."""

    def setUp(self):
        self.server = HTTPServer(("127.0.0.1", 0), _Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.shutdown)
        self.port = self.server.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}/echo"

    def conn(self):
        # apiToken short-circuits token minting, so no TigerGraph is needed.
        return AsyncTigerGraphConnection(
            host="http://127.0.0.1", apiToken="dummy",
            restppPort=str(self.port), gsPort="14240")

    async def _call(self, conn):
        status, body, _ = await conn._do_request(
            "GET", self.url, {}, None, False, None,
            aiohttp.ClientTimeout(total=10))
        return status, json.loads(body)["results"]


class TestSequentialLoops(_ServerCase):
    """A connection outliving its event loop, the asyncio.run() pattern."""

    def test_request_succeeds_on_later_loop(self):
        conn = self.conn()
        with warnings.catch_warnings():
            warnings.simplefilter("error", ResourceWarning)
            self.assertEqual(asyncio.run(self._call(conn)), (200, "pong"))
            self.assertEqual(asyncio.run(self._call(conn)), (200, "pong"))
        asyncio.run(conn.aclose())

    def test_new_binding_per_loop(self):
        conn = self.conn()

        async def touch():
            b = conn._binding()
            return b, asyncio.get_running_loop()

        first, loop_a = asyncio.run(touch())
        second, loop_b = asyncio.run(touch())

        self.assertIsNot(loop_a, loop_b)
        self.assertIsNot(first, second)
        self.assertIsNot(first.client, second.client)
        self.assertIsNot(first.token_refresh_lock, second.token_refresh_lock)

    def test_binding_reused_within_one_loop(self):
        conn = self.conn()

        async def twice():
            return conn._binding(), conn._binding()

        first, second = asyncio.run(twice())
        self.assertIs(first, second)

    def test_locks_usable_on_each_loop(self):
        """A lock bound to a dead loop would raise when awaited on a new one."""
        conn = self.conn()

        async def acquire():
            b = conn._binding()
            async with b.token_refresh_lock:
                pass
            async with b.restpp_failover_lock:
                pass

        asyncio.run(acquire())
        asyncio.run(acquire())  # would raise "bound to a different event loop"

    def test_stale_session_not_reported_as_leaked(self):
        conn = self.conn()

        async def touch():
            return conn._binding().client

        stale = asyncio.run(touch())
        self.assertFalse(stale.closed)  # aiohttp does not notice the loop died
        asyncio.run(touch())
        self.assertTrue(stale.closed)

    def test_closed_loops_pruned_from_registry(self):
        """The registry must not grow by one entry per asyncio.run() call."""
        conn = self.conn()

        async def touch():
            conn._binding()

        for _ in range(5):
            asyncio.run(touch())
        self.assertEqual(len(conn._loop_bindings), 1)


class TestConcurrentLoops(_ServerCase):
    """Separate threads, each with its own loop, sharing one connection."""

    def test_threads_do_not_close_each_others_session(self):
        conn = self.conn()
        results, errors = [], []

        def worker():
            try:
                for _ in range(8):
                    results.append(asyncio.run(self._call(conn)))
            except Exception as e:  # noqa: BLE001 - recorded and asserted below
                errors.append(f"{type(e).__name__}: {e}")

        threads = [threading.Thread(target=worker) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        self.assertEqual(results, [(200, "pong")] * 24)

    def test_concurrent_first_use_on_one_loop_shares_a_session(self):
        """_binding() performs no await, so racing tasks cannot double-create."""
        conn = self.conn()

        async def many():
            out = await asyncio.gather(*(self._call(conn) for _ in range(20)))
            return out, len(conn._loop_bindings)

        results, bindings = asyncio.run(many())
        self.assertEqual(results, [(200, "pong")] * 20)
        self.assertEqual(bindings, 1)


class TestClose(_ServerCase):
    """aclose() and __del__ across loop boundaries."""

    def test_aclose_on_owning_loop(self):
        conn = self.conn()

        async def run():
            await self._call(conn)
            client = conn._binding().client
            await conn.aclose()
            return client

        client = asyncio.run(run())
        self.assertTrue(client.closed)
        self.assertEqual(conn._loop_bindings, {})

    def test_aclose_from_a_foreign_loop(self):
        """Awaiting close() on the wrong loop would fail; it must be disposed instead."""
        conn = self.conn()
        asyncio.run(self._call(conn))
        stale = next(iter(conn._loop_bindings.values())).client

        asyncio.run(conn.aclose())

        self.assertTrue(stale.closed)
        self.assertEqual(conn._loop_bindings, {})

    def test_del_releases_every_binding(self):
        conn = self.conn()
        asyncio.run(self._call(conn))
        clients = [b.client for b in conn._loop_bindings.values()]
        self.assertTrue(clients)

        conn.__del__()

        self.assertTrue(all(c.closed for c in clients))

    def test_reusable_after_aclose(self):
        conn = self.conn()
        asyncio.run(self._call(conn))
        asyncio.run(conn.aclose())
        self.assertEqual(asyncio.run(self._call(conn)), (200, "pong"))
        asyncio.run(conn.aclose())


if __name__ == "__main__":
    unittest.main()
