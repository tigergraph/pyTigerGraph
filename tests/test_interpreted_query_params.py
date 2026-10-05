"""Regression tests for interpreted-query parameter encoding (GML-2310).

Parameter values travel in the URL query string. They must be percent-encoded
exactly once, so the server's single decode yields the original value. The
async client used to pass the already-encoded string to aiohttp as params=,
which encoded it a second time.
"""

import asyncio
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlsplit

from pyTigerGraph import AsyncTigerGraphConnection, TigerGraphConnection

VALUES = {
    "plain": "abc",
    "space": "a b",
    "quotes": "it's \"x\"",
    "amp": "x&y",
    "eq": "k=v",
    "pct": "50%",
    "hash": "#1",
    "plus": "1+1",
    "unicode": "東京",
}


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        self.server.paths.append(self.path)
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        body = json.dumps({"error": False, "message": "", "results": [{}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class TestInterpretedQueryParams(unittest.TestCase):
    def setUp(self):
        self.server = HTTPServer(("127.0.0.1", 0), _Handler)
        self.server.paths = []
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.shutdown)
        self.kwargs = dict(host="http://127.0.0.1", graphname="g", apiToken="dummy",
                           restppPort=str(self.server.server_address[1]),
                           gsPort=str(self.server.server_address[1]))

    def _received(self):
        """The parameters as the server sees them after decoding once."""
        query = urlsplit(self.server.paths[-1]).query
        return {k: v[0] for k, v in parse_qs(query).items()}

    def _run_async(self, params, v4: bool):
        async def run():
            conn = AsyncTigerGraphConnection(**self.kwargs)

            async def version_check():
                return v4
            conn._version_greater_than_4_0 = version_check
            try:
                await conn.runInterpretedQuery("INTERPRET QUERY () {}", params)
            finally:
                await conn.aclose()
        asyncio.run(run())

    def test_async_encodes_values_once(self):
        for v4 in (True, False):
            with self.subTest(v4=v4):
                self._run_async(VALUES, v4)
                self.assertEqual(self._received(), VALUES)

    def test_async_string_params_unchanged(self):
        self._run_async("s=a%20b&n=1", True)
        self.assertEqual(self._received(), {"s": "a b", "n": "1"})

    def test_async_no_params(self):
        self._run_async(None, True)
        self.assertEqual(urlsplit(self.server.paths[-1]).query, "")

    def test_sync_matches_async(self):
        conn = TigerGraphConnection(**self.kwargs)
        conn._version_greater_than_4_0 = lambda: True
        conn.runInterpretedQuery("INTERPRET QUERY () {}", VALUES)
        self.assertEqual(self._received(), VALUES)


if __name__ == "__main__":
    unittest.main()
