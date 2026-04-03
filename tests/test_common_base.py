"""Unit tests for pyTigerGraph.common.base (PyTigerGraphCore).

These tests run without a live TigerGraph server by mocking network calls.
They guard against init-ordering bugs where _cached_auth is accessed
before _refresh_auth_headers() has been called.
"""

import unittest
from unittest.mock import MagicMock, patch

from pyTigerGraph import TigerGraphConnection


def _make_conn(**kwargs):
    """Create a TigerGraphConnection without any real network calls."""
    defaults = dict(
        host="http://127.0.0.1",
        graphname="tests",
        username="tigergraph",
        password="tigergraph",
    )
    defaults.update(kwargs)
    with patch.object(TigerGraphConnection, "_verify_jwt_token_support", return_value=None):
        conn = TigerGraphConnection(**defaults)
    return conn


class TestRefreshAuthHeadersOrdering(unittest.TestCase):
    """_refresh_auth_headers() must be called before any _get()/_req() in __init__.

    Regression test for GML-2041 ordering bug:
      _cached_auth was set AFTER _verify_jwt_token_support() (and the
      tgCloud ping), causing AttributeError swallowed as a JWT error message.
    """

    def test_cached_auth_set_with_username_password(self):
        conn = _make_conn()
        self.assertTrue(hasattr(conn, "_cached_auth"))
        self.assertIn("Basic ", conn._cached_auth["Authorization"])

    def test_cached_auth_set_with_api_token(self):
        conn = _make_conn(apiToken="myapitoken123")
        self.assertIn("Bearer myapitoken123", conn._cached_auth["Authorization"])

    def test_cached_auth_set_with_jwt_token(self):
        """Regression: jwtToken must not cause AttributeError during __init__."""
        conn = _make_conn(jwtToken="header.payload.signature")
        self.assertIn("Bearer header.payload.signature", conn._cached_auth["Authorization"])

    def test_jwt_token_calls_verify(self):
        """_verify_jwt_token_support() must be called when jwtToken is provided."""
        with patch.object(TigerGraphConnection, "_verify_jwt_token_support") as mock_verify:
            TigerGraphConnection(
                host="http://127.0.0.1",
                jwtToken="header.payload.signature",
            )
        mock_verify.assert_called_once()

    def test_no_jwt_skips_verify(self):
        """_verify_jwt_token_support() must NOT be called without jwtToken."""
        with patch.object(TigerGraphConnection, "_verify_jwt_token_support") as mock_verify:
            TigerGraphConnection(host="http://127.0.0.1")
        mock_verify.assert_not_called()

    def test_tgcloud_ping_does_not_crash_without_jwt(self):
        """tgCloud _get() ping fires before _verify_jwt_token_support; must not AttributeError."""
        with patch.object(TigerGraphConnection, "_get", return_value="pong") as mock_get:
            conn = TigerGraphConnection(host="http://my.tgcloud.io")
        # _cached_auth must exist at the point _get() was called
        self.assertTrue(hasattr(conn, "_cached_auth"))

    def test_tgcloud_ping_does_not_crash_with_jwt(self):
        """tgCloud ping + JWT verification both fire; _cached_auth must precede both."""
        with patch.object(TigerGraphConnection, "_get", return_value="pong"):
            with patch.object(TigerGraphConnection, "_verify_jwt_token_support"):
                conn = TigerGraphConnection(
                    host="http://my.tgcloud.io",
                    jwtToken="header.payload.signature",
                )
        self.assertIn("Bearer header.payload.signature", conn._cached_auth["Authorization"])

    def test_x_user_agent_header_present(self):
        """X-User-Agent must be baked into cached auth dict."""
        conn = _make_conn()
        self.assertEqual(conn._cached_auth.get("X-User-Agent"), "pyTigerGraph")


class TestRefreshAuthHeadersUpdate(unittest.TestCase):
    """_refresh_auth_headers() must update the cache after credentials change."""

    def test_refresh_after_get_token(self):
        conn = _make_conn()
        self.assertIn("Basic ", conn._cached_auth["Authorization"])

        conn.apiToken = "newtoken456"
        conn._refresh_auth_headers()

        self.assertIn("Bearer newtoken456", conn._cached_auth["Authorization"])

    def test_refresh_clears_old_token(self):
        conn = _make_conn(apiToken="oldtoken")
        self.assertIn("Bearer oldtoken", conn._cached_auth["Authorization"])

        conn.apiToken = ""
        conn._refresh_auth_headers()

        self.assertIn("Basic ", conn._cached_auth["Authorization"])


# ──────────────────────────────────────────────────────────────────────
# _token_source tracking
# ──────────────────────────────────────────────────────────────────────

class TestTokenSource(unittest.TestCase):
    """_token_source tracks whether the token was user-provided or generated."""

    def test_no_token_source_is_none(self):
        conn = _make_conn()
        self.assertIsNone(conn._token_source)

    def test_api_token_source_is_user(self):
        conn = _make_conn(apiToken="usertoken")
        self.assertEqual(conn._token_source, "user")

    def test_jwt_token_source_is_user(self):
        conn = _make_conn(jwtToken="header.payload.signature")
        self.assertEqual(conn._token_source, "user")

    def test_get_token_sets_source_to_generated(self):
        conn = _make_conn()
        self.assertIsNone(conn._token_source)

        with patch.object(conn, "_token", return_value=({"token": "newtoken"}, "4")):
            conn.getToken()

        self.assertEqual(conn._token_source, "generated")

    def test_get_token_overrides_user_source(self):
        conn = _make_conn(apiToken="usertoken")
        self.assertEqual(conn._token_source, "user")

        with patch.object(conn, "_token", return_value=({"token": "newtoken"}, "4")):
            conn.getToken()

        self.assertEqual(conn._token_source, "generated")


# ──────────────────────────────────────────────────────────────────────
# Auto-refresh on 401
# ──────────────────────────────────────────────────────────────────────

class TestAutoRefreshOn401(unittest.TestCase):
    """Token auto-refresh on 401 for generated tokens; error for user tokens."""

    def _mock_response(self, status_code=200, content=b'{"results": "ok"}'):
        resp = MagicMock()
        resp.status_code = status_code
        resp.content = content
        resp.raise_for_status = MagicMock()
        if status_code >= 400:
            import requests
            resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
                response=resp)
        return resp

    def test_401_with_generated_token_auto_refreshes(self):
        conn = _make_conn()
        conn._token_source = "generated"
        conn.restppPort = "9000"

        resp_401 = self._mock_response(401, b'{"message": "token expired"}')
        resp_200 = self._mock_response(200, b'{"results": "ok"}')

        with patch.object(conn, "_do_request", side_effect=[resp_401, resp_200]) as mock_do, \
             patch.object(conn, "getToken", return_value="newtoken") as mock_get_token:
            result = conn._req("GET", "http://127.0.0.1:9000/query/test")

        mock_get_token.assert_called_once()
        self.assertEqual(mock_do.call_count, 2)
        self.assertEqual(result, "ok")

    def test_401_with_user_token_raises(self):
        import requests
        conn = _make_conn(apiToken="usertoken")
        conn.restppPort = "9000"

        resp_401 = self._mock_response(401, b'{"message": "token expired"}')

        with patch.object(conn, "_do_request", return_value=resp_401):
            with self.assertRaises(requests.exceptions.HTTPError):
                conn._req("GET", "http://127.0.0.1:9000/query/test")

    def test_401_with_no_token_raises(self):
        import requests
        conn = _make_conn()
        conn.restppPort = "9000"

        resp_401 = self._mock_response(401, b'{"message": "unauthorized"}')

        with patch.object(conn, "_do_request", return_value=resp_401):
            with self.assertRaises(requests.exceptions.HTTPError):
                conn._req("GET", "http://127.0.0.1:9000/query/test")

    def test_non_401_error_not_refreshed(self):
        import requests
        conn = _make_conn()
        conn._token_source = "generated"
        conn.restppPort = "9000"

        resp_500 = self._mock_response(500, b'{"message": "server error"}')

        with patch.object(conn, "_do_request", return_value=resp_500), \
             patch.object(conn, "getToken") as mock_get_token:
            with self.assertRaises(requests.exceptions.HTTPError):
                conn._req("GET", "http://127.0.0.1:9000/query/test")

        mock_get_token.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
