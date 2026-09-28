"""HttpClient retries, rate limits, pacing and URL redaction."""

from __future__ import annotations

import json
import time
import unittest
import urllib.parse

from .helpers import FakeClock, ScriptedTransport, json_response, make_client

from social_listening.http import HttpError, Response, redact_url


class RetryTest(unittest.TestCase):
    def test_retries_5xx_up_to_max_then_raises(self):
        clock = FakeClock()
        transport = ScriptedTransport(json_response({"error": "down"}, status=503))
        client = make_client(transport, clock, max_retries=2)
        with self.assertRaises(HttpError) as caught:
            client.get_json("https://api.example.test/x")
        self.assertEqual(caught.exception.status, 503)
        self.assertEqual(len(transport.requests), 3)  # first try + 2 retries
        self.assertEqual(client.stats.retries, 2)
        self.assertEqual(client.stats.errors, 1)
        self.assertEqual(clock.sleeps, [2.0, 4.0])  # exponential backoff
        self.assertEqual(len(client.stats.error_messages), 1)

    def test_every_retryable_status_is_retried(self):
        for status in (429, 500, 502, 503, 504):
            with self.subTest(status=status):
                transport = ScriptedTransport(
                    json_response({}, status=status), json_response({"ok": True})
                )
                client = make_client(transport, max_retries=1)
                self.assertEqual(
                    client.get_json("https://api.example.test/x"), {"ok": True}
                )
                self.assertEqual(client.stats.retries, 1)
                self.assertEqual(
                    client.stats.rate_limit_waits, 1 if status == 429 else 0
                )

    def test_zero_retries_raises_on_first_failure(self):
        transport = ScriptedTransport(json_response({}, status=500))
        client = make_client(transport, max_retries=0)
        with self.assertRaises(HttpError):
            client.get_json("https://api.example.test/x")
        self.assertEqual(len(transport.requests), 1)

    def test_backoff_is_capped(self):
        clock = FakeClock()
        transport = ScriptedTransport(json_response({}, status=502))
        client = make_client(transport, clock, max_retries=4, max_backoff_seconds=5)
        with self.assertRaises(HttpError):
            client.get_json("https://api.example.test/x")
        self.assertEqual(clock.sleeps, [2.0, 4.0, 5.0, 5.0])

    def test_honours_retry_after(self):
        clock = FakeClock()
        transport = ScriptedTransport(
            json_response({}, status=429, headers={"Retry-After": "7"}),
            json_response({"ok": True}),
        )
        client = make_client(transport, clock, max_retries=3)
        self.assertEqual(client.get_json("https://api.example.test/x"), {"ok": True})
        self.assertEqual(clock.sleeps, [7.0])
        self.assertEqual(client.stats.rate_limit_waits, 1)
        self.assertEqual(client.stats.waited_seconds, 7.0)

    def test_retry_after_is_capped_by_max_backoff(self):
        clock = FakeClock()
        transport = ScriptedTransport(
            json_response({}, status=429, headers={"Retry-After": "3600"}),
            json_response({}),
        )
        client = make_client(transport, clock, max_backoff_seconds=60)
        client.get_json("https://api.example.test/x")
        self.assertEqual(clock.sleeps, [60.0])

    def test_non_retryable_4xx_raises_immediately(self):
        for status in (400, 401, 403, 404):
            with self.subTest(status=status):
                clock = FakeClock()
                transport = ScriptedTransport(
                    json_response({"message": "nope"}, status=status)
                )
                client = make_client(transport, clock, max_retries=3)
                with self.assertRaises(HttpError) as caught:
                    client.get_json("https://api.example.test/x")
                self.assertEqual(caught.exception.status, status)
                self.assertEqual(len(transport.requests), 1)
                self.assertEqual(client.stats.retries, 0)
                self.assertEqual(clock.sleeps, [])

    def test_network_error_becomes_http_error(self):
        transport = ScriptedTransport(OSError("connection reset"))
        client = make_client(transport, max_retries=3)
        with self.assertRaises(HttpError) as caught:
            client.get_json("https://api.example.test/x")
        self.assertEqual(caught.exception.status, 599)
        self.assertIn("connection reset", str(caught.exception))


class RateLimitTest(unittest.TestCase):
    def test_exhausted_rate_limit_blocks_next_request(self):
        clock = FakeClock()
        transport = ScriptedTransport(
            json_response(
                {}, headers={"X-Ratelimit-Remaining": "0", "X-Ratelimit-Reset": "30"}
            ),
            json_response(
                {}, headers={"X-Ratelimit-Remaining": "99", "X-Ratelimit-Reset": "600"}
            ),
        )
        client = make_client(transport, clock)
        client.get_json("https://oauth.reddit.test/a")
        self.assertEqual(client.stats.rate_limit_waits, 0)
        start = clock.now
        client.get_json("https://oauth.reddit.test/b")
        self.assertEqual(client.stats.rate_limit_waits, 1)
        self.assertEqual(clock.sleeps, [30.0])
        self.assertGreaterEqual(clock.now - start, 30.0)
        # Remaining > 0 does not block the following request.
        client.get_json("https://oauth.reddit.test/c")
        self.assertEqual(client.stats.rate_limit_waits, 1)

    def test_fractional_remaining_below_one_blocks(self):
        clock = FakeClock()
        transport = ScriptedTransport(
            json_response(
                {}, headers={"X-Ratelimit-Remaining": "0.0", "X-Ratelimit-Reset": "12"}
            ),
            json_response({}),
        )
        client = make_client(transport, clock)
        client.get_json("https://oauth.reddit.test/a")
        client.get_json("https://oauth.reddit.test/b")
        self.assertEqual(clock.sleeps, [12.0])

    def test_reset_already_passed_does_not_wait(self):
        clock = FakeClock()
        transport = ScriptedTransport(
            json_response(
                {}, headers={"X-Ratelimit-Remaining": "0", "X-Ratelimit-Reset": "5"}
            ),
            json_response({}),
        )
        client = make_client(transport, clock)
        client.get_json("https://oauth.reddit.test/a")
        clock.advance(10)
        client.get_json("https://oauth.reddit.test/b")
        self.assertEqual(clock.sleeps, [])
        self.assertEqual(client.stats.rate_limit_waits, 0)

    def test_epoch_reset_is_converted_to_seconds(self):
        # GitHub sends X-Ratelimit-Reset as a Unix timestamp, Reddit as seconds.
        clock = FakeClock()
        reset_at = int(time.time()) + 30
        transport = ScriptedTransport(
            json_response(
                {},
                headers={
                    "X-Ratelimit-Remaining": "0",
                    "X-Ratelimit-Reset": str(reset_at),
                },
            ),
            json_response(
                {},
                headers={
                    "X-Ratelimit-Remaining": "29",
                    "X-Ratelimit-Reset": str(reset_at + 60),
                },
            ),
        )
        client = make_client(transport, clock)
        for _ in range(4):
            client.get_json("https://api.github.test/search/issues")
        self.assertEqual(
            len(clock.sleeps), 1
        )  # only the request after the exhausted one waits
        self.assertAlmostEqual(clock.sleeps[0], 30, delta=2)

    def test_malformed_rate_headers_are_ignored(self):
        clock = FakeClock()
        transport = ScriptedTransport(
            json_response(
                {},
                headers={"X-Ratelimit-Remaining": "n/a", "X-Ratelimit-Reset": "soon"},
            ),
        )
        client = make_client(transport, clock)
        client.get_json("https://oauth.reddit.test/a")
        client.get_json("https://oauth.reddit.test/b")
        self.assertEqual(clock.sleeps, [])

    def test_pause_counts_as_rate_limit_wait(self):
        clock = FakeClock()
        client = make_client(ScriptedTransport(json_response({})), clock)
        client.pause(10)
        client.pause(0)
        self.assertEqual(clock.sleeps, [10.0])
        self.assertEqual(client.stats.rate_limit_waits, 1)


class PacingTest(unittest.TestCase):
    def test_min_interval_between_requests(self):
        clock = FakeClock()
        transport = ScriptedTransport(json_response({}))
        client = make_client(transport, clock, min_interval_seconds=1.0)
        client.get_json("https://api.example.test/1")
        self.assertEqual(clock.sleeps, [])  # the first request is not delayed
        client.get_json("https://api.example.test/2")
        client.get_json("https://api.example.test/3")
        self.assertEqual(clock.sleeps, [1.0, 1.0])
        self.assertEqual(client.stats.rate_limit_waits, 0)

    def test_elapsed_time_counts_towards_interval(self):
        clock = FakeClock()
        transport = ScriptedTransport(json_response({}))
        client = make_client(transport, clock, min_interval_seconds=1.0)
        client.get_json("https://api.example.test/1")
        clock.advance(0.4)
        client.get_json("https://api.example.test/2")
        self.assertEqual(len(clock.sleeps), 1)
        self.assertAlmostEqual(clock.sleeps[0], 0.6)
        clock.advance(5)
        client.get_json("https://api.example.test/3")
        self.assertEqual(len(clock.sleeps), 1)

    def test_retries_are_paced_too(self):
        clock = FakeClock()
        transport = ScriptedTransport(json_response({}, status=503), json_response({}))
        client = make_client(transport, clock, min_interval_seconds=10.0)
        client.get_json("https://api.example.test/1")
        # The 2 s backoff is followed by the remaining 8 s of the pacing gap.
        self.assertEqual(clock.sleeps, [2.0, 8.0])


class RequestShapeTest(unittest.TestCase):
    def test_params_headers_and_bodies(self):
        transport = ScriptedTransport(json_response({}))
        client = make_client(transport, user_agent="ua-test/1.0")
        client.request(
            "GET",
            "https://api.example.test/s?x=1",
            params={"q": '"Example Co"', "page": 2},
        )
        client.request(
            "POST",
            "https://api.example.test/j",
            json_body={"a": 1},
            headers={"X-Extra": "y"},
        )
        client.request(
            "POST",
            "https://api.example.test/f",
            form={"grant_type": "client_credentials"},
        )
        get, post_json, post_form = transport.requests
        query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(get["url"]).query))
        self.assertEqual(query, {"x": "1", "q": '"Example Co"', "page": "2"})
        self.assertEqual(get["headers"]["User-Agent"], "ua-test/1.0")
        self.assertEqual(json.loads(post_json["body"]), {"a": 1})
        self.assertEqual(post_json["headers"]["Content-Type"], "application/json")
        self.assertEqual(post_json["headers"]["X-Extra"], "y")
        self.assertEqual(post_form["body"], b"grant_type=client_credentials")
        self.assertEqual(
            post_form["headers"]["Content-Type"], "application/x-www-form-urlencoded"
        )

    def test_empty_body_is_none(self):
        client = make_client(ScriptedTransport(Response(204, {}, b"")))
        self.assertIsNone(client.get_json("https://api.example.test/x"))


class RedactUrlTest(unittest.TestCase):
    def test_hides_credential_query_values(self):
        url = "https://api.example.test/p?q=example&key=K123&access_token=T456&token=T789&client_secret=S0&page=2"
        redacted = redact_url(url)
        for secret in ("K123", "T456", "T789", "S0"):
            self.assertNotIn(secret, redacted)
        params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(redacted).query))
        self.assertEqual(
            params,
            {
                "q": "example",
                "key": "***",
                "access_token": "***",
                "token": "***",
                "client_secret": "***",
                "page": "2",
            },
        )
        self.assertTrue(redacted.startswith("https://api.example.test/p?"))

    def test_key_names_are_case_insensitive(self):
        self.assertNotIn("K1", redact_url("https://x.test/?KEY=K1"))

    def test_http_error_message_is_redacted(self):
        transport = ScriptedTransport(json_response({"error": "bad key"}, status=400))
        client = make_client(transport)
        with self.assertRaises(HttpError) as caught:
            client.get_json(
                "https://api.stackexchange.test/2.3/search",
                params={"key": "SECRETKEY", "q": "x"},
            )
        self.assertNotIn("SECRETKEY", str(caught.exception))
        self.assertNotIn("SECRETKEY", " ".join(client.stats.error_messages))
        self.assertIn("HTTP 400", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
