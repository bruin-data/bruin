"""Reddit collector against the recorded fixtures."""

from __future__ import annotations

import base64
import unittest
from datetime import timedelta

from .helpers import (
    ANCHOR,
    FunctionTransport,
    RecordingTransport,
    fixture_transport,
    json_response,
    make_client,
    make_ctx,
    query_params,
)

from social_listening.http import HttpError
from social_listening.settings import Window
from social_listening.sources import reddit
from social_listening.sources.reddit import (
    KEEP_FIELDS,
    MAX_QUERY_CHARS,
    RedditCollector,
    query_chunks,
    time_filter,
)


def run(ctx, transport=None):
    transport = transport or RecordingTransport(
        fixture_transport(ctx, reddit.fixture_route)
    )
    collector = RedditCollector(ctx, make_client(transport))
    records = list(collector.collect(ctx.collection_window()))
    return collector, records, transport


def ids(records):
    return [r.external_id for r in records]


class PaginationTest(unittest.TestCase):
    def test_follows_after_cursor_to_second_page(self):
        collector, records, transport = run(make_ctx())
        searches = [c["url"] for c in transport.calls if "/search" in c["url"]]
        self.assertEqual(len(searches), 2)
        self.assertNotIn("after", query_params(searches[0]))
        self.assertEqual(query_params(searches[1])["after"], "t3_p5")
        self.assertEqual(collector.stats.pages, 2)
        self.assertEqual(collector.stats.cursor, "t3_p5")
        # t3_p2 is on both pages; de-duplication happens in collect_rows.
        self.assertEqual(
            ids(records),
            ["t3_p1", "t3_p2", "t3_p3", "t3_p4", "t3_p5", "t3_p6", "t3_p7", "t3_p2"],
        )
        self.assertEqual(collector.stats.partial_errors, [])

    def test_record_outside_window_is_not_returned(self):
        _, records, _ = run(make_ctx())
        self.assertNotIn("t3_p8", ids(records))  # created 200,000 s before the end

    def test_stops_paginating_once_window_start_is_reached(self):
        # 4.5 h window: t3_p5 (5 h old) is on page 1, so page 2 is never requested.
        ctx = make_ctx({"late_arrival_hours": 0}, hours=4.5)
        collector, records, transport = run(ctx)
        searches = [c["url"] for c in transport.calls if "/search" in c["url"]]
        self.assertEqual(len(searches), 1)
        self.assertEqual(ids(records), ["t3_p1", "t3_p2", "t3_p3", "t3_p4"])
        self.assertEqual(collector.stats.pages, 1)

    def test_max_pages_is_reported_as_partial(self):
        collector, records, _ = run(make_ctx({"max_pages_per_source": 1}))
        self.assertEqual(len(records), 5)
        self.assertEqual(len(collector.stats.partial_errors), 1)
        self.assertIn("max_pages_per_source=1", collector.stats.partial_errors[0])

    def test_search_parameters(self):
        _, _, transport = run(make_ctx())
        params = query_params(
            next(c["url"] for c in transport.calls if "/search" in c["url"])
        )
        self.assertEqual(params["sort"], "new")
        self.assertEqual(params["t"], "week")  # 30 h window
        self.assertEqual(params["limit"], "100")
        self.assertIn('"Example Co"', params["q"])
        self.assertIn('"social listening"', params["q"])
        self.assertNotIn("restrict_sr", params)


class AuthTest(unittest.TestCase):
    def test_token_request_and_bearer_header(self):
        env = {"REDDIT_CLIENT_ID": "cid", "REDDIT_CLIENT_SECRET": "csecret"}
        _, _, transport = run(make_ctx(env=env))
        token_call = transport.calls[0]
        self.assertEqual(token_call["method"], "POST")
        self.assertTrue(token_call["url"].endswith("/api/v1/access_token"))
        expected = "Basic " + base64.b64encode(b"cid:csecret").decode()
        self.assertEqual(token_call["headers"]["Authorization"], expected)
        self.assertEqual(token_call["body"], b"grant_type=client_credentials")
        for call in transport.calls[1:]:
            self.assertEqual(call["headers"]["Authorization"], "Bearer fixture-token")

    def test_forbidden_global_search_is_fatal(self):
        ctx = make_ctx()
        inner = fixture_transport(ctx, reddit.fixture_route)

        def handler(method, url, headers, body):
            if "/search" in url:
                return json_response({"message": "Forbidden"}, status=403)
            return inner.send(method, url, headers, body, 30)

        collector = RedditCollector(ctx, make_client(FunctionTransport(handler)))
        with self.assertRaises(HttpError) as caught:
            list(collector.collect(ctx.collection_window()))
        self.assertEqual(caught.exception.status, 403)


class CommunitiesTest(unittest.TestCase):
    def test_excluded_communities_are_dropped(self):
        _, records, _ = run(
            make_ctx({"reddit_communities_exclude": ["cooking", "Analytics"]})
        )
        self.assertEqual(ids(records), ["t3_p1", "t3_p3", "t3_p4", "t3_p6", "t3_p7"])

    def test_included_community_searches_and_filters_comments(self):
        collector, records, transport = run(
            make_ctx({"reddit_communities_include": ["dataengineering"]})
        )
        urls = [c["url"] for c in transport.calls[1:]]
        self.assertTrue(
            urls[0].startswith("https://oauth.reddit.com/r/dataengineering/search?")
        )
        self.assertEqual(query_params(urls[0])["restrict_sr"], "1")
        self.assertTrue(
            urls[1].startswith("https://oauth.reddit.com/r/dataengineering/comments?")
        )
        self.assertEqual(
            [(r.external_id, r.content_type) for r in records],
            [("t3_p1", "post"), ("t1_c1", "comment")],
        )
        comment = records[1]
        self.assertIn("Example Co", comment.payload["body"])
        self.assertNotIn("t1_c2", ids(records))  # no configured phrase in the text

    def test_comments_can_be_disabled(self):
        _, records, transport = run(
            make_ctx(
                {
                    "reddit_communities_include": ["dataengineering"],
                    "reddit_include_comments": False,
                }
            )
        )
        self.assertFalse(any("/comments" in c["url"] for c in transport.calls))
        self.assertEqual(ids(records), ["t3_p1"])

    def test_one_failing_community_is_a_partial_error(self):
        ctx = make_ctx({"reddit_communities_include": ["dataengineering", "brokensub"]})
        inner = fixture_transport(ctx, reddit.fixture_route)

        def handler(method, url, headers, body):
            if "/r/brokensub/" in url:
                return json_response({}, status=500)
            return inner.send(method, url, headers, body, 30)

        collector = RedditCollector(
            ctx, make_client(FunctionTransport(handler), max_retries=0)
        )
        records = list(collector.collect(ctx.collection_window()))
        self.assertEqual(ids(records), ["t3_p1", "t1_c1"])
        scopes = [e.split(":")[0] for e in collector.stats.partial_errors]
        self.assertEqual(scopes, ["search brokensub", "comments brokensub"])


class PayloadTest(unittest.TestCase):
    def test_payload_is_minimised(self):
        _, records, _ = run(
            make_ctx({"reddit_communities_include": ["dataengineering"]})
        )
        _, more, _ = run(make_ctx())
        for record in records + more:
            with self.subTest(record=record.external_id):
                self.assertNotIn("author_fullname", record.payload)
                self.assertNotIn("crosspost_parent_list", record.payload)
                self.assertTrue(set(record.payload) <= set(KEEP_FIELDS))

    def test_crosspost_parent_is_captured(self):
        _, records, _ = run(make_ctx())
        by_id = {r.external_id: r for r in records}
        self.assertEqual(by_id["t3_p6"].payload["crosspost_parent"], "t3_p1")
        self.assertNotIn("crosspost_parent", by_id["t3_p1"].payload)

    def test_record_fields(self):
        _, records, _ = run(make_ctx())
        first = records[0]
        self.assertEqual(first.source, "reddit")
        self.assertEqual(first.content_type, "post")
        self.assertEqual(first.cursor, "t3_p1")
        self.assertEqual(first.published_at, ANCHOR - timedelta(seconds=3600))
        self.assertEqual(first.payload["subreddit"], "dataengineering")


class QueryChunksTest(unittest.TestCase):
    def test_respects_max_length_and_keeps_every_phrase(self):
        phrases = [f"phrase number {i} with some padding" for i in range(40)]
        chunks = query_chunks(phrases)
        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(len(chunk), MAX_QUERY_CHARS)
        joined = [p for chunk in chunks for p in chunk.split(" OR ")]
        self.assertEqual(joined, [f'"{p}"' for p in phrases])

    def test_small_list_is_one_chunk(self):
        self.assertEqual(
            query_chunks(["Example Co", "Rival Suite"]),
            ['"Example Co" OR "Rival Suite"'],
        )

    def test_quotes_are_stripped_from_phrases(self):
        self.assertEqual(query_chunks(['say "hi"']), ['"say hi"'])

    def test_oversized_phrase_gets_its_own_chunk(self):
        long_phrase = "x" * (MAX_QUERY_CHARS + 10)
        self.assertEqual(
            query_chunks(["short", long_phrase, "tail"]),
            ['"short"', f'"{long_phrase}"', '"tail"'],
        )

    def test_empty(self):
        self.assertEqual(query_chunks([]), [])


class TimeFilterTest(unittest.TestCase):
    def test_smallest_covering_filter(self):
        cases = [
            (1, "hour"),
            (2, "day"),
            (24, "day"),
            (30, "week"),
            (24 * 20, "month"),
            (24 * 100, "year"),
            (24 * 400, "all"),
        ]
        for hours, expected in cases:
            with self.subTest(hours=hours):
                self.assertEqual(
                    time_filter(Window(ANCHOR - timedelta(hours=hours), ANCHOR)),
                    expected,
                )


if __name__ == "__main__":
    unittest.main()
