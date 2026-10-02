"""Hacker News collector: pagination and window splitting over the 1,000-hit cap."""

from __future__ import annotations

import re
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
from social_listening.sources import hackernews
from social_listening.sources.hackernews import KEEP_FIELDS, HackerNewsCollector

ONLY_BRAND = {
    "brand_aliases": [],
    "competitors": [],
    "tracked_terms": [],
    "term_rules": [],
}


def run(ctx, transport):
    collector = HackerNewsCollector(ctx, make_client(transport, max_retries=0))
    records = list(collector.collect(ctx.collection_window()))
    return collector, records


def bounds(url: str) -> tuple[int, int]:
    match = re.fullmatch(
        r"created_at_i>=(\d+),created_at_i<(\d+)", query_params(url)["numericFilters"]
    )
    return int(match.group(1)), int(match.group(2))


class CappedApi:
    """Fake Algolia: more than 1,000 hits for windows wider than ``cap_above`` seconds."""

    def __init__(self, cap_above: float):
        self.cap_above = cap_above
        self.windows: list[tuple[int, int]] = []

    def __call__(self, method, url, headers, body):
        start, end = bounds(url)
        self.windows.append((start, end))
        if end - start > self.cap_above:
            return json_response({"hits": [], "nbHits": 1500, "nbPages": 1, "page": 0})
        hit = {
            "objectID": f"w{start}",
            "_tags": ["story"],
            "created_at_i": start,
            "title": "Example Co",
        }
        return json_response({"hits": [hit], "nbHits": 1, "nbPages": 1, "page": 0})


class FixtureTest(unittest.TestCase):
    def setUp(self):
        self.ctx = make_ctx()
        self.transport = RecordingTransport(
            fixture_transport(self.ctx, hackernews.fixture_route)
        )
        self.collector, self.records = run(self.ctx, self.transport)

    def test_fetches_both_pages_for_example_co(self):
        pages = [
            query_params(c["url"])["page"]
            for c in self.transport.calls
            if query_params(c["url"])["query"] == '"Example Co"'
        ]
        self.assertEqual(pages, ["0", "1"])
        ids = [r.external_id for r in self.records]
        self.assertEqual(ids[:3], ["9001", "9002", "9003"])  # 9003 comes from page 1
        self.assertEqual(sorted(ids), ["9001", "9002", "9003", "9004", "9005", "9006"])

    def test_one_query_per_phrase_bounded_by_window(self):
        queries = {query_params(c["url"])["query"] for c in self.transport.calls}
        self.assertEqual(
            queries,
            {
                '"Example Co"',
                '"exampleco"',
                '"Rival Suite"',
                '"example alternative"',
                '"social listening"',
            },
        )
        window = self.ctx.collection_window()
        for call in self.transport.calls:
            self.assertEqual(
                bounds(call["url"]), (window.start_epoch, window.end_epoch)
            )
            self.assertEqual(query_params(call["url"])["tags"], "(story,comment)")

    def test_record_shape(self):
        by_id = {r.external_id: r for r in self.records}
        self.assertEqual(by_id["9001"].content_type, "story")
        self.assertEqual(by_id["9002"].content_type, "comment")
        self.assertEqual(by_id["9001"].published_at, ANCHOR - timedelta(seconds=5400))
        for record in self.records:
            self.assertTrue(set(record.payload) <= set(KEEP_FIELDS))
        self.assertEqual(self.collector.stats.partial_errors, [])
        self.assertEqual(self.collector.stats.pages, len(self.transport.calls))

    def test_stories_only(self):
        ctx = make_ctx({"hackernews_include_comments": False})
        transport = RecordingTransport(fixture_transport(ctx, hackernews.fixture_route))
        run(ctx, transport)
        self.assertEqual(
            {query_params(c["url"])["tags"] for c in transport.calls}, {"story"}
        )


class SplitTest(unittest.TestCase):
    def test_splits_window_over_hit_cap(self):
        ctx = make_ctx(ONLY_BRAND)  # 30 h collection window
        api = CappedApi(cap_above=6 * 3600)
        collector, records = run(ctx, FunctionTransport(api))
        window = ctx.collection_window()
        leaves = sorted(w for w in api.windows if w[1] - w[0] <= 6 * 3600)
        self.assertEqual(len(leaves), 8)  # 30 h -> 15 h -> 7.5 h -> 3.75 h
        self.assertEqual(leaves[0][0], window.start_epoch)
        self.assertEqual(leaves[-1][1], window.end_epoch)
        for left, right in zip(leaves, leaves[1:]):
            self.assertEqual(
                left[1], right[0]
            )  # halves tile the window, no gap or overlap
        self.assertEqual(len(api.windows), 1 + 2 + 4 + 8)
        self.assertEqual(len(records), 8)
        self.assertEqual(collector.stats.partial_errors, [])

    def test_never_splits_below_one_hour(self):
        ctx = make_ctx(ONLY_BRAND)
        api = CappedApi(cap_above=0)  # every window is over the cap
        collector, records = run(ctx, FunctionTransport(api))
        requested = set(api.windows)
        for start, end in requested:
            mid = start + (end - start) // 2
            if (start, mid) in requested:  # this window was split
                self.assertGreater(end - start, 3600)
        spans = [end - start for start, end in requested]
        self.assertGreaterEqual(min(spans), 1800)
        leaves = [s for s in spans if s <= 3600]
        self.assertEqual(len(leaves), 32)  # 30 h / 2**5 = 56.25 min
        self.assertEqual(len(collector.stats.partial_errors), 32)
        self.assertIn(
            "more than 1,000 hits in one hour", collector.stats.partial_errors[0]
        )
        self.assertEqual(records, [])

    def test_one_hour_window_is_not_split(self):
        ctx = make_ctx(dict(ONLY_BRAND, late_arrival_hours=0), hours=1)
        api = CappedApi(cap_above=0)
        collector, _ = run(ctx, FunctionTransport(api))
        self.assertEqual(len(api.windows), 1)
        self.assertEqual(len(collector.stats.partial_errors), 1)

    def test_split_depth_is_bounded(self):
        ctx = make_ctx(dict(ONLY_BRAND, late_arrival_hours=0), hours=24 * 7)
        api = CappedApi(cap_above=0)
        run(ctx, FunctionTransport(api))
        self.assertEqual(len(api.windows), 2**7 - 1)  # depth 0..6
        self.assertEqual(
            min(end - start for start, end in api.windows), 7 * 24 * 3600 // 64
        )


class ErrorTest(unittest.TestCase):
    def test_max_pages_is_reported(self):
        def handler(method, url, headers, body):
            return json_response({"hits": [], "nbHits": 500, "nbPages": 10})

        transport = FunctionTransport(handler)
        collector, _ = run(
            make_ctx(dict(ONLY_BRAND, max_pages_per_source=2)), transport
        )
        self.assertEqual(len(transport.requests), 2)
        self.assertIn("max_pages_per_source=2", collector.stats.partial_errors[0])

    def test_failing_phrase_is_partial(self):
        ctx = make_ctx()
        inner = fixture_transport(ctx, hackernews.fixture_route)

        def handler(method, url, headers, body):
            if query_params(url)["query"] == '"Rival Suite"':
                return json_response({}, status=500)
            return inner.send(method, url, headers, body, 30)

        collector, records = run(ctx, FunctionTransport(handler))
        self.assertNotIn("9004", [r.external_id for r in records])
        self.assertIn("9005", [r.external_id for r in records])
        self.assertEqual(len(collector.stats.partial_errors), 1)
        self.assertTrue(
            collector.stats.partial_errors[0].startswith("query 'Rival Suite'")
        )

    def test_unauthorised_is_fatal(self):
        transport = FunctionTransport(lambda *a: json_response({}, status=401))
        with self.assertRaises(HttpError):
            run(make_ctx(), transport)


if __name__ == "__main__":
    unittest.main()
