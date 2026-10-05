"""GitHub, Stack Exchange and authorised-export collectors against their fixtures."""

from __future__ import annotations

import csv
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from .helpers import (
    ANCHOR,
    FakeClock,
    FunctionTransport,
    MutatingTransport,
    RecordingTransport,
    fixture_transport,
    json_response,
    make_client,
    make_ctx,
    query_params,
)

from social_listening.http import HttpError
from social_listening.sources import github, stackexchange
from social_listening.sources.base import collect_rows
from social_listening.sources.exports import REQUIRED, ExportFileCollector
from social_listening.sources.github import GitHubCollector
from social_listening.sources.stackexchange import StackExchangeCollector


class GitHubTest(unittest.TestCase):
    def run_github(self, ctx, transport=None):
        transport = transport or RecordingTransport(
            fixture_transport(ctx, github.fixture_route)
        )
        collector = GitHubCollector(ctx, make_client(transport))
        return collector, list(collector.collect(ctx.collection_window())), transport

    def test_parses_fixture(self):
        ctx = make_ctx({"github_enabled": True}, env={"GITHUB_TOKEN": "ghp_test"})
        collector, records, transport = self.run_github(ctx)
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(
            (record.source, record.external_id, record.content_type),
            ("github", "7001", "issue"),
        )
        self.assertEqual(record.published_at, ANCHOR - timedelta(seconds=4000))
        self.assertEqual(record.payload["author"], "gh_user")
        self.assertEqual(record.payload["author_type"], "User")
        self.assertEqual(record.payload["reactions"], 2)
        self.assertEqual(record.payload["number"], 12)
        self.assertNotIn("user", record.payload)
        self.assertEqual(collector.stats.partial_errors, [])

    def test_query_and_headers(self):
        ctx = make_ctx({"github_enabled": True}, env={"GITHUB_TOKEN": "ghp_test"})
        _, _, transport = self.run_github(ctx)
        call = transport.calls[0]
        self.assertEqual(call["headers"]["Authorization"], "Bearer ghp_test")
        self.assertEqual(call["headers"]["X-GitHub-Api-Version"], "2022-11-28")
        q = query_params(call["url"])["q"]
        self.assertTrue(q.startswith('"Example Co" in:title,body is:public created:'))
        self.assertIn("2026-09-26T18:00:00Z..2026-09-28T00:00:00Z", q)

    def test_no_token_no_authorization_header(self):
        ctx = make_ctx({"github_enabled": True})
        _, _, transport = self.run_github(ctx)
        self.assertNotIn("Authorization", transport.calls[0]["headers"])

    def test_pull_request_kind(self):
        ctx = make_ctx({"github_enabled": True})

        def mark_pr(url, body):
            for item in body.get("items") or []:
                item["pull_request"] = {
                    "url": "https://api.github.com/repos/example-org/example-repo/pulls/12"
                }
            return body

        transport = MutatingTransport(
            fixture_transport(ctx, github.fixture_route), mark_pr
        )
        _, records, _ = self.run_github(ctx, transport)
        self.assertEqual(records[0].content_type, "pull_request")

    def test_forbidden_is_fatal(self):
        ctx = make_ctx({"github_enabled": True})
        with self.assertRaises(HttpError):
            self.run_github(
                ctx, FunctionTransport(lambda *a: json_response({}, status=403))
            )

    def test_collect_rows(self):
        ctx = make_ctx({"github_enabled": True})
        collector = GitHubCollector(
            ctx, make_client(fixture_transport(ctx, github.fixture_route))
        )
        rows = collect_rows(ctx, "github", collector, enabled=True, mode="demo")
        self.assertEqual([r["record_kind"] for r in rows], ["content", "run_summary"])


class StackExchangeTest(unittest.TestCase):
    def test_parses_fixture(self):
        ctx = make_ctx({"stackoverflow_enabled": True})
        transport = RecordingTransport(
            fixture_transport(ctx, stackexchange.fixture_route)
        )
        collector = StackExchangeCollector(ctx, make_client(transport))
        records = list(collector.collect(ctx.collection_window()))
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(
            (record.source, record.external_id, record.content_type),
            ("stackoverflow", "8001", "question"),
        )
        self.assertEqual(record.published_at, ANCHOR - timedelta(seconds=5000))
        self.assertEqual(record.payload["author"], "so_asker")
        self.assertEqual(
            record.payload["link"], "https://stackoverflow.com/questions/8001"
        )
        self.assertNotIn("owner", record.payload)
        # One request per phrase: the fixture has has_more=false.
        self.assertEqual(len(transport.calls), 5)
        window = ctx.collection_window()
        params = query_params(transport.calls[0]["url"])
        self.assertEqual(params["q"], "Example Co")
        self.assertEqual(int(params["fromdate"]), window.start_epoch)
        self.assertEqual(
            int(params["todate"]), window.end_epoch - 1
        )  # API bounds are inclusive
        self.assertNotIn("key", params)

    def test_key_is_sent_when_configured(self):
        ctx = make_ctx(
            {"stackoverflow_enabled": True}, env={"STACKEXCHANGE_KEY": "se-key"}
        )
        transport = RecordingTransport(
            fixture_transport(ctx, stackexchange.fixture_route)
        )
        list(
            StackExchangeCollector(ctx, make_client(transport)).collect(
                ctx.collection_window()
            )
        )
        self.assertEqual(query_params(transport.calls[0]["url"])["key"], "se-key")

    def test_backoff_pauses_before_next_request(self):
        ctx = make_ctx({"stackoverflow_enabled": True})
        clock = FakeClock()

        def add_backoff(url, body):
            if query_params(url)["q"] == "Example Co":
                body["backoff"] = 10
            return body

        transport = MutatingTransport(
            fixture_transport(ctx, stackexchange.fixture_route), add_backoff
        )
        client = make_client(transport, clock)
        records = list(
            StackExchangeCollector(ctx, client).collect(ctx.collection_window())
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(clock.sleeps, [10.0])
        self.assertEqual(client.stats.rate_limit_waits, 1)
        self.assertEqual(client.stats.waited_seconds, 10.0)

    def test_has_more_pages(self):
        ctx = make_ctx(
            {
                "stackoverflow_enabled": True,
                "max_pages_per_source": 3,
                "brand_aliases": [],
                "competitors": [],
                "tracked_terms": [],
                "term_rules": [],
            }
        )
        transport = FunctionTransport(
            lambda *a: json_response({"items": [], "has_more": True})
        )
        list(
            StackExchangeCollector(ctx, make_client(transport)).collect(
                ctx.collection_window()
            )
        )
        self.assertEqual(
            [query_params(r["url"])["page"] for r in transport.requests],
            ["1", "2", "3"],
        )


class ExportTest(unittest.TestCase):
    def test_parses_sample_export(self):
        ctx = make_ctx({"authorised_export_enabled": True})
        transport = FunctionTransport(lambda *a: json_response({}))
        collector = ExportFileCollector(ctx, make_client(transport))
        window = ctx.collection_window()
        records = list(collector.collect(window))
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record.source, "authorised_export")
        self.assertEqual(record.external_id, "linkedin:urn-li-activity-1")
        self.assertEqual(record.content_type, "export_item")
        self.assertEqual(record.published_at, window.end - timedelta(seconds=4200))
        self.assertEqual(record.cursor, "@-4200")
        self.assertEqual(record.payload["metrics"], {"likes": 12, "comments": 4})
        self.assertEqual(set(record.payload), set(REQUIRED) | {"metrics"})
        self.assertIn("example alternative", record.payload["body"])
        self.assertEqual(transport.requests, [])  # exports never fetch anything

    def write_csv(self, rows, fieldnames):
        handle = tempfile.NamedTemporaryFile(
            "w", suffix=".csv", delete=False, newline="", encoding="utf-8"
        )
        self.addCleanup(os.unlink, handle.name)
        with handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return handle.name

    def collector(self, path):
        ctx = make_ctx(
            {"authorised_export_enabled": True, "authorised_export_path": path}
        )
        return ctx, ExportFileCollector(
            ctx, make_client(FunctionTransport(lambda *a: json_response({})))
        )

    def test_iso_times_and_unsupported_platforms(self):
        base = {
            "url": "u",
            "author": "a",
            "title": "t",
            "body": "b",
            "content_type": "export_item",
        }
        path = self.write_csv(
            [
                dict(
                    base,
                    platform="X",
                    external_id="1",
                    published_at="2026-09-27T12:00:00Z",
                ),
                dict(
                    base,
                    platform="myspace",
                    external_id="2",
                    published_at="2026-09-27T12:00:00Z",
                ),
            ],
            list(REQUIRED),
        )
        ctx, collector = self.collector(path)
        records = list(collector.collect(ctx.collection_window()))
        self.assertEqual([r.external_id for r in records], ["x:1"])
        self.assertEqual(
            records[0].published_at, datetime(2026, 9, 27, 12, tzinfo=timezone.utc)
        )
        self.assertEqual(records[0].payload["metrics"], {})
        self.assertEqual(len(collector.stats.partial_errors), 1)
        self.assertIn(
            "unsupported platform 'myspace'", collector.stats.partial_errors[0]
        )

    def test_missing_columns(self):
        path = self.write_csv([], ["platform", "external_id"])
        ctx, collector = self.collector(path)
        with self.assertRaises(ValueError) as caught:
            list(collector.collect(ctx.collection_window()))
        self.assertIn("missing columns", str(caught.exception))


if __name__ == "__main__":
    unittest.main()


class GitHubPullRequestToggleTest(unittest.TestCase):
    def queries(self, overrides):
        from social_listening import collect
        from social_listening.sources import github

        ctx = make_ctx({"github_enabled": True, **overrides})
        http, _ = collect.build_http(ctx, github.fixture_route, 0.0)
        list(github.GitHubCollector(ctx, http).collect(ctx.collection_window()))
        return [u for u in http.transport.requests if "search/issues" in u]

    def test_issues_only_by_default(self):
        self.assertTrue(all("is%3Aissue" in u for u in self.queries({})))

    def test_pull_requests_opt_in(self):
        self.assertTrue(
            all(
                "is%3Aissue" not in u
                for u in self.queries({"github_include_pull_requests": True})
            )
        )
