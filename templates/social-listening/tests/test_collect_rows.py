"""collect_rows: event keys, de-duplication, run summaries and failure handling."""

from __future__ import annotations

import contextlib
import io
import json
import unittest
from datetime import timedelta

from .helpers import (
    ANCHOR,
    FunctionTransport,
    MutatingTransport,
    ScriptedTransport,
    fixture_transport,
    json_response,
    make_client,
    make_ctx,
)

from social_listening import collect
from social_listening.sources import reddit
from social_listening.sources.base import (
    RAW_COLUMNS,
    RawRecord,
    SourceCollector,
    canonical_json,
    collect_rows,
    content_hash,
    event_key,
    sha256,
)
from social_listening.sources.reddit import RedditCollector


def reddit_rows(ctx, transport=None, **client_kwargs):
    transport = transport or fixture_transport(ctx, reddit.fixture_route)
    collector = RedditCollector(ctx, make_client(transport, **client_kwargs))
    return collect_rows(ctx, "reddit", collector, enabled=True, mode="demo")


def content(rows):
    return [r for r in rows if r["record_kind"] == "content"]


def summary(rows):
    self_rows = [r for r in rows if r["record_kind"] == "run_summary"]
    assert len(self_rows) == 1, self_rows
    return json.loads(self_rows[0]["payload_json"])


class StubCollector(SourceCollector):
    source = "stub"

    def __init__(self, ctx, records):
        super().__init__(ctx, make_client(ScriptedTransport(json_response({}))))
        self.records = records

    def collect(self, window):
        yield from self.records


class DedupeTest(unittest.TestCase):
    def test_duplicate_within_run_is_stored_once(self):
        rows = reddit_rows(make_ctx())
        ids = [r["external_id"] for r in content(rows)]
        self.assertEqual(ids.count("t3_p2"), 1)
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(len(ids), 7)
        s = summary(rows)
        self.assertEqual(s["duplicates_in_run"], 1)
        self.assertEqual(s["records"], 7)
        self.assertEqual(s["pages"], 2)
        self.assertEqual(s["requests"], 3)  # token + two search pages
        self.assertEqual(s["status"], "ok")

    def test_rows_have_the_raw_columns(self):
        names = [c["name"] for c in RAW_COLUMNS]
        for row in reddit_rows(make_ctx()):
            self.assertEqual(sorted(row), sorted(names))

    def test_event_key_is_derived_from_source_id_and_payload(self):
        row = content(reddit_rows(make_ctx()))[0]
        self.assertEqual(row["payload_sha256"], sha256(row["payload_json"]))
        self.assertEqual(
            row["event_key"],
            event_key(
                "reddit",
                row["external_id"],
                content_hash(json.loads(row["payload_json"])),
            ),
        )
        self.assertNotIn("author_fullname", json.loads(row["payload_json"]))

    def test_times_are_naive_utc(self):
        rows = reddit_rows(make_ctx())
        first = content(rows)[0]
        self.assertIsNone(first["published_at"].tzinfo)
        self.assertEqual(
            first["published_at"], (ANCHOR - timedelta(hours=1)).replace(tzinfo=None)
        )
        self.assertEqual(
            first["window_start"], (ANCHOR - timedelta(hours=30)).replace(tzinfo=None)
        )
        self.assertEqual(first["window_end"], ANCHOR.replace(tzinfo=None))
        self.assertEqual(first["ingest_run_id"], "run-1")
        self.assertEqual(first["collection_mode"], "demo")


class IdempotencyTest(unittest.TestCase):
    def test_rerunning_a_window_yields_identical_keys(self):
        first = content(reddit_rows(make_ctx(run_id="run-1")))
        second = content(reddit_rows(make_ctx(run_id="run-2")))
        self.assertEqual(
            [r["event_key"] for r in first], [r["event_key"] for r in second]
        )
        merged = {r["event_key"]: r for r in first + second}  # merge on the primary key
        self.assertEqual(len(merged), len(first))

    def test_engagement_changes_keep_the_key(self):
        ctx = make_ctx()

        def bump_score(url, body):
            for child in (body.get("data") or {}).get("children") or []:
                if child["data"].get("name") == "t3_p1":
                    child["data"]["score"] += 1
                    child["data"]["num_comments"] += 3
            return body

        before = {r["external_id"]: r for r in content(reddit_rows(ctx))}
        after = {
            r["external_id"]: r
            for r in content(
                reddit_rows(
                    ctx,
                    MutatingTransport(
                        fixture_transport(ctx, reddit.fixture_route), bump_score
                    ),
                )
            )
        }
        self.assertEqual(before["t3_p1"]["event_key"], after["t3_p1"]["event_key"])
        self.assertNotEqual(
            before["t3_p1"]["payload_sha256"], after["t3_p1"]["payload_sha256"]
        )

    def test_changed_payload_yields_a_new_key(self):
        ctx = make_ctx()

        def bump_score(url, body):
            for child in (body.get("data") or {}).get("children") or []:
                if child["data"].get("name") == "t3_p1":
                    child["data"]["selftext"] += " (edited)"
            return body

        before = content(reddit_rows(ctx))
        after = content(
            reddit_rows(
                ctx,
                MutatingTransport(
                    fixture_transport(ctx, reddit.fixture_route), bump_score
                ),
            )
        )
        keys_before = {r["external_id"]: r["event_key"] for r in before}
        keys_after = {r["external_id"]: r["event_key"] for r in after}
        self.assertNotEqual(keys_before["t3_p1"], keys_after["t3_p1"])
        for external_id in keys_before.keys() - {"t3_p1"}:
            self.assertEqual(keys_before[external_id], keys_after[external_id])
        merged = {r["event_key"]: r for r in before + after}
        self.assertEqual(
            len(merged), len(before) + 1
        )  # the edit is a new, auditable version

    def test_canonical_json_ignores_key_order(self):
        self.assertEqual(
            canonical_json({"b": 1, "a": "é"}), canonical_json({"a": "é", "b": 1})
        )
        self.assertEqual(canonical_json({"a": "é"}), '{"a":"é"}')


class SummaryTest(unittest.TestCase):
    def test_summary_row_is_last(self):
        rows = reddit_rows(make_ctx())
        self.assertEqual(rows[-1]["record_kind"], "run_summary")
        self.assertEqual(rows[-1]["external_id"], "run:run-1")
        s = summary(rows)
        self.assertEqual(
            s["high_watermark"], (ANCHOR - timedelta(seconds=3000)).isoformat()
        )  # t3_p6
        self.assertEqual(
            rows[-1]["published_at"],
            (ANCHOR - timedelta(seconds=3000)).replace(tzinfo=None),
        )
        self.assertEqual(s["window_start"], (ANCHOR - timedelta(hours=30)).isoformat())

    def test_disabled_source_writes_one_summary(self):
        ctx = make_ctx()
        rows = collect_rows(ctx, "reddit", None, enabled=False, mode="demo")
        self.assertEqual(len(rows), 1)
        s = summary(rows)
        self.assertEqual(s["status"], "disabled")
        self.assertEqual((s["records"], s["requests"]), (0, 0))
        self.assertIsNone(rows[0]["published_at"])

    def test_disabled_flag_wins_over_collector(self):
        ctx = make_ctx()
        collector = StubCollector(ctx, [])
        rows = collect_rows(ctx, "stub", collector, enabled=False, mode="demo")
        self.assertEqual(len(rows), 1)
        self.assertEqual(summary(rows)["status"], "disabled")

    def test_records_outside_window_are_skipped(self):
        ctx = make_ctx()
        window = ctx.collection_window()
        records = [
            RawRecord("stub", "start", "post", {"n": 1}, window.start),
            RawRecord("stub", "end", "post", {"n": 2}, window.end),
            RawRecord(
                "stub", "before", "post", {"n": 3}, window.start - timedelta(seconds=1)
            ),
        ]
        rows = collect_rows(
            ctx, "stub", StubCollector(ctx, records), enabled=True, mode="demo"
        )
        self.assertEqual([r["external_id"] for r in content(rows)], ["start"])
        self.assertEqual(summary(rows)["skipped_outside_window"], 2)

    def test_partial_errors_mark_the_run_partial(self):
        ctx = make_ctx({"max_pages_per_source": 1})
        s = summary(reddit_rows(ctx))
        self.assertEqual(s["status"], "partial")
        self.assertEqual(len(s["partial_errors"]), 1)


class FailureTest(unittest.TestCase):
    def failing(self):
        return FunctionTransport(
            lambda *a: json_response({"error": "down"}, status=500)
        )

    def test_fatal_http_error_raises_by_default(self):
        with self.assertRaises(RuntimeError) as caught:
            reddit_rows(make_ctx(), self.failing(), max_retries=1)
        self.assertIn("reddit collection failed", str(caught.exception))
        self.assertIn("HTTP 500", str(caught.exception))

    def test_fatal_http_error_is_stored_when_not_failing(self):
        rows = reddit_rows(
            make_ctx({"fail_on_source_error": False}), self.failing(), max_retries=1
        )
        self.assertEqual(len(rows), 1)
        s = summary(rows)
        self.assertEqual(s["status"], "failed")
        self.assertIn("HTTP 500", s["fatal_error"])
        self.assertEqual((s["requests"], s["retries"], s["http_errors"]), (2, 1, 1))

    def test_rows_before_the_failure_are_kept(self):
        ctx = make_ctx({"fail_on_source_error": False})
        inner = fixture_transport(ctx, reddit.fixture_route)

        def handler(method, url, headers, body):
            if "after=t3_p5" in url:
                return json_response({}, status=401)
            return inner.send(method, url, headers, body, 30)

        rows = reddit_rows(ctx, FunctionTransport(handler))
        self.assertEqual(len(content(rows)), 5)
        self.assertEqual(summary(rows)["status"], "failed")


class RunSourceTest(unittest.TestCase):
    """collect.run_source in demo mode: fixtures only, no pacing, nothing leaves the process."""

    def run_source(self, source, overrides=None):
        with contextlib.redirect_stdout(io.StringIO()) as out:
            rows = collect.run_source(source, make_ctx(overrides))
        return rows, out.getvalue()

    def test_reddit_demo(self):
        rows, out = self.run_source("reddit")
        self.assertEqual(len(content(rows)), 7)
        self.assertIn("[reddit] mode=demo enabled=True", out)

    def test_hackernews_demo(self):
        rows, _ = self.run_source("hackernews")
        # 9006 is in assets/config/redaction_requests.csv, so it never reaches raw.
        self.assertEqual(
            sorted(r["external_id"] for r in content(rows)),
            ["9001", "9002", "9003", "9004", "9005"],
        )
        self.assertEqual(summary(rows)["redacted_skipped"], 1)

    def test_redacted_ids_are_read_per_source(self):
        self.assertEqual(collect.redacted_ids("hackernews"), {"9006"})
        self.assertEqual(collect.redacted_ids("reddit"), set())

    def test_redacted_records_are_not_written_to_raw(self):
        ctx = make_ctx()
        http, mode = collect.build_http(ctx, reddit.fixture_route, 0.0)
        rows = collect_rows(
            ctx,
            "reddit",
            RedditCollector(ctx, http),
            enabled=True,
            mode=mode,
            suppressed={"t3_p1"},
        )
        self.assertNotIn("t3_p1", [r["external_id"] for r in content(rows)])
        self.assertEqual(summary(rows)["redacted_skipped"], 1)

    def test_disabled_source(self):
        rows, out = self.run_source("github")
        self.assertEqual(len(rows), 1)
        self.assertEqual(summary(rows)["status"], "disabled")
        self.assertIn("enabled=False", out)

    def test_every_source_in_demo(self):
        flags = {
            "github_enabled": True,
            "stackoverflow_enabled": True,
            "authorised_export_enabled": True,
        }
        for source in collect.SOURCES:
            with self.subTest(source=source):
                rows, _ = self.run_source(source, flags)
                self.assertEqual(summary(rows)["status"], "ok")
                self.assertGreater(len(content(rows)), 0)

    def test_live_mode_uses_urllib(self):
        from social_listening.http import UrllibTransport

        client, mode = collect.build_http(make_ctx({"demo_mode": False}), None, 2.1)
        self.assertEqual(mode, "live")
        self.assertIsInstance(client.transport, UrllibTransport)
        self.assertEqual(client.min_interval, 2.1)


if __name__ == "__main__":
    unittest.main()


class OutageTest(unittest.TestCase):
    def test_every_request_failing_is_fatal(self):
        class AlwaysFails(SourceCollector):
            source = "hackernews"

            def collect(self, window):
                self.partial_error("query 'a'", RuntimeError("HTTP 503"))
                self.partial_error("query 'b'", RuntimeError("HTTP 503"))
                return iter(())

        ctx = make_ctx()
        with self.assertRaises(RuntimeError):
            collect_rows(
                ctx,
                "hackernews",
                AlwaysFails(ctx, make_client(None)),
                enabled=True,
                mode="demo",
            )
        rows = collect_rows(
            make_ctx({"fail_on_source_error": False}),
            "hackernews",
            AlwaysFails(ctx, make_client(None)),
            enabled=True,
            mode="demo",
        )
        self.assertEqual(summary(rows)["status"], "failed")
        self.assertIn("no request succeeded", summary(rows)["fatal_error"])


class StoredKeysTest(unittest.TestCase):
    def test_already_stored_records_are_not_rewritten(self):
        ctx = make_ctx()
        first = content(reddit_rows(ctx))
        http, mode = collect.build_http(ctx, reddit.fixture_route, 0.0)
        rows = collect_rows(
            ctx,
            "reddit",
            RedditCollector(ctx, http),
            enabled=True,
            mode=mode,
            stored={r["event_key"] for r in first},
        )
        self.assertEqual(content(rows), [])
        self.assertEqual(summary(rows)["already_stored"], len(first))
