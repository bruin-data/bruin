"""Alert delivery: planning across runs, execution, Slack formatting and idempotency headers."""

from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone

from .helpers import ScriptedTransport, default_vars, json_response, make_client

from social_listening.delivery import (
    AlertHistory,
    attempt_key,
    execute,
    format_slack,
    plan_attempts,
    send,
    summarize,
)
from social_listening.http import HttpError

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
URL = "https://hooks.example.test/alerts"


def decision(
    key, priority=0.8, destination="slack_webhook", age_hours=1.0, **payload_extra
):
    payload = {
        "alert_key": key,
        "source": "reddit",
        "content_type": "post",
        "priority": priority,
        "url": f"https://www.reddit.com/r/dataengineering/comments/{key}/",
        "title": f"Post {key}",
        "matched_text": "Example Co",
        "rule_ids": ["brand"],
        "intent": "seeking_recommendation",
        "relevance": 0.9,
        "confidence": 0.8,
        "model_generated": False,
    }
    payload.update(payload_extra)
    return {
        "alert_key": key,
        "priority": priority,
        "destination": destination,
        "published_at": NOW - timedelta(hours=age_hours),
        "payload_json": json.dumps(payload),
        "payload_version": "v1",
    }


class Senders:
    def __init__(self, outcomes=None):
        self.outcomes = list(outcomes or [])
        self.calls = []

    def __call__(self, decision, http, url, destination):
        self.calls.append((decision["alert_key"], url, destination))
        outcome = self.outcomes.pop(0) if self.outcomes else None
        if isinstance(outcome, Exception):
            raise outcome


class Outbox:
    """Simulates consecutive pipeline runs sharing one attempts table."""

    def __init__(self, decisions, **overrides):
        self.decisions = decisions
        self.vars = default_vars(**overrides)
        self.attempts: list[dict] = []
        self.runs = 0

    def run(self, sender=None, now=None):
        self.runs += 1
        now = now or NOW + timedelta(minutes=15 * self.runs)
        plan = plan_attempts(self.decisions, summarize(self.attempts), self.vars, now)
        rows = execute(
            plan,
            http=None,
            url=URL,
            run_id=f"run-{self.runs}",
            now=now,
            sender=sender or Senders(),
        )
        self.attempts.extend(rows)
        return rows

    def statuses(self, key):
        return [r["status"] for r in self.attempts if r["alert_key"] == key]


def failure(status=503):
    return HttpError(status, URL, "unavailable")


class PlanTest(unittest.TestCase):
    def test_delivered_alert_is_never_resent(self):
        outbox = Outbox([decision("a")], notification_dry_run=False)
        sender = Senders()
        outbox.run(sender)
        for _ in range(3):
            self.assertEqual(outbox.run(sender), [])
        self.assertEqual(outbox.statuses("a"), ["delivered"])
        self.assertEqual(len(sender.calls), 1)

    def test_dry_run_records_once_per_alert(self):
        outbox = Outbox(
            [decision("a"), decision("b", priority=0.5)]
        )  # dry run is the default
        sender = Senders()
        for _ in range(4):
            outbox.run(sender)
        self.assertEqual(outbox.statuses("a"), ["dry_run"])
        self.assertEqual(outbox.statuses("b"), ["dry_run"])
        self.assertEqual(sender.calls, [])  # nothing leaves the machine

    def test_live_attempts_are_capped_across_runs(self):
        outbox = Outbox(
            [decision("a")], notification_dry_run=False, max_delivery_attempts=3
        )
        sender = Senders([failure(), failure(), failure(), failure()])
        for expected_number in (1, 2, 3):
            rows = outbox.run(sender)
            self.assertEqual(
                [(r["status"], r["attempt_number"]) for r in rows],
                [("failed", expected_number)],
            )
        self.assertEqual(outbox.run(sender), [])  # no 4th attempt
        self.assertEqual(outbox.run(sender), [])
        self.assertEqual(len(sender.calls), 3)
        self.assertEqual(len({r["attempt_key"] for r in outbox.attempts}), 3)

    def test_failure_then_success(self):
        outbox = Outbox([decision("a")], notification_dry_run=False)
        sender = Senders([failure(), None])
        outbox.run(sender)
        outbox.run(sender)
        self.assertEqual(outbox.run(sender), [])
        self.assertEqual(outbox.statuses("a"), ["failed", "delivered"])

    def test_one_attempt_per_alert_per_run(self):
        outbox = Outbox([decision("a")], notification_dry_run=False)
        rows = outbox.run(Senders([failure()]))
        self.assertEqual(len(rows), 1)

    def test_max_alerts_per_run_highest_priority_first(self):
        decisions = [
            decision(k, priority=p)
            for k, p in (("a", 0.5), ("b", 0.9), ("c", 0.7), ("d", 0.9), ("e", 0.6))
        ]
        outbox = Outbox(decisions, notification_dry_run=False, max_alerts_per_run=2)
        order = [[r["alert_key"] for r in outbox.run()] for _ in range(4)]
        self.assertEqual(order, [["b", "d"], ["c", "e"], ["a"], []])

    def test_cap_counts_only_live_sends(self):
        decisions = [
            decision("old", priority=0.99, age_hours=100),
            decision("a"),
            decision("b"),
        ]
        outbox = Outbox(decisions, notification_dry_run=False, max_alerts_per_run=2)
        rows = outbox.run()
        self.assertEqual(
            sorted((r["alert_key"], r["status"]) for r in rows),
            [("a", "delivered"), ("b", "delivered"), ("old", "expired")],
        )

    def test_old_content_is_expired_not_sent(self):
        outbox = Outbox(
            [decision("old", age_hours=100), decision("new", age_hours=1)],
            notification_dry_run=False,
        )
        sender = Senders()
        outbox.run(sender)
        outbox.run(sender)
        self.assertEqual(outbox.statuses("old"), ["expired"])
        self.assertEqual(outbox.statuses("new"), ["delivered"])
        self.assertEqual([c[0] for c in sender.calls], ["new"])

    def test_expiry_applies_in_dry_run_and_accepts_iso_strings(self):
        old = dict(
            decision("old"),
            published_at=(NOW - timedelta(hours=73)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        naive = dict(
            decision("naive"),
            published_at=(NOW - timedelta(hours=1)).replace(tzinfo=None),
        )
        outbox = Outbox([old, naive], alert_max_age_hours=72)
        outbox.run(now=NOW)
        self.assertEqual(outbox.statuses("old"), ["expired"])
        self.assertEqual(outbox.statuses("naive"), ["dry_run"])

    def test_destination_none_skips_once(self):
        outbox = Outbox(
            [decision("a", destination="none")],
            notification_destination="none",
            notification_dry_run=False,
        )
        sender = Senders()
        for _ in range(3):
            outbox.run(sender)
        self.assertEqual(outbox.statuses("a"), ["skipped_no_destination"])
        self.assertEqual(sender.calls, [])

    def test_other_destination_is_left_untouched(self):
        outbox = Outbox(
            [decision("a", destination="webhook"), decision("b", destination="none")],
            notification_destination="slack_webhook",
            notification_dry_run=False,
        )
        sender = Senders()
        self.assertEqual(outbox.run(sender), [])
        self.assertEqual(outbox.run(sender), [])
        self.assertEqual(outbox.attempts, [])
        self.assertEqual(sender.calls, [])

    def test_dry_run_does_not_use_live_attempts(self):
        outbox = Outbox([decision("a")])
        outbox.run()
        outbox.vars["notification_dry_run"] = False
        rows = outbox.run(Senders([failure()]))
        self.assertEqual(
            [(r["status"], r["attempt_number"]) for r in rows], [("failed", 1)]
        )

    def test_summarize(self):
        rows = [
            {"record_kind": "attempt", "alert_key": "a", "status": "failed"},
            {"record_kind": "attempt", "alert_key": "a", "status": "delivered"},
            {"record_kind": "attempt", "alert_key": "b", "status": "dry_run"},
            {"record_kind": "attempt", "alert_key": "c", "status": "expired"},
            {
                "record_kind": "attempt",
                "alert_key": "d",
                "status": "skipped_no_destination",
            },
            {"record_kind": "run_summary", "alert_key": "e", "status": "delivered"},
        ]
        history = summarize(rows)
        self.assertEqual(history["a"], AlertHistory(delivered=True, live_attempts=2))
        self.assertTrue(history["b"].dry_run_recorded)
        self.assertTrue(history["c"].terminal)
        self.assertTrue(history["d"].terminal)
        self.assertNotIn("e", history)


class ExecuteTest(unittest.TestCase):
    def test_failed_and_delivered_rows(self):
        plan = [
            (decision("a"), "live", 1),
            (decision("b"), "live", 2),
            (decision("c"), "live", 1),
        ]
        sender = Senders([failure(503), None, ValueError("bad payload")])
        rows = execute(plan, http=None, url=URL, run_id="run-9", now=NOW, sender=sender)
        by_key = {r["alert_key"]: r for r in rows}
        self.assertEqual(
            (by_key["a"]["status"], by_key["a"]["http_status"]), ("failed", 503)
        )
        self.assertIn("HTTP 503", by_key["a"]["error_message"])
        self.assertEqual(
            (by_key["b"]["status"], by_key["b"]["http_status"]), ("delivered", 200)
        )
        self.assertIsNone(by_key["b"]["error_message"])
        self.assertEqual(
            (by_key["c"]["status"], by_key["c"]["http_status"]), ("failed", None)
        )
        self.assertEqual(by_key["c"]["error_message"], "bad payload")
        for row in rows:
            self.assertEqual(row["record_kind"], "attempt")
            self.assertEqual(row["run_id"], "run-9")
            self.assertEqual(row["destination"], "slack_webhook")
            self.assertEqual(row["attempted_at"], NOW.replace(tzinfo=None))
        self.assertEqual(sender.calls[0], ("a", URL, "slack_webhook"))

    def test_non_live_kinds(self):
        plan = [
            (decision("a"), "skip", 0),
            (decision("b"), "expired", 0),
            (decision("c"), "dry_run", 0),
        ]
        sender = Senders()
        rows = execute(plan, http=None, url=URL, run_id="r", now=NOW, sender=sender)
        self.assertEqual(
            [r["status"] for r in rows],
            ["skipped_no_destination", "expired", "dry_run"],
        )
        self.assertTrue(all(r["http_status"] is None for r in rows))
        self.assertEqual(sender.calls, [])

    def test_attempt_keys_are_deterministic(self):
        self.assertEqual(attempt_key("a", "live", 1), attempt_key("a", "live", 1))
        self.assertNotEqual(attempt_key("a", "live", 1), attempt_key("a", "live", 2))
        self.assertNotEqual(attempt_key("a", "live", 1), attempt_key("b", "live", 1))
        self.assertNotEqual(attempt_key("a", "dry_run"), attempt_key("a", "expired"))
        plan = [(decision("a"), "live", 1), (decision("b"), "dry_run", 0)]
        first = execute(
            plan, http=None, url=URL, run_id="r1", now=NOW, sender=Senders()
        )
        second = execute(
            plan,
            http=None,
            url=URL,
            run_id="r2",
            now=NOW + timedelta(hours=1),
            sender=Senders([failure()]),
        )
        self.assertEqual(
            [r["attempt_key"] for r in first], [r["attempt_key"] for r in second]
        )
        self.assertEqual(first[0]["attempt_key"], attempt_key("a", "live", 1))


class FormatSlackTest(unittest.TestCase):
    def payload(self, **extra):
        return json.loads(decision("0123456789abcdef", **extra)["payload_json"])

    def test_rule_based(self):
        text = format_slack(self.payload())["text"]
        self.assertIn("rule-based score", text)
        self.assertNotIn("model-assessed", text)
        self.assertIn("Reply manually on the source platform", text)
        self.assertIn("alert 0123456789ab)", text)
        self.assertIn(
            "<https://www.reddit.com/r/dataengineering/comments/0123456789abcdef/|Post 0123456789abcdef>",
            text,
        )
        self.assertNotIn("Data warnings", text)

    def test_model_generated(self):
        text = format_slack(
            self.payload(model_generated=True, data_warnings="llm_error")
        )["text"]
        self.assertIn("model-assessed (verify before acting)", text)
        self.assertIn("Data warnings: llm_error", text)
        self.assertIn("Reply manually", text)

    def test_long_title_is_truncated(self):
        text = format_slack(self.payload(title="t" * 400))["text"]
        self.assertIn("|" + "t" * 150 + ">", text)
        self.assertNotIn("t" * 151, text)


class SendTest(unittest.TestCase):
    def test_slack_request(self):
        transport = ScriptedTransport(json_response({}))
        d = decision("key-1")
        send(d, make_client(transport), URL, "slack_webhook")
        request = transport.requests[0]
        self.assertEqual(request["method"], "POST")
        self.assertEqual(request["url"], URL)
        self.assertEqual(request["headers"]["Idempotency-Key"], "key-1")
        self.assertEqual(request["headers"]["X-Payload-Version"], "v1")
        self.assertEqual(request["headers"]["Content-Type"], "application/json")
        self.assertEqual(
            json.loads(request["body"]), format_slack(json.loads(d["payload_json"]))
        )

    def test_webhook_request_sends_payload(self):
        transport = ScriptedTransport(json_response({}))
        d = decision("key-2", destination="webhook")
        d["payload_json"] = json.loads(
            d["payload_json"]
        )  # dict payloads are accepted too
        send(d, make_client(transport), URL, "webhook")
        self.assertEqual(json.loads(transport.requests[0]["body"]), d["payload_json"])
        self.assertEqual(transport.requests[0]["headers"]["Idempotency-Key"], "key-2")

    def test_retries_reuse_the_idempotency_key(self):
        transport = ScriptedTransport(
            json_response({}, status=503),
            json_response({}, status=503),
            json_response({}),
        )
        rows = execute(
            [(decision("key-3"), "live", 1)],
            http=make_client(transport, max_retries=2),
            url=URL,
            run_id="r",
            now=NOW,
        )
        self.assertEqual(rows[0]["status"], "delivered")
        self.assertEqual(
            [r["headers"]["Idempotency-Key"] for r in transport.requests], ["key-3"] * 3
        )

    def test_execute_with_real_sender_records_http_status(self):
        transport = ScriptedTransport(json_response({"error": "gone"}, status=410))
        rows = execute(
            [(decision("key-4"), "live", 1)],
            http=make_client(transport),
            url=URL,
            run_id="r",
            now=NOW,
        )
        self.assertEqual((rows[0]["status"], rows[0]["http_status"]), ("failed", 410))


if __name__ == "__main__":
    unittest.main()


class SlackEscapingTest(unittest.TestCase):
    def test_content_cannot_ping_or_fake_links(self):
        from social_listening.delivery import format_slack

        text = format_slack(
            {
                "source": "reddit",
                "content_type": "post",
                "url": "https://www.reddit.com/r/x/comments/1/",
                "title": "<!channel> see <https://evil.example|docs> & more",
                "matched_text": "Example Co",
                "intent": "question",
            }
        )["text"]
        self.assertNotIn("<!channel>", text)
        self.assertNotIn("<https://evil.example|docs>", text)
        self.assertIn("&lt;!channel&gt;", text)
        self.assertIn("&amp; more", text)
