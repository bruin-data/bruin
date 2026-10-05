"""End-to-end checks for the DuckDB demo. Used by tests/run_demo.sh and CI.

Each scenario runs the real pipeline with `bruin run` in a scratch project and
asserts on the warehouse through `bruin query`:

1. validate, then two identical runs: no duplicate raw rows, alerts or attempts;
2. the evaluation fixture matches (true/false positives, ambiguous terms,
   competitor context, exclusions, redaction, window boundary);
3. LLM fixture run with a recorded failure and an invalid answer: failed
   assessments keep NULL model scores and are not routed; no duplicate alerts;
4. live webhook delivery against a local receiver that fails the first two
   requests: retries succeed, nothing is delivered twice, reruns send nothing;
5. policy validation: reply drafts without human approval, and a backfill
   window over source_window_max_days, both fail before any source is called;
6. a bounded two-day backfill succeeds and adds no duplicates.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
TEMPLATE = HERE.parent
sys.path.insert(0, str(HERE))

from evaluate_demo import evaluate, query  # noqa: E402

PIPELINE = "social-listening"


class Demo:
    def __init__(self, bruin: str):
        self.bruin = bruin
        self.dir = Path(tempfile.mkdtemp(prefix="sl-e2e-"))
        shutil.copytree(
            TEMPLATE,
            self.dir / PIPELINE,
            ignore=shutil.ignore_patterns("__pycache__", "*.duckdb*", ".venv"),
        )
        shutil.copy(TEMPLATE / ".bruin.yml", self.dir / ".bruin.yml")
        subprocess.run(["git", "init", "-q"], cwd=self.dir, check=True)
        os.chdir(self.dir)

    def run(self, *args: str, env: dict | None = None, expect_ok: bool = True) -> str:
        cmd = [self.bruin, "run", *args, PIPELINE]
        proc = subprocess.run(
            cmd, capture_output=True, text=True, env={**os.environ, **(env or {})}
        )
        output = proc.stdout + proc.stderr
        if expect_ok and proc.returncode != 0:
            print(output[-6000:])
            raise AssertionError(f"bruin run failed: {' '.join(args)}")
        if not expect_ok and proc.returncode == 0:
            raise AssertionError(f"bruin run was expected to fail: {' '.join(args)}")
        return output

    def q(self, sql: str) -> list[dict]:
        return query(self.bruin, sql)

    def one(self, sql: str):
        rows = self.q(sql)
        return next(iter(rows[0].values())) if rows else None

    def counts(self) -> dict:
        return self.q(
            """
            SELECT
              (SELECT COUNT(*) FROM raw.raw_reddit_content WHERE record_kind = 'content') AS raw_reddit,
              (SELECT COUNT(*) FROM raw.raw_hackernews_content WHERE record_kind = 'content') AS raw_hn,
              (SELECT COUNT(*) FROM raw.raw_reddit_content) - (SELECT COUNT(DISTINCT event_key) FROM raw.raw_reddit_content) AS dup_keys,
              (SELECT COUNT(*) FROM staging.stg_content_item) AS content,
              (SELECT COUNT(*) FROM operations.fct_alert_decision) AS alerts,
              (SELECT COUNT(*) FROM operations.alert_delivery_attempt WHERE record_kind = 'attempt') AS attempts
            """
        )[0]


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"  ok  {message}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main() -> int:
    bruin = sys.argv[1] if len(sys.argv) > 1 else "bruin"
    demo = Demo(bruin)
    print(f"scratch project: {demo.dir}")

    print("1. validate and idempotent reruns")
    subprocess.run([bruin, "validate", PIPELINE], check=True, capture_output=True)
    demo.run()
    first = demo.counts()
    demo.run()
    second = demo.counts()
    check(first == second, f"second run added nothing: {second}")
    check(first["dup_keys"] == 0, "raw event keys are unique")
    check(first["alerts"] == 5, f"five alerts routed ({first['alerts']})")
    check(
        demo.one(
            "SELECT COUNT(*) FROM operations.fct_alert_outbox WHERE status = 'dry_run'"
        )
        == 5,
        "dry run sends nothing",
    )

    print("2. evaluation fixture")
    failures = evaluate(bruin)
    check(
        not failures,
        "evaluation cases match" + ("" if not failures else ": " + "; ".join(failures)),
    )
    queue = demo.q(
        "SELECT source_url, matched_text, rule_ids, reasons_json, relevance, intent_score, fit, engagement, freshness, authenticity, priority, queue_state FROM marts.mart_mention_queue"
    )
    check(
        all(
            r["source_url"]
            and r["matched_text"]
            and r["rule_ids"]
            and r["reasons_json"]
            for r in queue
        ),
        "every queue item exposes URL, matched text, rule and reasons",
    )
    sov = demo.q(
        "SELECT * FROM marts.mart_share_of_voice_monthly WHERE source = 'all' AND term_id = 'brand'"
    )
    check(
        sov
        and sov[0]["self_mentions"] == 1
        and sov[0]["denominator_organic_mentions"] > 0
        and sov[0]["coverage_ratio"] is not None,
        "share of voice separates self content and reports denominator and coverage",
    )

    print("3. model failures never invent scores")
    demo.run("--var", "llm_enabled=true")
    bad = demo.q(
        "SELECT assessment_status, llm_relevance, llm_intent, score_basis FROM enrichment.fct_mention_assessment "
        "WHERE assessment_version LIKE '%fixture%' AND assessment_status IN ('llm_error', 'llm_invalid_response')"
    )
    check(len(bad) == 2, f"one recorded error and one invalid answer ({len(bad)})")
    check(
        all(
            r["llm_relevance"] is None
            and r["llm_intent"] is None
            and r["score_basis"] == "rules_fallback_after_model_failure"
            for r in bad
        ),
        "failed assessments keep NULL model scores",
    )
    check(
        demo.one("SELECT COUNT(*) FROM operations.fct_alert_decision") == 5,
        "model run creates no duplicate alerts",
    )

    print("4. live delivery with retries")
    port = free_port()
    log = demo.dir / "webhook.log"
    server = subprocess.Popen(
        [sys.executable, str(HERE / "fake_webhook.py"), str(port), "2", str(log)]
    )
    try:
        time.sleep(1)
        env = {"SL_WEBHOOK_URL": f"http://127.0.0.1:{port}/hook"}
        live = [
            "--var",
            "notification_dry_run=false",
            "--var",
            'notification_destination="webhook"',
            "--var",
            "http_max_retries=0",
        ]
        for _ in range(3):
            demo.run(*live, env=env)
    finally:
        server.terminate()
    hits = [json.loads(line) for line in log.read_text().splitlines()]
    delivered = [h["key"] for h in hits if h["status"] == 200]
    check(
        len(delivered) == len(set(delivered)) == 5,
        f"each webhook alert delivered exactly once ({len(delivered)})",
    )
    check(sum(1 for h in hits if h["status"] == 503) == 2, "two failures were retried")
    check(
        demo.one(
            "SELECT COUNT(*) FROM operations.fct_alert_outbox WHERE destination = 'webhook' AND status = 'delivered'"
        )
        == 5,
        "outbox shows delivered",
    )
    check(
        demo.one(
            "SELECT COUNT(*) FROM operations.fct_alert_outbox WHERE destination = 'slack_webhook' AND status = 'dry_run'"
        )
        == 5,
        "alerts for the old destination were not sent",
    )

    print("5. policy validation fails fast")
    out = demo.run(
        "--var",
        "reply_drafts_enabled=true",
        "--var",
        "require_human_approval=false",
        expect_ok=False,
    )
    check("require_human_approval=true" in out, "reply drafts require human approval")
    out = demo.run("--var", "demo_mode=false", expect_ok=False)
    check("sl-reddit-client-id" in out, "live Reddit needs credentials")
    end = date.today()
    out = demo.run(
        "--start-date",
        str(end - timedelta(days=20)),
        "--end-date",
        str(end),
        expect_ok=False,
    )
    check("source_window_max_days" in out, "oversized backfill window is rejected")

    print("6. bounded backfill")
    before = demo.counts()
    demo.run(
        "--start-date",
        str(end - timedelta(days=2)),
        "--end-date",
        str(end - timedelta(days=1)),
    )
    after = demo.counts()
    check(after["dup_keys"] == 0, "raw event keys stay unique after the backfill")
    check(after["alerts"] == before["alerts"], "re-collected items raise no new alerts")
    check(
        demo.one(
            "SELECT COUNT(*) - COUNT(DISTINCT content_id) FROM staging.stg_content_item"
        )
        == 0,
        "no duplicate content after backfill",
    )

    print("all end-to-end checks passed")
    shutil.rmtree(demo.dir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
