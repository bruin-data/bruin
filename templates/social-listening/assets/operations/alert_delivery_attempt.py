"""@bruin
name: operations.alert_delivery_attempt
type: python
description: |
  Delivers routed alerts to the configured webhook or Slack incoming webhook
  and records every attempt. Idempotent: an alert with a delivered attempt is
  never sent again, dry-run mode records one dry_run row per alert and sends
  nothing, live alerts are attempted at most max_delivery_attempts times (one
  per run, highest priority first, capped at max_alerts_per_run), and items
  older than alert_max_age_hours are marked expired instead of being sent late.
  Requests carry the alert key as an Idempotency-Key header. Delivery never
  touches the source platform.
tags: [enrich, routing, delivery]
depends:
  - operations.fct_alert_decision
materialization:
  type: table
  strategy: merge
secrets:
  - key: sl-webhook-url
    inject_as: SOCIAL_LISTENING_WEBHOOK_URL
  - key: sl-slack-webhook-url
    inject_as: SOCIAL_LISTENING_SLACK_WEBHOOK_URL
columns:
  - name: attempt_key
    type: varchar
    primary_key: true
    checks:
      - name: not_null
  - name: record_kind
    type: varchar
    checks:
      - name: accepted_values
        value: [attempt, run_summary]
  - name: alert_key
    type: varchar
  - name: destination
    type: varchar
  - name: attempt_number
    type: integer
  - name: status
    type: varchar
    checks:
      - name: accepted_values
        value: [delivered, failed, dry_run, expired, skipped_no_destination, completed]
  - name: http_status
    type: integer
  - name: error_message
    type: varchar
  - name: attempted_at
    type: timestamp
  - name: run_id
    type: varchar
custom_checks:
  - name: an alert is delivered at most once
    query: |
      SELECT COUNT(*) FROM (
        SELECT alert_key FROM operations.alert_delivery_attempt
        WHERE record_kind = 'attempt' AND status = 'delivered'
        GROUP BY alert_key HAVING COUNT(*) > 1
      )
    value: 0
@bruin"""

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from social_listening import delivery, warehouse  # noqa: E402
from social_listening.http import HttpClient, UrllibTransport  # noqa: E402
from social_listening.frames import to_frame  # noqa: E402
from social_listening.settings import load_context  # noqa: E402


COLUMNS = [
    {"name": "attempt_key", "type": "varchar"},
    {"name": "record_kind", "type": "varchar"},
    {"name": "alert_key", "type": "varchar"},
    {"name": "destination", "type": "varchar"},
    {"name": "attempt_number", "type": "integer"},
    {"name": "status", "type": "varchar"},
    {"name": "http_status", "type": "integer"},
    {"name": "error_message", "type": "varchar"},
    {"name": "attempted_at", "type": "timestamp"},
    {"name": "run_id", "type": "varchar"},
]


def materialize():
    ctx = load_context()
    v = ctx.vars
    now = datetime.now(timezone.utc).replace(microsecond=0)
    decisions = warehouse.read(
        "SELECT alert_key, content_id, destination, payload_version, payload_json, priority, published_at "
        "FROM operations.fct_alert_decision"
    )
    previous = []
    if warehouse.table_exists("operations", "alert_delivery_attempt"):
        previous = warehouse.read(
            "SELECT alert_key, record_kind, status FROM operations.alert_delivery_attempt"
        )
    plan = delivery.plan_attempts(decisions, delivery.summarize(previous), v, now)

    destination = v.get("notification_destination", "none")
    url = ctx.secret(
        "slack_webhook_url" if destination == "slack_webhook" else "webhook_url"
    )
    http = HttpClient(
        UrllibTransport(),
        min_interval_seconds=1.0,
        max_retries=int(v.get("http_max_retries", 3)),
    )
    rows = delivery.execute(plan, http=http, url=url, run_id=ctx.run_id, now=now)

    counts = {}
    for row in rows:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    summary = {
        "dry_run": bool(v.get("notification_dry_run", True)),
        "destination": destination,
        "decisions": len(decisions),
        **counts,
    }
    rows.append(
        {
            "attempt_key": hashlib.sha256(f"run\x1f{ctx.run_id}".encode()).hexdigest(),
            "record_kind": "run_summary",
            "alert_key": None,
            "destination": destination,
            "attempt_number": 0,
            "status": "completed",
            "http_status": None,
            "error_message": json.dumps(summary),
            "attempted_at": now.replace(tzinfo=None),
            "run_id": ctx.run_id,
        }
    )
    print(f"delivery: {json.dumps(summary)}")
    return to_frame(rows, COLUMNS)
