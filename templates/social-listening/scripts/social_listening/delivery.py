"""Deliver routed alerts from the outbox, idempotently.

``plan_attempts`` is a pure function: given routing decisions and every earlier
attempt, it decides what this run does for each alert key.

* An alert with a ``delivered`` attempt is never sent again.
* With ``notification_dry_run=true`` each alert gets exactly one ``dry_run``
  row and nothing leaves the machine.
* A live alert is attempted at most ``max_delivery_attempts`` times, once per
  run, highest priority first, and at most ``max_alerts_per_run`` per run.
* Alerts about content published more than ``alert_max_age_hours`` ago are
  marked ``expired`` instead of being sent late, so a backfill never floods
  the channel with old conversations.
* Destination ``none`` records ``skipped_no_destination`` once.
* Alerts routed for a different destination than the current one are left
  untouched, so changing the destination never re-sends old alerts.

Each request carries the alert key as ``Idempotency-Key`` so a receiver can
drop a duplicate if a run dies between sending and recording the attempt.
Delivery only posts to the configured webhook; it never touches the source.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Mapping

from .http import HttpClient, HttpError


def attempt_key(alert_key: str, kind: str, number: int = 0) -> str:
    return hashlib.sha256(f"{alert_key}\x1f{kind}\x1f{number}".encode()).hexdigest()


@dataclass
class AlertHistory:
    delivered: bool = False
    dry_run_recorded: bool = False
    terminal: bool = False  # expired or skipped
    live_attempts: int = 0


def summarize(attempts: Iterable[Mapping[str, Any]]) -> dict[str, AlertHistory]:
    history: dict[str, AlertHistory] = {}
    for row in attempts:
        if row.get("record_kind") != "attempt":
            continue
        h = history.setdefault(row["alert_key"], AlertHistory())
        status = row.get("status")
        if status == "delivered":
            h.delivered = True
        if status == "dry_run":
            h.dry_run_recorded = True
        if status in ("expired", "skipped_no_destination"):
            h.terminal = True
        if status in ("delivered", "failed"):
            h.live_attempts += 1
    return history


def _as_utc(value: Any) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value


def plan_attempts(
    decisions: list[Mapping[str, Any]],
    history: Mapping[str, AlertHistory],
    v: Mapping[str, Any],
    now: datetime,
) -> list[tuple[Mapping[str, Any], str, int]]:
    """Return (decision, kind, attempt_number) for work to do this run."""
    dry_run = bool(v.get("notification_dry_run", True))
    max_attempts = int(v.get("max_delivery_attempts", 3))
    max_per_run = int(v.get("max_alerts_per_run", 20))
    max_age = timedelta(hours=int(v.get("alert_max_age_hours", 72)))
    destination = v.get("notification_destination", "none")
    plan: list[tuple[Mapping[str, Any], str, int]] = []
    live = 0
    for decision in sorted(
        decisions, key=lambda d: (-(d.get("priority") or 0), d["alert_key"])
    ):
        h = history.get(decision["alert_key"], AlertHistory())
        if h.delivered or h.terminal:
            continue
        if decision.get("destination") != destination:
            # Routed for a destination that is no longer configured; leave it untouched.
            continue
        if decision.get("destination") == "none":
            plan.append((decision, "skip", 0))
            continue
        if now - _as_utc(decision["published_at"]) > max_age:
            plan.append((decision, "expired", 0))
            continue
        if dry_run:
            if not h.dry_run_recorded:
                plan.append((decision, "dry_run", 0))
            continue
        if h.live_attempts >= max_attempts or live >= max_per_run:
            continue
        live += 1
        plan.append((decision, "live", h.live_attempts + 1))
    return plan


def format_slack(payload: Mapping[str, Any]) -> dict[str, Any]:
    basis = (
        "model-assessed (verify before acting)"
        if payload.get("model_generated")
        else "rule-based score"
    )
    lines = [
        f"*Social listening* · {payload.get('source')} {payload.get('content_type')} · priority {payload.get('priority')} ({basis})",
        f"<{payload.get('url')}|{(payload.get('title') or payload.get('snippet') or 'open')[:150]}>",
        f"Matched: {payload.get('matched_text')} · rule {payload.get('rule_ids')}",
        f"Intent: {payload.get('intent')} · relevance {payload.get('relevance')} · confidence {payload.get('confidence')}",
    ]
    if payload.get("data_warnings"):
        lines.append(f"Data warnings: {payload.get('data_warnings')}")
    lines.append(
        f"Review in the queue (alert {str(payload.get('alert_key'))[:12]}). Reply manually on the source platform."
    )
    return {"text": "\n".join(lines)}


def send(
    decision: Mapping[str, Any], http: HttpClient, url: str, destination: str
) -> None:
    payload = (
        json.loads(decision["payload_json"])
        if isinstance(decision["payload_json"], str)
        else dict(decision["payload_json"])
    )
    body = format_slack(payload) if destination == "slack_webhook" else payload
    http.request(
        "POST",
        url,
        json_body=body,
        headers={
            "Idempotency-Key": decision["alert_key"],
            "X-Payload-Version": str(decision.get("payload_version", "v1")),
        },
    )


def execute(
    plan: list[tuple[Mapping[str, Any], str, int]],
    *,
    http: HttpClient | None,
    url: str,
    run_id: str,
    now: datetime,
    sender: Callable[..., None] = send,
) -> list[dict[str, Any]]:
    rows = []
    attempted_at = now.astimezone(timezone.utc).replace(tzinfo=None)
    for decision, kind, number in plan:
        status, http_status, error = {
            "skip": ("skipped_no_destination", None, None),
            "expired": ("expired", None, None),
            "dry_run": ("dry_run", None, None),
        }.get(kind, (None, None, None))
        if kind == "live":
            try:
                sender(decision, http, url, decision["destination"])
                status, http_status = "delivered", 200
            except HttpError as err:
                status, http_status, error = "failed", err.status, str(err)[:300]
            except Exception as err:  # noqa: BLE001 - a bad payload must not stop the batch
                status, error = "failed", str(err)[:300]
        rows.append(
            {
                "attempt_key": attempt_key(decision["alert_key"], kind, number),
                "record_kind": "attempt",
                "alert_key": decision["alert_key"],
                "destination": decision["destination"],
                "attempt_number": number,
                "status": status,
                "http_status": http_status,
                "error_message": error,
                "attempted_at": attempted_at,
                "run_id": run_id,
            }
        )
    return rows
