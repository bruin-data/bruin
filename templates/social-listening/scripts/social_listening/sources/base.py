"""Source-client interface and the shared raw-row writer.

Every source adapter implements ``SourceCollector.collect(window)`` and yields
``RawRecord`` objects. ``collect_rows`` turns those into rows for the source's
``raw.raw_<source>_content`` table:

* one ``content`` row per source record, keyed by an event key of source,
  external ID and a hash of the record's content (engagement counters such as
  score or comment count are left out of the hash). Re-reading a window never
  adds a row: a record already stored under the same key is skipped, so raw
  rows are never rewritten, while an edited title or body becomes a new,
  auditable version;
* one ``run_summary`` row per run with request, retry, rate-limit and error
  counts plus the high watermark. ``marts.mart_pipeline_health`` reads these.

A disabled source still writes its run summary so the table exists and the
health mart can report it as disabled rather than missing.
"""

from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator

from ..http import HttpClient, HttpError
from ..settings import RunContext, Window

RAW_COLUMNS = [
    {"name": "event_key", "type": "varchar", "primary_key": True},
    {"name": "source", "type": "varchar"},
    {"name": "external_id", "type": "varchar"},
    {"name": "record_kind", "type": "varchar"},
    {"name": "content_type", "type": "varchar"},
    {"name": "payload_json", "type": "varchar"},
    {"name": "payload_sha256", "type": "varchar"},
    {"name": "published_at", "type": "timestamp"},
    {"name": "collected_at", "type": "timestamp"},
    {"name": "source_cursor", "type": "varchar"},
    {"name": "ingest_run_id", "type": "varchar"},
    {"name": "window_start", "type": "timestamp"},
    {"name": "window_end", "type": "timestamp"},
    {"name": "collection_mode", "type": "varchar"},
]


class SourceDisabled(Exception):
    """Raised by interface-only sources that cannot be enabled yet."""


@dataclass
class RawRecord:
    source: str
    external_id: str
    content_type: str
    payload: dict[str, Any]
    published_at: datetime
    cursor: str = ""


@dataclass
class CollectStats:
    pages: int = 0
    records: int = 0
    skipped_outside_window: int = 0
    duplicates_in_run: int = 0
    redacted_skipped: int = 0
    already_stored: int = 0
    partial_errors: list[str] = field(default_factory=list)
    high_watermark: datetime | None = None
    cursor: str = ""


class SourceCollector(ABC):
    """Base class for a bounded-window source client."""

    source: str = ""

    def __init__(self, ctx: RunContext, http: HttpClient):
        self.ctx = ctx
        self.http = http
        self.stats = CollectStats()

    @abstractmethod
    def collect(self, window: Window) -> Iterator[RawRecord]:
        """Yield records published inside ``window`` (start inclusive, end exclusive)."""

    def keep(self, record: RawRecord, window: Window) -> bool:
        if not window.contains(record.published_at):
            self.stats.skipped_outside_window += 1
            return False
        return True

    def partial_error(self, scope: str, err: Exception) -> None:
        """Record a non-fatal error (one community or query failed) and continue."""
        message = f"{scope}: {err}"[:300]
        self.stats.partial_errors.append(message)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    )


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _naive_utc(ts: datetime | None) -> datetime | None:
    if ts is None:
        return None
    return ts.astimezone(timezone.utc).replace(tzinfo=None)


# Engagement counters change on every poll; they are stored but not part of the event key.
VOLATILE_FIELDS = frozenset(
    {
        "score",
        "num_comments",
        "points",
        "comments",
        "reactions",
        "view_count",
        "answer_count",
        "metrics",
    }
)


def content_hash(payload: dict[str, Any]) -> str:
    return sha256(
        canonical_json({k: v for k, v in payload.items() if k not in VOLATILE_FIELDS})
    )


def event_key(source: str, external_id: str, payload_hash: str) -> str:
    return sha256(f"{source}\x1f{external_id}\x1f{payload_hash}")


def collect_rows(
    ctx: RunContext,
    source: str,
    collector: SourceCollector | None,
    *,
    enabled: bool,
    mode: str,
    suppressed: set[str] | None = None,
    stored: set[str] | None = None,
) -> list[dict[str, Any]]:
    window = ctx.collection_window()
    collected_at = _naive_utc(datetime.now(timezone.utc).replace(microsecond=0))
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    status = "disabled"
    fatal: str | None = None
    stats = collector.stats if collector else CollectStats()

    if enabled and collector is not None:
        status = "ok"
        try:
            for record in collector.collect(window):
                if not collector.keep(record, window):
                    continue
                if suppressed and record.external_id in suppressed:
                    # Redaction requests apply at the source, so deleted content never returns to raw.
                    stats.redacted_skipped += 1
                    continue
                payload = canonical_json(record.payload)
                digest = sha256(payload)
                key = event_key(
                    source, record.external_id, content_hash(record.payload)
                )
                if key in seen:
                    stats.duplicates_in_run += 1
                    continue
                seen.add(key)
                if stored and key in stored:
                    # Already in raw from an earlier run: keep that immutable row as it is.
                    stats.already_stored += 1
                    continue
                stats.records += 1
                if (
                    stats.high_watermark is None
                    or record.published_at > stats.high_watermark
                ):
                    stats.high_watermark = record.published_at
                rows.append(
                    {
                        "event_key": key,
                        "source": source,
                        "external_id": record.external_id,
                        "record_kind": "content",
                        "content_type": record.content_type,
                        "payload_json": payload,
                        "payload_sha256": digest,
                        "published_at": _naive_utc(record.published_at),
                        "collected_at": collected_at,
                        "source_cursor": record.cursor,
                        "ingest_run_id": ctx.run_id,
                        "window_start": _naive_utc(window.start),
                        "window_end": _naive_utc(window.end),
                        "collection_mode": mode,
                    }
                )
        except HttpError as err:
            # Authentication, permission and exhausted-retry errors stop the source.
            fatal = str(err)
        if fatal is None and stats.partial_errors and stats.pages == 0:
            # Every request failed: that is an outage, not a partial result.
            fatal = f"no request succeeded ({len(stats.partial_errors)} errors); first: {stats.partial_errors[0]}"
        if stats.partial_errors and fatal is None:
            status = "partial"

    http = collector.http.stats if collector else None
    summary = {
        "status": "failed" if fatal else status,
        "fatal_error": fatal,
        "window_start": window.start.isoformat(),
        "window_end": window.end.isoformat(),
        "pages": stats.pages,
        "records": stats.records,
        "skipped_outside_window": stats.skipped_outside_window,
        "duplicates_in_run": stats.duplicates_in_run,
        "redacted_skipped": stats.redacted_skipped,
        "already_stored": stats.already_stored,
        "requests": http.requests if http else 0,
        "retries": http.retries if http else 0,
        "rate_limit_waits": http.rate_limit_waits if http else 0,
        "http_errors": http.errors if http else 0,
        "partial_errors": stats.partial_errors,
        "high_watermark": stats.high_watermark.isoformat()
        if stats.high_watermark
        else None,
        "cursor": stats.cursor,
    }
    payload = canonical_json(summary)
    run_external_id = f"run:{ctx.run_id}"
    rows.append(
        {
            "event_key": event_key(source, run_external_id, sha256(payload)),
            "source": source,
            "external_id": run_external_id,
            "record_kind": "run_summary",
            "content_type": "run_summary",
            "payload_json": payload,
            "payload_sha256": sha256(payload),
            "published_at": _naive_utc(stats.high_watermark),
            "collected_at": collected_at,
            "source_cursor": stats.cursor,
            "ingest_run_id": ctx.run_id,
            "window_start": _naive_utc(window.start),
            "window_end": _naive_utc(window.end),
            "collection_mode": mode,
        }
    )
    if fatal and ctx.vars.get("fail_on_source_error", True):
        # Default: fail the asset so the run is visibly red. With
        # fail_on_source_error=false the failed summary is stored instead and
        # marts.mart_pipeline_health reports the source as failing.
        raise RuntimeError(f"{source} collection failed: {fatal}")
    return rows
