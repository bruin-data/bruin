"""Entry point shared by the ``raw.raw_<source>_content`` Python assets."""

from __future__ import annotations

import csv
from typing import Any

from .http import FixtureTransport, HttpClient, UrllibTransport
from .settings import TEMPLATE_ROOT, RunContext, load_context
from .sources import exports, github, hackernews, reddit, stackexchange
from .sources.base import collect_rows

# source name -> (collector class, fixture router, enable flag, seconds between requests)
SOURCES = {
    "reddit": (reddit.RedditCollector, reddit.fixture_route, "reddit_enabled", 1.0),
    "hackernews": (
        hackernews.HackerNewsCollector,
        hackernews.fixture_route,
        "hackernews_enabled",
        0.25,
    ),
    "github": (github.GitHubCollector, github.fixture_route, "github_enabled", 2.1),
    "stackoverflow": (
        stackexchange.StackExchangeCollector,
        stackexchange.fixture_route,
        "stackoverflow_enabled",
        0.5,
    ),
    "authorised_export": (
        exports.ExportFileCollector,
        None,
        "authorised_export_enabled",
        0.0,
    ),
}


REDACTIONS = TEMPLATE_ROOT / "assets" / "config" / "redaction_requests.csv"


def redacted_ids(source: str, path=REDACTIONS) -> set[str]:
    """External IDs with a redaction request for this source; never written to raw again."""
    if not path.exists():
        return set()
    with path.open(newline="", encoding="utf-8") as handle:
        return {
            row["external_id"].strip()
            for row in csv.DictReader(handle)
            if row.get("source", "").strip() == source
        }


def build_http(ctx: RunContext, route, min_interval: float) -> tuple[HttpClient, str]:
    demo = bool(ctx.vars.get("demo_mode", True))
    if demo:
        transport = FixtureTransport(
            TEMPLATE_ROOT / "fixtures", ctx.as_of, route or (lambda m, u: None)
        )
        min_interval = 0.0
    else:
        transport = UrllibTransport()
    user_agent = (
        ctx.secret("reddit_user_agent") or "social-listening-template/1.0 (bruin)"
    )
    client = HttpClient(
        transport,
        min_interval_seconds=min_interval,
        max_retries=int(ctx.vars.get("http_max_retries", 3)),
        user_agent=user_agent,
    )
    return client, "demo" if demo else "live"


def stored_keys(source: str, ctx: RunContext) -> set[str]:
    """Event keys already in raw for this window, so reruns never rewrite a row."""
    from . import warehouse

    table = f"raw_{source}_content"
    if not warehouse.table_exists("raw", table):
        return set()
    start = ctx.collection_window().start.replace(tzinfo=None).isoformat(sep=" ")
    rows = warehouse.read(
        f"SELECT event_key FROM raw.{table} WHERE record_kind = 'content' "
        f"AND published_at >= CAST({warehouse.literal(start)} AS TIMESTAMP)"
    )
    return {r["event_key"] for r in rows}


def run_source(
    source: str, ctx: RunContext | None = None, *, lookup_stored: bool = False
) -> list[dict[str, Any]]:
    ctx = ctx or load_context()
    cls, route, flag, min_interval = SOURCES[source]
    http, mode = build_http(ctx, route, min_interval)
    enabled = bool(ctx.vars.get(flag, False))
    collector = cls(ctx, http) if enabled else None
    stored = stored_keys(source, ctx) if (enabled and lookup_stored) else set()
    rows = collect_rows(
        ctx,
        source,
        collector,
        enabled=enabled,
        mode=mode,
        suppressed=redacted_ids(source),
        stored=stored,
    )
    content = sum(1 for r in rows if r["record_kind"] == "content")
    print(
        f"[{source}] mode={mode} enabled={enabled} window={ctx.collection_window().start.isoformat()}..{ctx.collection_window().end.isoformat()} records={content}"
    )
    return rows
