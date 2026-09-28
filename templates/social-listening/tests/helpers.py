"""Shared test helpers: import path, pipeline.yml defaults, fakes.

Everything here is standard library. No test touches the network or sleeps:
HTTP goes through fake transports and ``FakeClock`` replaces ``time.sleep``
and ``time.monotonic``.
"""

from __future__ import annotations

import copy
import json
import sys
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections.abc import Callable
from typing import Any

TEMPLATE_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = TEMPLATE_ROOT / "scripts"
FIXTURES = TEMPLATE_ROOT / "fixtures"
PIPELINE_YML = TEMPLATE_ROOT / "pipeline.yml"

if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from social_listening.http import (  # noqa: E402
    FixtureTransport,
    HttpClient,
    Response,
    Transport,
)
from social_listening.settings import RunContext  # noqa: E402

# End of the default test interval. Fixture times are relative to this.
ANCHOR = datetime(2026, 9, 28, 0, 0, tzinfo=timezone.utc)

# --------------------------------------------------------------------------- defaults
# Mirrors the ``default:`` values in pipeline.yml. test_settings checks that the
# variable names (and scalar defaults) stay in sync with the file.

_DEFAULT_VARS: dict[str, Any] = {
    # brand and terms
    "brand_name": "Example Co",
    "brand_description": (
        "Example Co makes a workflow tool for small data teams. Ideal customers run a "
        "warehouse and want fewer manual steps. Exclude job posts and students. Never "
        "claim guaranteed savings or certifications."
    ),
    "brand_aliases": ["exampleco"],
    "competitors": ["Rival Suite"],
    "tracked_terms": ["example alternative"],
    "term_rules": [
        {
            "term_id": "topic-social-listening",
            "phrases": ["social listening"],
            "category": "topic",
            "platforms": [],
            "requires_context_any": ["tool", "alerts", "workflow", "monitor"],
            "excludes_context_any": ["therapy", "relationship", "counselling"],
            "valid_from": "2026-01-01",
            "valid_to": "2099-12-31",
            "active": True,
        }
    ],
    "excluded_terms": ["we're hiring", "job opening"],
    "matcher_version": "v1",
    # sources
    "demo_mode": True,
    "reddit_enabled": True,
    "reddit_communities_include": [],
    "reddit_communities_exclude": [],
    "reddit_include_comments": True,
    "reddit_poll_minutes": 15,
    "hackernews_enabled": True,
    "hackernews_include_comments": True,
    "github_enabled": False,
    "stackoverflow_enabled": False,
    "slack_community_enabled": False,
    "authorised_export_enabled": False,
    "authorised_export_path": "fixtures/exports/sample_export.csv",
    "late_arrival_hours": 6,
    "source_window_max_days": 7,
    "max_pages_per_source": 5,
    "http_max_retries": 3,
    "fail_on_source_error": True,
    # exclusions
    "excluded_authors": [],
    "team_authors": ["exampleco_team"],
    "route_team_content": False,
    "bot_author_patterns": ["(?i)^automoderator$", "(?i)bot$", "(?i)^\\[deleted\\]$"],
    "supported_content_types": [
        "post",
        "comment",
        "story",
        "issue",
        "pull_request",
        "question",
        "export_item",
    ],
    "max_content_age_days": 30,
    "author_enrichment_enabled": False,
    # scoring
    "intent_taxonomy": [
        {
            "intent": "seeking_recommendation",
            "score": 0.95,
            "keywords": [
                "alternative to",
                "recommend",
                "looking for",
                "any suggestions",
                "what do you use",
            ],
        },
        {
            "intent": "comparison",
            "score": 0.85,
            "keywords": [
                " vs ",
                "versus",
                "compared to",
                "switching from",
                "migrate from",
            ],
        },
        {
            "intent": "problem_report",
            "score": 0.7,
            "keywords": [
                "not working",
                "broken",
                "issue with",
                "frustrated",
                "keeps failing",
            ],
        },
        {
            "intent": "question",
            "score": 0.6,
            "keywords": ["how do i", "how to", "does anyone know", "?"],
        },
        {
            "intent": "praise",
            "score": 0.35,
            "keywords": ["love", "great experience", "impressed"],
        },
    ],
    "fit_keywords": ["warehouse", "data team", "pipeline", "small team", "startup"],
    "disqualifying_keywords": ["homework", "internship", "for my class"],
    "priority_weights": {
        "relevance": 0.3,
        "intent": 0.25,
        "fit": 0.15,
        "engagement": 0.1,
        "freshness": 0.1,
        "authenticity": 0.1,
    },
    "category_relevance": {"brand": 0.9, "competitor": 0.85, "topic": 0.75},
    "freshness_half_life_hours": 48,
    "engagement_saturation": 200,
    "min_relevance": 0.70,
    "min_priority": 0.65,
    "min_confidence": 0.50,
    "min_intent_score": 0.50,
    "scoring_version": "v1",
    # model
    "llm_enabled": False,
    "llm_provider": "fixture",
    "llm_model": "",
    "llm_base_url": "",
    "llm_prompt_version": "v1",
    "llm_max_items_per_run": 25,
    "llm_max_attempts": 3,
    "llm_timeout_seconds": 30,
    "route_on_llm_error": False,
    # notifications
    "notification_destination": "slack_webhook",
    "notification_dry_run": True,
    "max_delivery_attempts": 3,
    "max_alerts_per_run": 20,
    "alert_max_age_hours": 72,
    "payload_version": "v1",
    "routing_policy_version": "v1",
    # replies and mutations
    "reply_drafts_enabled": False,
    "require_human_approval": True,
    "reply_eligible_intents": ["seeking_recommendation", "comparison", "question"],
    "reply_disclosure": "Disclosure: I work on {brand}.",
    "reply_claims_to_avoid": ["guaranteed", "certified", "best in the world"],
    "allow_mutations": False,
    # health
    "stale_after_minutes": 180,
    "volume_anomaly_ratio": 4,
}

_STR = {"type": "string"}
_BOOL = {"type": "boolean"}
_STR_ARRAY = {"type": "array", "items": _STR}
_UNIT = {"type": "number", "minimum": 0, "maximum": 1}
_DATE = {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"}


def _int(lo: int, hi: int | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"type": "integer", "minimum": lo}
    if hi is not None:
        out["maximum"] = hi
    return out


def _num(lo: float, hi: float | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"type": "number", "minimum": lo}
    if hi is not None:
        out["maximum"] = hi
    return out


# The JSON Schema Bruin passes as BRUIN_VARS_SCHEMA, mirroring pipeline.yml.
_SCHEMA_PROPERTIES: dict[str, Any] = {
    "brand_name": {"type": "string", "minLength": 1},
    "brand_description": _STR,
    "brand_aliases": _STR_ARRAY,
    "competitors": _STR_ARRAY,
    "tracked_terms": _STR_ARRAY,
    "term_rules": {
        "type": "array",
        "items": {
            "type": "object",
            "required": ["term_id", "phrases", "category"],
            "properties": {
                "term_id": {"type": "string", "pattern": "^[a-z0-9][a-z0-9_-]*$"},
                "phrases": {"type": "array", "items": _STR, "minItems": 1},
                "category": {
                    "type": "string",
                    "enum": ["brand", "competitor", "topic"],
                },
                "platforms": _STR_ARRAY,
                "requires_context_any": _STR_ARRAY,
                "excludes_context_any": _STR_ARRAY,
                "valid_from": _DATE,
                "valid_to": _DATE,
                "active": _BOOL,
            },
        },
    },
    "excluded_terms": _STR_ARRAY,
    "matcher_version": _STR,
    "demo_mode": _BOOL,
    "reddit_enabled": _BOOL,
    "reddit_communities_include": _STR_ARRAY,
    "reddit_communities_exclude": _STR_ARRAY,
    "reddit_include_comments": _BOOL,
    "reddit_poll_minutes": _int(5, 1440),
    "hackernews_enabled": _BOOL,
    "hackernews_include_comments": _BOOL,
    "github_enabled": _BOOL,
    "stackoverflow_enabled": _BOOL,
    "slack_community_enabled": _BOOL,
    "authorised_export_enabled": _BOOL,
    "authorised_export_path": _STR,
    "late_arrival_hours": _int(0, 168),
    "source_window_max_days": _int(1, 31),
    "max_pages_per_source": _int(1, 50),
    "http_max_retries": _int(0, 8),
    "fail_on_source_error": _BOOL,
    "excluded_authors": _STR_ARRAY,
    "team_authors": _STR_ARRAY,
    "route_team_content": _BOOL,
    "bot_author_patterns": _STR_ARRAY,
    "supported_content_types": {
        "type": "array",
        "items": {
            "type": "string",
            "enum": [
                "post",
                "comment",
                "story",
                "issue",
                "pull_request",
                "question",
                "export_item",
            ],
        },
    },
    "max_content_age_days": _int(1, 365),
    "author_enrichment_enabled": _BOOL,
    "intent_taxonomy": {
        "type": "array",
        "minItems": 1,
        "items": {
            "type": "object",
            "required": ["intent", "keywords", "score"],
            "properties": {
                "intent": {"type": "string", "pattern": "^[a-z][a-z_]*$"},
                "keywords": {"type": "array", "items": _STR, "minItems": 1},
                "score": _UNIT,
            },
        },
    },
    "fit_keywords": _STR_ARRAY,
    "disqualifying_keywords": _STR_ARRAY,
    "priority_weights": {
        "type": "object",
        "required": [
            "relevance",
            "intent",
            "fit",
            "engagement",
            "freshness",
            "authenticity",
        ],
        "properties": {
            k: _UNIT
            for k in (
                "relevance",
                "intent",
                "fit",
                "engagement",
                "freshness",
                "authenticity",
            )
        },
    },
    "category_relevance": {
        "type": "object",
        "required": ["brand", "competitor", "topic"],
        "properties": {k: _UNIT for k in ("brand", "competitor", "topic")},
    },
    "freshness_half_life_hours": _num(1, 720),
    "engagement_saturation": _num(1),
    "min_relevance": _UNIT,
    "min_priority": _UNIT,
    "min_confidence": _UNIT,
    "min_intent_score": _UNIT,
    "scoring_version": _STR,
    "llm_enabled": _BOOL,
    "llm_provider": {
        "type": "string",
        "enum": ["fixture", "anthropic", "openai_compatible"],
    },
    "llm_model": _STR,
    "llm_base_url": _STR,
    "llm_prompt_version": _STR,
    "llm_max_items_per_run": _int(1, 500),
    "llm_max_attempts": _int(1, 5),
    "llm_timeout_seconds": _int(5, 120),
    "route_on_llm_error": _BOOL,
    "notification_destination": {
        "type": "string",
        "enum": ["none", "webhook", "slack_webhook"],
    },
    "notification_dry_run": _BOOL,
    "max_delivery_attempts": _int(1, 10),
    "max_alerts_per_run": _int(1, 200),
    "alert_max_age_hours": _int(1, 720),
    "payload_version": {"type": "string", "enum": ["v1"]},
    "routing_policy_version": _STR,
    "reply_drafts_enabled": _BOOL,
    "require_human_approval": _BOOL,
    "reply_eligible_intents": _STR_ARRAY,
    "reply_disclosure": {"type": "string", "minLength": 1},
    "reply_claims_to_avoid": _STR_ARRAY,
    "allow_mutations": _BOOL,
    "stale_after_minutes": _int(15, 10080),
    "volume_anomaly_ratio": _num(1.5, 100),
}


def default_vars(**overrides: Any) -> dict[str, Any]:
    """A fresh copy of the pipeline.yml defaults with ``overrides`` applied."""
    values = copy.deepcopy(_DEFAULT_VARS)
    values.update(copy.deepcopy(overrides))
    return values


def default_schema() -> dict[str, Any]:
    return {"type": "object", "properties": copy.deepcopy(_SCHEMA_PROPERTIES)}


def make_ctx(
    overrides: dict[str, Any] | None = None,
    *,
    env: dict[str, str] | None = None,
    end: datetime = ANCHOR,
    hours: float = 24,
    run_id: str = "run-1",
) -> RunContext:
    """A RunContext for an interval of ``hours`` ending at ``end``."""
    return RunContext(
        vars=default_vars(**(overrides or {})),
        schema=default_schema(),
        run_id=run_id,
        interval_start=end - timedelta(hours=hours),
        interval_end=end,
        env=dict(env or {}),
    )


# --------------------------------------------------------------------------- fakes


class FakeClock:
    """Monotonic clock whose ``sleep`` advances time instead of blocking."""

    def __init__(self, start: float = 1000.0):
        self.now = start
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds

    def advance(self, seconds: float) -> None:
        self.now += seconds


def json_response(
    body: Any, status: int = 200, headers: dict[str, str] | None = None
) -> Response:
    return Response(
        status,
        {k.lower(): v for k, v in (headers or {}).items()},
        json.dumps(body).encode(),
    )


class ScriptedTransport(Transport):
    """Returns queued responses in order (the last one repeats) and records requests."""

    def __init__(
        self,
        *responses: Response | Exception,
        clock: FakeClock | None = None,
        latency: float = 0.0,
    ):
        self.responses = list(responses)
        self.requests: list[dict[str, Any]] = []
        self.clock = clock
        self.latency = latency

    def send(self, method, url, headers, body, timeout):
        self.requests.append(
            {"method": method, "url": url, "headers": dict(headers), "body": body}
        )
        if self.clock is not None and self.latency:
            self.clock.advance(self.latency)
        item = self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
        if isinstance(item, Exception):
            raise item
        return item


class FunctionTransport(Transport):
    """Answers each request with ``handler(method, url, headers, body) -> Response``."""

    def __init__(self, handler: Callable[..., Response]):
        self.handler = handler
        self.requests: list[dict[str, Any]] = []

    def send(self, method, url, headers, body, timeout):
        self.requests.append(
            {"method": method, "url": url, "headers": dict(headers), "body": body}
        )
        return self.handler(method, url, headers, body)


class MutatingTransport(Transport):
    """Wraps another transport and rewrites JSON bodies with ``mutate(url, body)``."""

    def __init__(self, inner: Transport, mutate: Callable[[str, Any], Any]):
        self.inner = inner
        self.mutate = mutate

    @property
    def requests(self):
        return self.inner.requests

    def send(self, method, url, headers, body, timeout):
        resp = self.inner.send(method, url, headers, body, timeout)
        if resp.status >= 400 or not resp.body:
            return resp
        return Response(
            resp.status,
            resp.headers,
            json.dumps(self.mutate(url, resp.json())).encode(),
        )


class RecordingTransport(Transport):
    """Wraps another transport and records method, URL, headers and body."""

    def __init__(self, inner: Transport):
        self.inner = inner
        self.calls: list[dict[str, Any]] = []

    def send(self, method, url, headers, body, timeout):
        self.calls.append(
            {"method": method, "url": url, "headers": dict(headers), "body": body}
        )
        return self.inner.send(method, url, headers, body, timeout)


def query_params(url: str) -> dict[str, str]:
    return dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))


def fixture_transport(ctx: RunContext, route) -> FixtureTransport:
    return FixtureTransport(FIXTURES, ctx.as_of, route)


def make_client(
    transport: Transport, clock: FakeClock | None = None, **kwargs: Any
) -> HttpClient:
    clock = clock or FakeClock()
    kwargs.setdefault("min_interval_seconds", 0.0)
    return HttpClient(transport, sleep=clock.sleep, clock=clock, **kwargs)


def load_fixture(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))
