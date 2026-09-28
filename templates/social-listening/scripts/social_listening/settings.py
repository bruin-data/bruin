"""Load and validate pipeline custom variables.

Bruin checks that every variable has a default, but it does not enforce the
JSON Schema at run time. ``validate`` does that here, then applies the
cross-field policy rules the schema cannot express. The ``config.settings``
asset calls it first, and every other asset depends on that asset, so an
invalid configuration stops the run before any source is contacted.
"""

from __future__ import annotations

import json
import math
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

TEMPLATE_ROOT = Path(__file__).resolve().parents[2]

# Environment variables each secret is injected as. The values are never logged.
SECRET_ENV = {
    "reddit_client_id": "REDDIT_CLIENT_ID",
    "reddit_client_secret": "REDDIT_CLIENT_SECRET",
    "reddit_user_agent": "REDDIT_USER_AGENT",
    "github_token": "GITHUB_TOKEN",
    "stackexchange_key": "STACKEXCHANGE_KEY",
    "webhook_url": "SOCIAL_LISTENING_WEBHOOK_URL",
    "slack_webhook_url": "SOCIAL_LISTENING_SLACK_WEBHOOK_URL",
    "llm_api_key": "LLM_API_KEY",
}

# Connection names in .bruin.yml / Bruin Cloud that hold each secret.
SECRET_CONNECTION = {
    "reddit_client_id": "sl-reddit-client-id",
    "reddit_client_secret": "sl-reddit-client-secret",
    "reddit_user_agent": "sl-reddit-user-agent",
    "github_token": "sl-github-token",
    "stackexchange_key": "sl-stackexchange-key",
    "webhook_url": "sl-webhook-url",
    "slack_webhook_url": "sl-slack-webhook-url",
    "llm_api_key": "sl-llm-api-key",
}

PRIORITY_COMPONENTS = (
    "relevance",
    "intent",
    "fit",
    "engagement",
    "freshness",
    "authenticity",
)


class SettingsError(ValueError):
    """Raised when custom variables or credentials are invalid."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        bullet = "\n  - "
        super().__init__(
            "Invalid social-listening configuration:" + bullet + bullet.join(errors)
        )


@dataclass(frozen=True)
class Window:
    """A half-open collection window: start <= published_at < end (UTC)."""

    start: datetime
    end: datetime

    def __post_init__(self):
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("window bounds must be timezone-aware")
        if self.end <= self.start:
            raise ValueError(
                f"window end {self.end.isoformat()} must be after start {self.start.isoformat()}"
            )

    def contains(self, ts: datetime) -> bool:
        return self.start <= ts < self.end

    @property
    def start_epoch(self) -> int:
        return int(self.start.timestamp())

    @property
    def end_epoch(self) -> int:
        return int(self.end.timestamp())

    def split(self) -> tuple["Window", "Window"]:
        mid = self.start + (self.end - self.start) / 2
        return Window(self.start, mid), Window(mid, self.end)


@dataclass
class RunContext:
    """Everything an asset needs from Bruin's environment."""

    vars: dict[str, Any]
    schema: dict[str, Any]
    run_id: str
    interval_start: datetime
    interval_end: datetime
    env: Mapping[str, str] = field(default_factory=dict)

    @property
    def as_of(self) -> datetime:
        return self.interval_end

    def collection_window(self) -> Window:
        """Bruin's run interval, widened by the late-arrival lookback."""
        late = timedelta(hours=int(self.vars.get("late_arrival_hours", 0)))
        return Window(self.interval_start - late, self.interval_end)

    def secret(self, name: str) -> str:
        return (self.env.get(SECRET_ENV[name]) or "").strip()


def _parse_bruin_time(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_context(env: Mapping[str, str] | None = None) -> RunContext:
    env = dict(os.environ if env is None else env)
    variables = json.loads(env.get("BRUIN_VARS") or "{}")
    schema = json.loads(env.get("BRUIN_VARS_SCHEMA") or "{}")
    now = datetime.now(timezone.utc).replace(microsecond=0)
    start = _parse_bruin_time(
        env.get("BRUIN_START_TIMESTAMP") or env.get("BRUIN_START_DATETIME")
    ) or now - timedelta(days=1)
    end = (
        _parse_bruin_time(
            env.get("BRUIN_END_TIMESTAMP") or env.get("BRUIN_END_DATETIME")
        )
        or now
    )
    # Bruin's daily intervals end at 23:59:59 (``bruin run`` defaults to 23:59:59.999999);
    # treat that as the next midnight so windows tile cleanly.
    if end.second == 59 and end.minute == 59 and end.hour == 23:
        end = end.replace(microsecond=0) + timedelta(seconds=1)
    return RunContext(
        vars=variables,
        schema=schema,
        run_id=env.get("BRUIN_RUN_ID") or f"local-{now.strftime('%Y%m%dT%H%M%S')}",
        interval_start=start,
        interval_end=end,
        env=env,
    )


# --------------------------------------------------------------------------- schema

_TYPE_CHECKS = {
    "string": lambda v: isinstance(v, str),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "number": lambda v: (
        isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)
    ),
    "boolean": lambda v: isinstance(v, bool),
    "array": lambda v: isinstance(v, list),
    "object": lambda v: isinstance(v, dict),
    "null": lambda v: v is None,
}


def _validate_value(
    path: str, value: Any, schema: Mapping[str, Any], errors: list[str]
) -> None:
    expected = schema.get("type")
    if expected:
        types = expected if isinstance(expected, list) else [expected]
        if not any(_TYPE_CHECKS[t](value) for t in types if t in _TYPE_CHECKS):
            errors.append(
                f"{path}: expected {'/'.join(types)}, got {type(value).__name__} {json.dumps(value)[:80]}"
            )
            return
    if "const" in schema and value != schema["const"]:
        errors.append(f"{path}: must be {json.dumps(schema['const'])}")
    if "enum" in schema and value not in schema["enum"]:
        errors.append(
            f"{path}: {json.dumps(value)} is not one of {json.dumps(schema['enum'])}"
        )
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            errors.append(f"{path}: {value} is below the minimum {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            errors.append(f"{path}: {value} is above the maximum {schema['maximum']}")
    if isinstance(value, str):
        if "minLength" in schema and len(value) < schema["minLength"]:
            errors.append(f"{path}: must be at least {schema['minLength']} characters")
        if "pattern" in schema and not re.search(schema["pattern"], value):
            errors.append(
                f"{path}: {json.dumps(value)} does not match {schema['pattern']}"
            )
    if isinstance(value, list):
        if "minItems" in schema and len(value) < schema["minItems"]:
            errors.append(f"{path}: needs at least {schema['minItems']} item(s)")
        if "maxItems" in schema and len(value) > schema["maxItems"]:
            errors.append(f"{path}: allows at most {schema['maxItems']} item(s)")
        if schema.get("uniqueItems") and len(
            {json.dumps(v, sort_keys=True) for v in value}
        ) != len(value):
            errors.append(f"{path}: items must be unique")
        if isinstance(schema.get("items"), dict):
            for i, item in enumerate(value):
                _validate_value(f"{path}[{i}]", item, schema["items"], errors)
    if isinstance(value, dict):
        props = schema.get("properties", {})
        for req in schema.get("required", []):
            if req not in value:
                errors.append(f"{path}: missing required key {req!r}")
        if schema.get("additionalProperties") is False:
            for key in value:
                if key not in props:
                    errors.append(f"{path}: unknown key {key!r}")
        for key, sub in props.items():
            if key in value:
                _validate_value(f"{path}.{key}", value[key], sub, errors)


def validate_schema(values: Mapping[str, Any], schema: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    properties = schema.get("properties", schema)
    for name, sub in properties.items():
        if not isinstance(sub, dict):
            continue
        if name not in values:
            errors.append(f"{name}: missing value")
            continue
        _validate_value(name, values[name], sub, errors)
    return errors


# --------------------------------------------------------------------------- policy


def _need(ctx: RunContext, secret: str, reason: str, errors: list[str]) -> None:
    if not ctx.secret(secret):
        connection = SECRET_CONNECTION[secret]
        env_var = "SL_" + connection[3:].upper().replace("-", "_")
        errors.append(
            f"{reason} requires a value for the generic connection {connection!r} "
            f"(the demo .bruin.yml reads it from ${env_var}; in Bruin Cloud add it as a secret). "
            f"Secrets never go in custom variables."
        )


def validate_policy(ctx: RunContext) -> list[str]:
    v = ctx.vars
    errors: list[str] = []
    live = not v.get("demo_mode", True)

    # Human approval is not optional while reply drafts exist.
    if v.get("reply_drafts_enabled") and not v.get("require_human_approval"):
        errors.append(
            "reply_drafts_enabled=true requires require_human_approval=true; drafts are never published automatically"
        )
    if v.get("allow_mutations"):
        errors.append(
            "allow_mutations=true is outside this template's scope. Mutation-capable adapters (CRM write-back, "
            "posting, DMs) must ship as a separate, approval-gated integration"
        )
    if v.get("author_enrichment_enabled"):
        errors.append(
            "author_enrichment_enabled=true needs an enrichment adapter; none ships with this template"
        )

    # Polling and windows.
    poll = v.get("reddit_poll_minutes", 15)
    if isinstance(poll, int) and poll < 5:
        errors.append(
            "reddit_poll_minutes must be at least 5 to respect Reddit rate limits"
        )
    stale_after = v.get("stale_after_minutes", 180)
    if (
        v.get("reddit_enabled")
        and isinstance(poll, int)
        and isinstance(stale_after, int)
        and stale_after < 2 * poll
    ):
        errors.append(
            f"stale_after_minutes={stale_after} is shorter than two Reddit polling intervals "
            f"({2 * poll} minutes); every healthy source would be reported stale"
        )
    window = ctx.collection_window()
    max_days = v.get("source_window_max_days", 7)
    if window.end - window.start > timedelta(days=max_days):
        errors.append(
            f"collection window {window.start.isoformat()} .. {window.end.isoformat()} is longer than "
            f"source_window_max_days={max_days}; run the backfill in smaller chunks"
        )

    # Thresholds.
    for key in ("min_relevance", "min_priority", "min_confidence", "min_intent_score"):
        value = v.get(key)
        if isinstance(value, (int, float)) and not 0 <= value <= 1:
            errors.append(f"{key} must be between 0 and 1")
    weights = v.get("priority_weights", {})
    if isinstance(weights, dict):
        missing = [c for c in PRIORITY_COMPONENTS if c not in weights]
        if missing:
            errors.append(f"priority_weights is missing {', '.join(missing)}")
        total = sum(float(weights.get(c, 0)) for c in PRIORITY_COMPONENTS)
        if abs(total - 1.0) > 0.001:
            errors.append(f"priority_weights must sum to 1.0 (got {total:.3f})")

    # Terms and taxonomy.
    phrases = (
        [v.get("brand_name", "")]
        + list(v.get("brand_aliases", []))
        + list(v.get("competitors", []))
    )
    phrases += list(v.get("tracked_terms", []))
    phrases += [p for rule in v.get("term_rules", []) for p in rule.get("phrases", [])]
    if not any(p.strip() for p in phrases if isinstance(p, str)):
        errors.append(
            "configure at least one of brand_name, competitors, tracked_terms or term_rules"
        )
    for phrase in phrases:
        if isinstance(phrase, str) and 0 < len(phrase.strip()) < 3:
            errors.append(
                f"term phrase {phrase!r} is shorter than 3 characters and would match too broadly"
            )
    rule_ids = [rule.get("term_id") for rule in v.get("term_rules", [])]
    if len(rule_ids) != len(set(rule_ids)):
        errors.append("term_rules term_id values must be unique")
    for rule in v.get("term_rules", []):
        valid_from, valid_to = rule.get("valid_from"), rule.get("valid_to")
        if valid_from and valid_to and valid_to < valid_from:
            errors.append(
                f"term_rules {rule.get('term_id')}: valid_to is before valid_from"
            )
    intents = [i.get("intent") for i in v.get("intent_taxonomy", [])]
    if len(intents) != len(set(intents)):
        errors.append("intent_taxonomy intent names must be unique")
    for intent in v.get("reply_eligible_intents", []):
        if intent not in intents:
            errors.append(
                f"reply_eligible_intents contains {intent!r}, which is not in intent_taxonomy"
            )

    # Source settings.
    include = {c.lower() for c in v.get("reddit_communities_include", [])}
    exclude = {c.lower() for c in v.get("reddit_communities_exclude", [])}
    overlap = sorted(include & exclude)
    if overlap:
        errors.append(
            f"communities in both include and exclude lists: {', '.join(overlap)}"
        )
    for name in include | exclude:
        if not re.fullmatch(r"[a-z0-9_]{2,21}", name):
            errors.append(
                f"{name!r} is not a valid community name (drop any r/ prefix)"
            )
    enabled_sources = [
        s
        for s in (
            "reddit",
            "hackernews",
            "github",
            "stackoverflow",
            "authorised_export",
        )
        if v.get(f"{s}_enabled")
    ]
    if not enabled_sources:
        errors.append("enable at least one source")
    if v.get("slack_community_enabled"):
        errors.append(
            "slack_community_enabled=true: the Slack community source is an interface only in this version. "
            "Implement scripts/social_listening/sources/slack_community.py with workspace-admin approval first"
        )
    if v.get("authorised_export_enabled"):
        path = v.get("authorised_export_path", "")
        if not path:
            errors.append(
                "authorised_export_enabled=true requires authorised_export_path"
            )
        elif not (TEMPLATE_ROOT / path).exists() and not Path(path).exists():
            errors.append(f"authorised_export_path {path!r} does not exist")
    if live:
        if v.get("reddit_enabled"):
            for secret in (
                "reddit_client_id",
                "reddit_client_secret",
                "reddit_user_agent",
            ):
                _need(ctx, secret, "reddit_enabled=true", errors)
        if v.get("github_enabled"):
            _need(ctx, "github_token", "github_enabled=true", errors)

    # Model settings.
    if v.get("llm_enabled"):
        provider = v.get("llm_provider")
        if provider != "fixture":
            if not v.get("llm_model"):
                errors.append("llm_enabled=true requires llm_model")
            _need(ctx, "llm_api_key", f"llm_provider={provider}", errors)
            if provider == "openai_compatible" and not v.get("llm_base_url"):
                errors.append("llm_provider=openai_compatible requires llm_base_url")
        elif live:
            errors.append(
                "llm_provider=fixture is for demo_mode only; choose a real provider or disable the LLM"
            )

    # Delivery settings.
    destination = v.get("notification_destination")
    if destination != "none" and not v.get("notification_dry_run", True):
        secret = (
            "slack_webhook_url" if destination == "slack_webhook" else "webhook_url"
        )
        _need(
            ctx,
            secret,
            f"notification_destination={destination} with notification_dry_run=false",
            errors,
        )
    return errors


def validate(ctx: RunContext) -> None:
    errors = validate_schema(ctx.vars, ctx.schema)
    try:
        errors += validate_policy(ctx)
    except (TypeError, ValueError, AttributeError):
        # Policy rules assume well-typed values; report the schema errors instead of a traceback.
        if not errors:
            raise
    if errors:
        raise SettingsError(errors)


def redacted_snapshot(ctx: RunContext) -> dict[str, Any]:
    """Settings for the audit table. Secrets are reported as present/absent only."""
    return {
        "vars": ctx.vars,
        "secrets_present": {name: bool(ctx.secret(name)) for name in SECRET_ENV},
        "window_start": ctx.collection_window().start.isoformat(),
        "window_end": ctx.collection_window().end.isoformat(),
    }


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-") or "term"


def search_phrases(v: Mapping[str, Any]) -> list[str]:
    """Every configured phrase, de-duplicated case-insensitively, in a stable order."""
    phrases: list[str] = []
    candidates = (
        [v.get("brand_name", "")]
        + list(v.get("brand_aliases", []))
        + list(v.get("competitors", []))
    )
    candidates += list(v.get("tracked_terms", []))
    candidates += [
        p
        for rule in v.get("term_rules", [])
        if rule.get("active", True)
        for p in rule.get("phrases", [])
    ]
    seen: set[str] = set()
    for phrase in candidates:
        if not isinstance(phrase, str) or not phrase.strip():
            continue
        key = phrase.strip().lower()
        if key not in seen:
            seen.add(key)
            phrases.append(phrase.strip())
    return phrases
