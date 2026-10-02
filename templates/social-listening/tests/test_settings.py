"""settings.validate_schema / validate_policy / validate, windows and load_context."""

from __future__ import annotations

import json
import re
import unittest
from datetime import datetime, timedelta, timezone

from .helpers import (
    _DEFAULT_VARS,
    _SCHEMA_PROPERTIES,
    ANCHOR,
    PIPELINE_YML,
    default_schema,
    default_vars,
    make_ctx,
)

from social_listening import settings
from social_listening.settings import (
    RunContext,
    SettingsError,
    Window,
    load_context,
    validate,
    validate_policy,
    validate_schema,
)

# --------------------------------------------------------------------------- pipeline.yml sync


def _yaml_scalar(text: str):
    text = text.strip()
    if text in ("true", "false"):
        return text == "true"
    try:
        return json.loads(text)
    except ValueError:
        pass
    if (
        text.startswith("[")
        and text.endswith("]")
        and not re.search(r"[\[\]{}]", text[1:-1])
    ):
        inner = text[1:-1].strip()
        return [_yaml_scalar(p) for p in inner.split(",")] if inner else []
    if (
        text.startswith("{")
        and text.endswith("}")
        and not re.search(r"[\[\]{}]", text[1:-1])
    ):
        pairs = [p.split(":", 1) for p in text[1:-1].split(",")]
        return {k.strip(): _yaml_scalar(v) for k, v in pairs}
    return text


def _pipeline_variables() -> dict[str, object]:
    """Variable names mapped to their inline default (or ``...`` for block defaults)."""
    lines = PIPELINE_YML.read_text(encoding="utf-8").splitlines()
    start = lines.index("variables:")
    found: dict[str, object] = {}
    current = None
    for line in lines[start + 1 :]:
        if line and not line.startswith(" ") and not line.startswith("#"):
            break
        name = re.fullmatch(r"  ([a-z_][a-z0-9_]*):\s*", line)
        if name:
            current = name.group(1)
            found[current] = ...
            continue
        default = re.fullmatch(r"    default:\s*(.*)", line)
        if default and current:
            value = default.group(1).strip()
            found[current] = _yaml_scalar(value) if value else ...
    return found


class PipelineDefaultsInSyncTest(unittest.TestCase):
    def test_helper_defaults_cover_every_pipeline_variable(self):
        declared = _pipeline_variables()
        self.assertEqual(sorted(declared), sorted(_DEFAULT_VARS))
        self.assertEqual(sorted(declared), sorted(_SCHEMA_PROPERTIES))

    def test_inline_defaults_match(self):
        for name, value in _pipeline_variables().items():
            if value is ...:
                continue
            with self.subTest(variable=name):
                self.assertEqual(_DEFAULT_VARS[name], value)


# --------------------------------------------------------------------------- validation


class DefaultConfigTest(unittest.TestCase):
    def test_default_config_passes(self):
        ctx = make_ctx()
        self.assertEqual(validate_schema(ctx.vars, ctx.schema), [])
        self.assertEqual(validate_policy(ctx), [])
        validate(ctx)  # does not raise

    def test_schema_without_properties_wrapper(self):
        self.assertEqual(
            validate_schema(default_vars(), default_schema()["properties"]), []
        )


class SchemaValidationTest(unittest.TestCase):
    def test_string_for_integer(self):
        errors = validate_schema(
            default_vars(reddit_poll_minutes="15"), default_schema()
        )
        self.assertTrue(
            any(e.startswith("reddit_poll_minutes: expected integer") for e in errors),
            errors,
        )

    def test_boolean_is_not_an_integer(self):
        errors = validate_schema(
            default_vars(max_pages_per_source=True), default_schema()
        )
        self.assertTrue(
            any("max_pages_per_source: expected integer" in e for e in errors), errors
        )

    def test_value_not_in_enum(self):
        errors = validate_schema(default_vars(llm_provider="gpt"), default_schema())
        self.assertTrue(
            any(e.startswith("llm_provider:") and "is not one of" in e for e in errors),
            errors,
        )

    def test_enum_inside_array_items(self):
        errors = validate_schema(
            default_vars(supported_content_types=["post", "tweet"]), default_schema()
        )
        self.assertTrue(any("supported_content_types[1]" in e for e in errors), errors)

    def test_nested_object_required_and_pattern(self):
        rules = [{"term_id": "Bad ID", "phrases": ["abc"]}]
        errors = validate_schema(default_vars(term_rules=rules), default_schema())
        self.assertTrue(
            any("term_rules[0]: missing required key 'category'" in e for e in errors),
            errors,
        )
        self.assertTrue(
            any("term_rules[0].term_id" in e and "does not match" in e for e in errors),
            errors,
        )

    def test_minimum_and_maximum(self):
        errors = validate_schema(
            default_vars(reddit_poll_minutes=2, min_priority=1.5), default_schema()
        )
        self.assertTrue(
            any("reddit_poll_minutes: 2 is below the minimum 5" in e for e in errors),
            errors,
        )
        self.assertTrue(
            any("min_priority: 1.5 is above the maximum 1" in e for e in errors), errors
        )

    def test_missing_value(self):
        values = default_vars()
        del values["brand_name"]
        self.assertIn(
            "brand_name: missing value", validate_schema(values, default_schema())
        )


class PolicyValidationTest(unittest.TestCase):
    def assertError(self, errors, fragment):
        self.assertTrue(
            any(fragment in e for e in errors), f"{fragment!r} not in {errors}"
        )

    def test_polling_interval_too_short(self):
        errors = validate_policy(make_ctx({"reddit_poll_minutes": 4}))
        self.assertError(errors, "reddit_poll_minutes must be at least 5")

    def test_thresholds_outside_unit_interval(self):
        for key, value in (
            ("min_relevance", -0.1),
            ("min_priority", 1.5),
            ("min_confidence", 2),
            ("min_intent_score", -1),
        ):
            with self.subTest(key=key):
                self.assertError(
                    validate_policy(make_ctx({key: value})),
                    f"{key} must be between 0 and 1",
                )

    def test_threshold_bounds_are_inclusive(self):
        self.assertEqual(
            validate_policy(make_ctx({"min_priority": 1, "min_relevance": 0})), []
        )

    def test_priority_weights_must_sum_to_one(self):
        weights = dict(_DEFAULT_VARS["priority_weights"], relevance=0.5)
        self.assertError(
            validate_policy(make_ctx({"priority_weights": weights})),
            "priority_weights must sum to 1.0",
        )

    def test_priority_weights_missing_component(self):
        weights = dict(_DEFAULT_VARS["priority_weights"])
        weights.pop("authenticity")
        self.assertError(
            validate_policy(make_ctx({"priority_weights": weights})),
            "priority_weights is missing authenticity",
        )

    def test_reply_drafts_require_human_approval(self):
        errors = validate_policy(
            make_ctx({"reply_drafts_enabled": True, "require_human_approval": False})
        )
        self.assertError(
            errors, "reply_drafts_enabled=true requires require_human_approval=true"
        )
        self.assertEqual(
            validate_policy(
                make_ctx({"reply_drafts_enabled": True, "require_human_approval": True})
            ),
            [],
        )

    def test_mutations_are_rejected(self):
        self.assertError(
            validate_policy(make_ctx({"allow_mutations": True})),
            "allow_mutations=true is outside",
        )

    def test_community_in_include_and_exclude(self):
        errors = validate_policy(
            make_ctx(
                {
                    "reddit_communities_include": ["DataEngineering"],
                    "reddit_communities_exclude": ["dataengineering"],
                }
            )
        )
        self.assertError(
            errors, "communities in both include and exclude lists: dataengineering"
        )

    def test_invalid_community_name(self):
        self.assertError(
            validate_policy(make_ctx({"reddit_communities_include": ["r/python"]})),
            "not a valid community name",
        )

    def test_live_reddit_without_credentials_names_the_connection(self):
        errors = validate_policy(make_ctx({"demo_mode": False, "reddit_enabled": True}))
        self.assertError(errors, "'sl-reddit-client-id'")
        self.assertError(errors, "'sl-reddit-client-secret'")
        self.assertError(errors, "'sl-reddit-user-agent'")

    def test_live_reddit_with_credentials_passes(self):
        env = {
            "REDDIT_CLIENT_ID": "id",
            "REDDIT_CLIENT_SECRET": "secret",
            "REDDIT_USER_AGENT": "ua/1.0",
        }
        self.assertEqual(validate_policy(make_ctx({"demo_mode": False}, env=env)), [])

    def test_blank_secret_counts_as_missing(self):
        env = {
            "REDDIT_CLIENT_ID": "   ",
            "REDDIT_CLIENT_SECRET": "secret",
            "REDDIT_USER_AGENT": "ua/1.0",
        }
        self.assertError(
            validate_policy(make_ctx({"demo_mode": False}, env=env)),
            "'sl-reddit-client-id'",
        )

    def test_live_webhook_without_url(self):
        errors = validate_policy(
            make_ctx(
                {"notification_destination": "webhook", "notification_dry_run": False}
            )
        )
        self.assertError(errors, "'sl-webhook-url'")
        errors = validate_policy(
            make_ctx(
                {
                    "notification_destination": "slack_webhook",
                    "notification_dry_run": False,
                }
            )
        )
        self.assertError(errors, "'sl-slack-webhook-url'")

    def test_live_webhook_with_url_passes(self):
        ctx = make_ctx(
            {"notification_destination": "webhook", "notification_dry_run": False},
            env={"SOCIAL_LISTENING_WEBHOOK_URL": "https://hooks.example.test/x"},
        )
        self.assertEqual(validate_policy(ctx), [])

    def test_no_sources_enabled(self):
        errors = validate_policy(
            make_ctx({"reddit_enabled": False, "hackernews_enabled": False})
        )
        self.assertError(errors, "enable at least one source")

    def test_window_longer_than_source_window_max_days(self):
        errors = validate_policy(make_ctx(hours=24 * 8))
        self.assertError(errors, "longer than source_window_max_days=7")
        # Late-arrival lookback counts towards the window: 6d 20h + 6h > 7d.
        self.assertError(
            validate_policy(make_ctx(hours=24 * 6 + 20)), "source_window_max_days"
        )
        self.assertEqual(validate_policy(make_ctx(hours=24 * 6 + 18)), [])

    def test_missing_export_file(self):
        errors = validate_policy(
            make_ctx(
                {
                    "authorised_export_enabled": True,
                    "authorised_export_path": "fixtures/nope.csv",
                }
            )
        )
        self.assertError(errors, "does not exist")
        self.assertEqual(
            validate_policy(make_ctx({"authorised_export_enabled": True})), []
        )

    def test_slack_community_is_interface_only(self):
        self.assertError(
            validate_policy(make_ctx({"slack_community_enabled": True})),
            "interface only",
        )

    def test_short_phrase_rejected(self):
        self.assertError(
            validate_policy(make_ctx({"tracked_terms": ["ab"]})),
            "shorter than 3 characters",
        )

    def test_reply_eligible_intent_must_exist(self):
        errors = validate_policy(make_ctx({"reply_eligible_intents": ["complaint"]}))
        self.assertError(errors, "'complaint', which is not in intent_taxonomy")

    def test_llm_settings(self):
        errors = validate_policy(
            make_ctx({"llm_enabled": True, "llm_provider": "openai_compatible"})
        )
        self.assertError(errors, "requires llm_model")
        self.assertError(errors, "'sl-llm-api-key'")
        self.assertError(errors, "requires llm_base_url")
        env = {
            "REDDIT_CLIENT_ID": "i",
            "REDDIT_CLIENT_SECRET": "s",
            "REDDIT_USER_AGENT": "u",
        }
        errors = validate_policy(
            make_ctx({"demo_mode": False, "llm_enabled": True}, env=env)
        )
        self.assertError(errors, "llm_provider=fixture is for demo_mode only")

    def test_validate_raises_with_all_errors(self):
        ctx = make_ctx({"allow_mutations": True, "reddit_poll_minutes": "x"})
        with self.assertRaises(SettingsError) as caught:
            validate(ctx)
        self.assertTrue(any("allow_mutations" in e for e in caught.exception.errors))
        self.assertTrue(
            any(
                "reddit_poll_minutes: expected integer" in e
                for e in caught.exception.errors
            )
        )
        self.assertIn("Invalid social-listening configuration", str(caught.exception))

    def test_mistyped_values_raise_settings_error_not_a_traceback(self):
        weights = dict(_DEFAULT_VARS["priority_weights"], relevance="high")
        cases = (
            {"late_arrival_hours": "six"},
            {"source_window_max_days": "7"},
            {"priority_weights": weights},
            {"term_rules": ["social listening"]},
        )
        for overrides in cases:
            with self.subTest(overrides=list(overrides)):
                with self.assertRaises(SettingsError) as caught:
                    validate(make_ctx(overrides))
                name = next(iter(overrides))
                self.assertTrue(
                    any(e.startswith(name) for e in caught.exception.errors),
                    caught.exception.errors,
                )

    def test_secret_values_never_in_snapshot(self):
        ctx = make_ctx(env={"GITHUB_TOKEN": "ghp_supersecret"})
        snapshot = settings.redacted_snapshot(ctx)
        self.assertTrue(snapshot["secrets_present"]["github_token"])
        self.assertNotIn("ghp_supersecret", json.dumps(snapshot))


# --------------------------------------------------------------------------- windows


class WindowTest(unittest.TestCase):
    def test_contains_is_start_inclusive_end_exclusive(self):
        start = datetime(2026, 9, 27, tzinfo=timezone.utc)
        end = datetime(2026, 9, 28, tzinfo=timezone.utc)
        window = Window(start, end)
        self.assertTrue(window.contains(start))
        self.assertTrue(window.contains(end - timedelta(microseconds=1)))
        self.assertFalse(window.contains(end))
        self.assertFalse(window.contains(start - timedelta(seconds=1)))

    def test_rejects_naive_and_empty_windows(self):
        with self.assertRaises(ValueError):
            Window(datetime(2026, 9, 27), datetime(2026, 9, 28))
        with self.assertRaises(ValueError):
            Window(ANCHOR, ANCHOR)

    def test_split_tiles_the_window(self):
        window = Window(ANCHOR - timedelta(hours=10), ANCHOR)
        left, right = window.split()
        self.assertEqual(
            (left.start, left.end, right.end),
            (window.start, ANCHOR - timedelta(hours=5), window.end),
        )
        self.assertEqual(left.end, right.start)

    def test_collection_window_widens_by_late_arrival(self):
        ctx = make_ctx({"late_arrival_hours": 6}, hours=24)
        window = ctx.collection_window()
        self.assertEqual(window.start, ANCHOR - timedelta(hours=30))
        self.assertEqual(window.end, ANCHOR)
        self.assertEqual(
            make_ctx({"late_arrival_hours": 0}).collection_window().start,
            ANCHOR - timedelta(hours=24),
        )


class LoadContextTest(unittest.TestCase):
    def _env(self, **extra):
        env = {
            "BRUIN_VARS": json.dumps(default_vars()),
            "BRUIN_VARS_SCHEMA": json.dumps(default_schema()),
            "BRUIN_RUN_ID": "run-42",
        }
        env.update(extra)
        return env

    def test_end_of_day_becomes_next_midnight(self):
        ctx = load_context(
            self._env(
                BRUIN_START_DATETIME="2026-09-27T00:00:00",
                BRUIN_END_DATETIME="2026-09-27T23:59:59",
            )
        )
        self.assertIsInstance(ctx, RunContext)
        self.assertEqual(ctx.interval_start, datetime(2026, 9, 27, tzinfo=timezone.utc))
        self.assertEqual(ctx.interval_end, datetime(2026, 9, 28, tzinfo=timezone.utc))
        self.assertEqual(ctx.run_id, "run-42")
        self.assertEqual(ctx.vars["brand_name"], "Example Co")
        validate(ctx)

    def test_end_of_day_with_microseconds_becomes_next_midnight(self):
        # `bruin run` defaults --end-date to 23:59:59.999999 and formats
        # BRUIN_END_TIMESTAMP with microseconds.
        ctx = load_context(
            self._env(
                BRUIN_START_TIMESTAMP="2026-09-27T00:00:00.000000Z",
                BRUIN_END_TIMESTAMP="2026-09-27T23:59:59.999999Z",
                BRUIN_END_DATETIME="2026-09-27T23:59:59",
            )
        )
        self.assertEqual(ctx.interval_end, datetime(2026, 9, 28, tzinfo=timezone.utc))
        self.assertEqual(
            ctx.collection_window().end.isoformat(), "2026-09-28T00:00:00+00:00"
        )

    def test_timestamp_with_zone_is_converted_to_utc(self):
        ctx = load_context(
            self._env(
                BRUIN_START_TIMESTAMP="2026-09-27T02:00:00+02:00",
                BRUIN_END_TIMESTAMP="2026-09-28T00:30:00Z",
            )
        )
        self.assertEqual(
            ctx.interval_start, datetime(2026, 9, 27, 0, 0, tzinfo=timezone.utc)
        )
        self.assertEqual(
            ctx.interval_end, datetime(2026, 9, 28, 0, 30, tzinfo=timezone.utc)
        )

    def test_other_end_times_are_kept(self):
        ctx = load_context(
            self._env(
                BRUIN_START_DATETIME="2026-09-27T00:00:00",
                BRUIN_END_DATETIME="2026-09-27T23:59:58",
            )
        )
        self.assertEqual(
            ctx.interval_end, datetime(2026, 9, 27, 23, 59, 58, tzinfo=timezone.utc)
        )

    def test_secret_reads_env(self):
        ctx = load_context(self._env(GITHUB_TOKEN="  tok  "))
        self.assertEqual(ctx.secret("github_token"), "tok")
        self.assertEqual(ctx.secret("stackexchange_key"), "")


if __name__ == "__main__":
    unittest.main()
