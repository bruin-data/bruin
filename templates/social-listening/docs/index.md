# Social listening template documentation

## Architecture and schedules

`raw_<source>_content` is the immutable ingest layer. Source adapters accept a start time, end time, page cap and cursor; merge by `(source, external_id)` (or an immutable event key) and keep the returned payload JSON, collection time and ingest run ID. `stg_content_item` normalizes source records into a stable content ID. The pipeline then applies deterministic term matching, an explainable assessment, a stable-key outbox, and warehouse marts.

Use three opt-in schedules: a bounded collection interval, daily enrichment/aggregation, and weekly reporting. In the demo, seeded fixtures stand in for source collection. A bounded backfill is safe to rerun: raw IDs and alert keys are unique.

```text
source adapter -> raw_<source>_content -> stg_content_item -> fct_term_match
                                                  -> fct_mention_assessment -> fct_alert_outbox
                                                                             -> mart_mention_queue
```

The DuckDB demo is ready after `bruin init social-listening`; `.bruin.yml.example` is the local connection. For a cloud warehouse, start with Postgres: replace the DuckDB connection with a least-privilege Postgres connection, set SQL assets to `postgres.sql`, and use JSON/UPSERT equivalents for the source merge. The canonical model keeps platform-specific payload fields in JSON, so BigQuery, Snowflake and ClickHouse adapters can be added without changing downstream concepts.

## Setup and configuration

1. Copy `.bruin.yml.example` to `.bruin.yml`, then run `bruin validate .` and `bruin run .`.
2. Set `brand_name`, `brand_description`, `tracked_terms`, optional competitors, source scopes and exclusions with typed `--var` values.
3. Keep collection bounded with `source_lookback_hours` and `max_pages_per_source`. Reddit polling must be 5–1440 minutes.
4. Use `python3 scripts/validate_config.py` before activating an external collector. It checks scores in `[0,1]`, approval settings, mutation policy and required credentials.
5. Start delivery with `notification_destination=none` or `notification_dry_run=true`. Only then configure a generic or Slack webhook secret.

Variables include source enable flags, community include/exclude lists, author/team exclusions, the intent taxonomy, model toggle/version, relevance/priority/confidence thresholds, freshness age, retry cap, destination, and reply policy. All defaults are safe. Custom variables are not a secret store.

## Data model and safety model

| Object | Role |
| --- | --- |
| `raw_reddit_content`, `raw_hackernews_content` | Immutable response/event, cursor, times, payload and ingest run |
| `stg_content_item` | Canonical source item, URL, author, parent, body and JSON metrics/metadata |
| `config.dim_term`, `enrichment.fct_term_match` | Versioned terms plus matched text, deterministic rule and matcher version |
| `enrichment.fct_mention_assessment` | Relevance, intent/fit, engagement, freshness, authenticity, priority, confidence, reasons and model state |
| `operations.fct_alert_outbox` | Stable alert key, payload version, retry state and delivery time |
| `operations.fct_reply_draft`, `operations.fct_human_feedback` | Draft-only review records and queryable feedback; neither can publish |
| `marts.mart_mention_queue` | Review-ready URLs, match evidence, score components and routing state |
| `marts.mart_share_of_voice_monthly` | Brand/topic/competitor counts, organic authors, team-content separation, denominator and coverage |
| `marts.mart_pipeline_health` | Freshness, cursor, volume, unavailable assessment, error, lag and dedupe metrics |

Exclusions run before model work: excluded authors/communities, team content, stale records, bots/spam and unsupported types. The demo's assessment is deterministic and stores components and evidence. An enabled but unavailable model is explicitly `llm_unavailable`; it never fabricates a score. Human feedback is evaluation data only and does not retrain rules or alter production thresholds automatically.

No automated publishing, DM, CRM write-back, private-message collection, identity matching or sensitive-attribute inference exists here. Any mutation-capable integration is a separate adapter requiring `allow_mutations: true` plus a human approval record, and is outside this template's scope.

## Sources, privacy and deletion

Read [sources and privacy](sources-and-privacy.md). Default collection is public content only. Honour source terms, rate limits, authentication and deletion requirements. To redact, identify the source and external ID, delete or tombstone the related raw record under the source’s rules, then rebuild affected staging/marts and retain an auditable request record without copying private content.

## Operations and troubleshooting

| Symptom | Check / action |
| --- | --- |
| No queue rows | Confirm terms, source window, exclusions and `min_relevance` / `min_priority`; query the raw and match tables first. |
| Reddit collector fails | Confirm approved API access, OAuth environment variables, community scope, rate-limit handling and the bounded cursor. |
| Duplicate alert concern | Verify `alert_key` uniqueness and only mark an outbox row delivered after the destination accepts it. |
| Model failure | Keep the `llm_unavailable` row and its evidence; do not substitute a synthetic score. |
| Stale source | Inspect `mart_pipeline_health`, cursor advancement, source terms and retry/error logs. Do not have the health agent backfill. |
| Reply draft is unsafe | Reject it in human review; no draft can publish. |

Use data-quality checks and `bruin validate .` before deployment. The `tests/` directory covers pagination boundaries, dedupe/alert idempotency, matcher cases, rate limits, outbox retries and model failure. See [Bruin Cloud setup](bruin-cloud-setup.md) for draft-first operational agents.

`fixtures/evaluation_cases.json` is a compact evaluation set covering true positives, false positives, ambiguous terms, competitor context and excluded/self content. Expand it with approved, non-sensitive examples before tuning thresholds.
