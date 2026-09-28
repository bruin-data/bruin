# Social listening template

This Bruin template finds public conversations about your brand, your competitors and your topics. It qualifies each one with scores you can explain, sends the ones worth a look to a review queue and to Slack or a webhook, and can draft replies for a person to review and post by hand.

It runs as a normal Bruin pipeline in your own warehouse. The demo uses DuckDB with recorded fixture data and needs no credentials. Production uses the same code against live APIs, and the documented cloud path is MotherDuck.

**What it never does:** post, reply, vote, send DMs, create or update CRM records, or change anything on a source platform. It does not scrape, and it does not read private messages. Every alert records why it was raised.

- [Quick start](#quick-start)
- [Architecture](#architecture)
- [Setup](#setup)
- [Sources and their limits](#sources-and-their-limits)
- [Configuration reference](#configuration-reference)
- [Data model](#data-model)
- [How an item is processed](#how-an-item-is-processed)
- [Operations](#operations)
- [Safety model](#safety-model)
- [Bruin Cloud agents](#bruin-cloud-agents)
- [Porting to another warehouse](#porting-to-another-warehouse)
- [Common failure modes and troubleshooting](#common-failure-modes-and-troubleshooting)
- [Testing and CI](#testing-and-ci)

Related pages: [sources-and-privacy.md](sources-and-privacy.md) and [bruin-cloud-setup.md](bruin-cloud-setup.md).

## Quick start

```bash
bruin init social-listening
cd social-listening
bruin validate .
bruin run .
```

Then open the review queue:

```bash
bruin query --connection social-listening-warehouse --query "
  SELECT source, external_id, queue_state, priority, intent, matched_text, rule_ids, source_url, reasons_json
  FROM marts.mart_mention_queue ORDER BY priority DESC"
```

With the default fixtures you get twelve content items. Five are routed: two Reddit posts asking for recommendations or comparing tools, and three Hacker News items (a user reporting failures, a competitor's user looking for alternatives, and a workflow discussion). The other seven are there to show the rules working:

- a grocery "co-op" post that matches the brand phrase but scores below the threshold;
- a launch post with no request;
- an ambiguous "social listening" post about relationships, rejected by a context rule;
- the brand team's own post, kept for share of voice but never routed;
- a Reddit cross-post, marked as a duplicate of the original;
- a moderator bot's post;
- a job advert caught by `excluded_terms`.

One more Hacker News comment has been redacted and never reaches staging, and one Reddit post lies outside the collection window.

Run `bruin run .` again and nothing is duplicated. Alerts are in dry-run mode: they are recorded in `operations.fct_alert_outbox` with status `dry_run` and nothing is sent.

## Architecture

```text
                    config.settings  (validates every variable; all assets depend on it)
                           │
   ┌───────────────┬───────┴───────┬────────────────┬──────────────────┐
raw.raw_reddit  raw.raw_hackernews  raw.raw_github  raw.raw_stackoverflow  raw.raw_authorised_export   (Python, merge)
   └───────────────┴───────┬───────┴────────────────┴──────────────────┘
                           │                     config.dim_term / dim_term_phrase / dim_term_context
                           ▼                     config.dim_intent_rule   config.redaction_request
                 staging.stg_content_item   ◄─── (redactions removed, latest version, cross-post dedupe)
                           │
                 enrichment.fct_term_match       (deterministic word-boundary matcher + context rules)
                           │
                 enrichment.fct_mention_candidate (exclusions, before any model work)
                           │
                 enrichment.llm_assessment_result (optional, Python; validated structured output)
                           │
                 enrichment.fct_mention_assessment (versioned components + priority, merge)
                           │
                 operations.fct_alert_decision   (insert-only routing log, stable alert keys)
                           │
                 operations.alert_delivery_attempt (Python; dry-run, retries, idempotency keys)
                           │
                 operations.fct_alert_outbox     (current delivery state)
                           │
   ┌───────────────────────┼──────────────────────────┬─────────────────────────────┐
operations.fct_reply_draft  operations.fct_human_feedback  marts.mart_mention_queue   marts.mart_pipeline_health
(review seeds)              (review seed)                                              marts.mart_share_of_voice_monthly
```

Design choices:

- **Warehouse-first.** Everything is a table in your warehouse. Staging, matching, scoring, routing, marts and data-quality checks are SQL. Python is used only where something external happens: source APIs, the model call and webhook delivery.
- **Bounded windows.** Each collector reads Bruin's run interval, widened by `late_arrival_hours`. A run can never read more than `source_window_max_days`.
- **Idempotent by key.** Raw rows are keyed by source, external ID and a hash of the content (engagement counters excluded). Alerts are keyed by content, destination and routing policy. Delivery attempts are keyed by alert and attempt number. Re-running any window is safe.
- **Explainable.** Each score component is stored separately, next to the rule or model that produced it, and the priority formula is checked on every run.
- **Portable.** Warehouse-specific SQL lives in `macros/social_listening.sql`. Python assets read the warehouse through the Bruin Python SDK.

## Setup

### Local demo

`bruin init social-listening` adds this to your project's `.bruin.yml`:

- a DuckDB connection named `social-listening-warehouse` (file `social-listening.duckdb`, `max_concurrent_assets: 1`, because a DuckDB file allows one writer at a time);
- generic connections for every secret, each reading an environment variable that is empty by default.

Python assets install `bruin-sdk` from `requirements.txt` with uv on first run. After that, `bruin run .` takes about 20 seconds.

### Live sources

1. **Reddit.** Create an app under an account you control and note the client ID and secret. Then:
   ```bash
   export SL_REDDIT_CLIENT_ID=... SL_REDDIT_CLIENT_SECRET=...
   export SL_REDDIT_USER_AGENT="<platform>:<app id>:1.0 (by /u/<account>)"
   ```
2. **Hacker News** needs nothing.
3. Choose your terms and communities, then run a bounded dry-run backfill:
   ```bash
   bruin run . --var demo_mode=false \
     --var 'brand_name="Your Brand"' --var 'competitors=["Other Co"]' \
     --var 'reddit_communities_include=["dataengineering","analytics"]' \
     --start-date 2026-01-01 --end-date 2026-01-02
   ```
   For lasting settings, change the defaults in `pipeline.yml` instead of passing `--var` every time.
4. Review `marts.mart_mention_queue`, `marts.mart_pipeline_health` and `operations.fct_alert_outbox`, and tune the thresholds.
   Start the live warehouse from a clean state: staging only reads rows from the current mode, but the demo rows stay in raw, so use a new DuckDB file (or `--environment production`) for live data. Replace every demo term too: set `brand_aliases`, `competitors`, `tracked_terms` and `term_rules` (use `[]` to clear), or they will generate live queries.
5. Enable delivery: set `SL_SLACK_WEBHOOK_URL` (Slack incoming webhook) or `SL_WEBHOOK_URL` with `notification_destination="webhook"`, and set `notification_dry_run=false`.

### Cloud warehouse: MotherDuck

MotherDuck runs the same DuckDB SQL, so no asset changes are needed. `requirements.txt` pins `duckdb` to a release MotherDuck accepts; raise it only when MotherDuck supports the newer version. Runs take several minutes on MotherDuck (each quality check is a network round trip) versus under a minute locally.

1. Create a MotherDuck database (for example `social_listening`) and a token.
2. In your project `.bruin.yml`, add an environment whose warehouse is a `motherduck` connection named `social-listening-warehouse`. `.bruin.yml.example` has a complete `production` environment.
3. Run with `--environment production --force`. Bruin asks for confirmation before running against an environment whose name contains "prod"; `--force` skips the prompt for scheduled runs.

For Bruin Cloud, create the same connection names there. See [bruin-cloud-setup.md](bruin-cloud-setup.md). For other warehouses, see [Porting to another warehouse](#porting-to-another-warehouse).

## Sources and their limits

| Source | Default | Path | Notes |
| --- | --- | --- | --- |
| Reddit | on | Official OAuth API (application-only) | Posts via search; comments only from included communities; excluded communities dropped before storage. |
| Hacker News | on | Algolia HN Search API | Stories and comments; windows with more than 1,000 hits are split. |
| GitHub | off | REST search API, public repositories | Needs a read-only token. |
| Stack Overflow | off | Stack Exchange API 2.3 | Honours the API's `backoff`. CC BY-SA attribution applies. |
| LinkedIn, X, Quora | off | Authorised export file only | No scraping, and no unofficial integrations. |
| Slack community | interface only | — | Needs workspace authority; enabling it fails validation. |
| CRM | not included | — | Enrichment and write-back must be separate, approval-gated adapters. |

Details, stored fields and deletion rules: [sources-and-privacy.md](sources-and-privacy.md).

Limitations to know about:

- Reddit search is not exhaustive and does not index comments. Comment coverage is limited to `reddit_communities_include`.
- Each collector stops at `max_pages_per_source` pages per query and records a partial error when it does, so the health mart can show that a window may be incomplete.
- Algolia and GitHub cap results at 1,000 per query. The collectors split the window down to one hour; beyond that they record a partial error.
- The deterministic matcher is phrase based. Variants you do not list (misspellings, new product names) are not found until you add them.

## Configuration reference

All policy lives in `pipeline.yml` as typed custom variables. Change the defaults there, pass `--var 'name=<json>'` for one run, or set custom-variable overrides in Bruin Cloud. `config.settings` validates types, ranges and enums against the schema, then checks the cross-field rules listed under [Validation](#validation). Secrets are never custom variables.

### Brand and terms

| Variable | Type | Default | Description |
| --- | --- | --- | --- |
| `brand_name` | string | `"Example Co"` | Brand phrase (term `brand`). |
| `brand_description` | string | product, ideal customer, exclusions, claims to avoid | Given to the model as context. Also a place to write down your policy. |
| `brand_aliases` | string[] | `["exampleco"]` | Extra brand phrases. |
| `competitors` | string[] | `["Rival Suite"]` | One `competitor-<slug>` term each. |
| `tracked_terms` | string[] | `["example alternative"]` | One `topic-<slug>` term each. |
| `term_rules` | object[] | one ambiguous-phrase rule | Advanced terms: `term_id`, `phrases`, `category` (brand/competitor/topic), `platforms` (sources; empty = all), `requires_context_any`, `excludes_context_any`, `valid_from`, `valid_to` (YYYY-MM-DD), `active`. |
| `excluded_terms` | string[] | `["we're hiring", "job opening"]` | Any of these in the text excludes the item. |
| `matcher_version` | string | `"v1"` | Recorded on matches and part of the assessment version. Bump it when you change matching behaviour. |

### Sources

| Variable | Type | Default | Description |
| --- | --- | --- | --- |
| `demo_mode` | boolean | `true` | Serve recorded fixtures instead of calling APIs. |
| `reddit_enabled` | boolean | `true` | Collect Reddit. Needs the three Reddit secrets when `demo_mode=false`. |
| `reddit_communities_include` | string[] | `[]` | Search only these communities and read their comments. Empty searches all of Reddit (posts only). |
| `reddit_communities_exclude` | string[] | `[]` | Never store content from these communities. |
| `reddit_include_comments` | boolean | `true` | Read comments from included communities. |
| `reddit_poll_minutes` | integer 5–1440 | `15` | Intended collection cadence. Must be at least 5, and `stale_after_minutes` must be at least twice this. |
| `hackernews_enabled` | boolean | `true` | Collect Hacker News. |
| `hackernews_include_comments` | boolean | `true` | Include comments as well as stories. |
| `github_enabled` | boolean | `false` | Collect public GitHub issues. Needs `sl-github-token` when live. |
| `github_include_pull_requests` | boolean | `false` | Also collect pull requests. They are high volume; expect many more candidates. |
| `stackoverflow_enabled` | boolean | `false` | Collect Stack Overflow questions. |
| `slack_community_enabled` | boolean | `false` | Interface only; `true` fails validation. |
| `authorised_export_enabled` | boolean | `false` | Load an authorised export CSV. |
| `authorised_export_path` | string | `"fixtures/exports/sample_export.csv"` | Path relative to the pipeline, or absolute. |
| `late_arrival_hours` | integer 0–168 | `6` | Extra lookback before the interval start, for late-indexed content. |
| `source_window_max_days` | integer 1–31 | `7` | Longest window one run may collect. Longer backfills must be chunked. |
| `max_pages_per_source` | integer 1–50 | `5` | Page limit per query and community. |
| `http_max_retries` | integer 0–8 | `3` | Retries for HTTP 429 and 5xx per request, with backoff and `Retry-After`. |
| `fail_on_source_error` | boolean | `true` | Fail the asset on authentication or exhausted-retry errors. `false` stores a `failed` run summary instead. |

### Exclusions

| Variable | Type | Default | Description |
| --- | --- | --- | --- |
| `excluded_authors` | string[] | `[]` | Never assess these handles. |
| `team_authors` | string[] | `["exampleco_team"]` | Your own accounts. Counted as self content in share of voice; not routed. |
| `route_team_content` | boolean | `false` | Route team content anyway. |
| `bot_author_patterns` | string[] (regex) | AutoModerator, `bot$`, `[deleted]` | Authors matching any pattern are excluded. |
| `supported_content_types` | string[] | all types | Allowed types: post, comment, story, issue, pull_request, question, export_item. |
| `max_content_age_days` | integer 1–365 | `30` | Older content is excluded as stale. |
| `author_enrichment_enabled` | boolean | `false` | No enrichment adapter ships; `true` fails validation. |

### Scoring and routing

| Variable | Type | Default | Description |
| --- | --- | --- | --- |
| `intent_taxonomy` | object[] | 5 intents | `{intent, keywords, score}`. The highest-scoring matching keyword wins; no match is `general_discussion` (0.2). |
| `fit_keywords` | string[] | warehouse, data team, … | Each hit adds 0.15 to fit (base 0.5, max 1). |
| `disqualifying_keywords` | string[] | homework, internship, … | Any hit sets fit to 0. |
| `priority_weights` | object | relevance 0.3, intent 0.25, fit 0.15, engagement 0.1, freshness 0.1, authenticity 0.1 | Must sum to 1. |
| `category_relevance` | object | brand 0.9, competitor 0.85, topic 0.75 | Base relevance by category of the best match. |
| `freshness_half_life_hours` | number 1–720 | `48` | Freshness halves every this many hours. |
| `engagement_saturation` | number ≥ 1 | `200` | Engagement signal at which engagement reaches 1. |
| `min_relevance` | number 0–1 | `0.70` | Routing threshold. |
| `min_priority` | number 0–1 | `0.65` | Routing threshold. |
| `min_confidence` | number 0–1 | `0.50` | Routing threshold. |
| `min_intent_score` | number 0–1 | `0.50` | Routing threshold. Keeps general discussion and praise out of alerts. |
| `scoring_version` | string | `"v1"` | Part of the assessment version. Bump it when you change weights or formulas. |

### Model

| Variable | Type | Default | Description |
| --- | --- | --- | --- |
| `llm_enabled` | boolean | `false` | Run the structured model assessment on eligible candidates. |
| `llm_provider` | enum | `fixture` | `fixture` (recorded answers, demo only), `anthropic` (Messages API) or `openai_compatible` (any Chat Completions-compatible endpoint). |
| `llm_model` | string | `""` | Model identifier. Required for real providers. |
| `llm_base_url` | string | `""` | Base URL for `openai_compatible`. |
| `llm_prompt_version` | string | `"v1"` | Loads `scripts/social_listening/prompts/assessment_<version>.md`. |
| `llm_max_items_per_run` | integer 1–500 | `25` | Model calls per run. Items over budget are `llm_pending`. |
| `llm_max_attempts` | integer 1–5 | `3` | Attempts per item across runs before it stays failed. |
| `llm_timeout_seconds` | integer 5–120 | `30` | Request timeout. |
| `route_on_llm_error` | boolean | `false` | Route items whose model call failed, using their rule-based scores. |

### Notifications

| Variable | Type | Default | Description |
| --- | --- | --- | --- |
| `notification_destination` | enum | `slack_webhook` | `none`, `webhook` (JSON payload) or `slack_webhook` (formatted text). |
| `notification_dry_run` | boolean | `true` | Record one `dry_run` attempt per alert and send nothing. |
| `max_delivery_attempts` | integer 1–10 | `3` | Live attempts per alert, one per run. |
| `max_alerts_per_run` | integer 1–200 | `20` | Live sends per run, highest priority first. |
| `alert_max_age_hours` | integer 1–720 | `72` | Content older than this is marked `expired`, not sent. Stops backfills from flooding the channel. |
| `payload_version` | enum | `v1` | Webhook payload schema, sent as `X-Payload-Version`. |
| `routing_policy_version` | string | `"v1"` | Part of the alert key. Bump it only when you want already-alerted items to alert again. |

### Replies and mutations

| Variable | Type | Default | Description |
| --- | --- | --- | --- |
| `reply_drafts_enabled` | boolean | `false` | Create draft replies for routed items. |
| `require_human_approval` | boolean | `true` | Must be true whenever drafts are enabled. |
| `reply_eligible_intents` | string[] | seeking_recommendation, comparison, question | Intents that get a draft. Each must be in `intent_taxonomy`. |
| `reply_disclosure` | string | `"Disclosure: I work on {brand}."` | Opening line of every draft. `{brand}` becomes `brand_name`. |
| `reply_claims_to_avoid` | string[] | guaranteed, certified, … | A draft containing any of these is `blocked_policy`. |
| `allow_mutations` | boolean | `false` | `true` fails validation. Mutations are outside this template. |

### Health

| Variable | Type | Default | Description |
| --- | --- | --- | --- |
| `stale_after_minutes` | integer 15–10080 | `180` | A source with no successful run for this long is `stale`. |
| `volume_anomaly_ratio` | number 1.5–100 | `4` | 24-hour volume above this multiple of the 7-day daily average is abnormal. |

### Validation

Besides types, ranges and enums, `config.settings` fails the run when:

- `reply_drafts_enabled=true` and `require_human_approval=false`;
- `allow_mutations=true`, `author_enrichment_enabled=true` or `slack_community_enabled=true`;
- `reddit_poll_minutes < 5`, or `stale_after_minutes` is shorter than two polling intervals;
- a threshold is outside 0–1, or `priority_weights` is missing a component or does not sum to 1;
- no term is configured, a phrase is shorter than 3 characters, `term_rules` IDs repeat, or `valid_to` is before `valid_from`;
- intents repeat, or `reply_eligible_intents` names an unknown intent;
- a community is in both include and exclude lists, or a name is not a valid community name (drop the `r/`);
- no source is enabled, or the export file is missing;
- `demo_mode=false` and an enabled source lacks its credentials (the message names the connection);
- a real LLM provider lacks `llm_model`, the API key or (for `openai_compatible`) `llm_base_url`, or `llm_provider=fixture` is used outside demo mode;
- live delivery is enabled without the webhook URL;
- the collection window is longer than `source_window_max_days`.

Each run's validated settings are stored in `config.settings` (secrets only as present or absent).

## Data model

Schemas: `config`, `raw`, `staging`, `enrichment`, `operations`, `marts`. Every table below is a Bruin asset with column checks. The asset files document each column.

| Table | Grain and purpose | Key columns |
| --- | --- | --- |
| `config.settings` | One row per run: the validated settings snapshot. | `run_id`, `settings_json`, `secrets_present_json`, `window_start`, `window_end` |
| `config.dim_term` | One tracked concept. | `term_id`, `category`, `phrases_json`, `platform_scope`, `is_active`, `valid_from`, `valid_to` |
| `config.dim_term_phrase` / `dim_term_context` | Phrases per term; context rules for ambiguous phrases. | `phrase_lower`, `phrase_pattern`; `context_type` (`require`/`exclude`) |
| `config.dim_intent_rule` | Intent keyword rules. | `intent`, `keyword_lower`, `intent_score` |
| `config.redaction_request` | Deletion and redaction requests (seed). | `source`, `external_id`, `reason` |
| `raw.raw_<source>_content` | Immutable source records plus one `run_summary` row per run. Merge on `event_key`. | `event_key`, `source`, `external_id`, `record_kind`, `payload_json`, `published_at`, `collected_at`, `source_cursor`, `ingest_run_id`, `window_start`, `window_end` |
| `staging.stg_content_item` | Latest version of each post, comment, story, issue or question. | `content_id` (hash of source and external ID), `source`, `content_type`, `url`, `author`, `community`, `parent_content_id`, `title`, `body`, `published_at`, `metrics_json`, `metadata_json`, `version_count`, `is_syndicated_duplicate` |
| `operations.fct_source_run` | One row per collection run. | `source`, `run_status`, `requests`, `retries`, `rate_limit_waits`, `http_errors`, `partial_error_count`, `high_watermark`, `source_cursor` |
| `enrichment.fct_term_match` | One content and term pair that the matcher considered. | `content_id`, `term_id`, `matched_text` (exact text), `rule_id`, `is_accepted`, `matcher_version`, `matched_at` |
| `enrichment.fct_mention_candidate` | Items with a match, and their eligibility. | `is_eligible`, `exclusion_reason`, `is_team_content`, `matched_terms` |
| `enrichment.llm_assessment_result` | Validated model results and run summaries. | `status` (`ok`/`error`/`invalid_response`), `llm_*` scores (NULL unless ok), `evidence_json`, `prompt_version`, `model_id`, `attempt_count` |
| `enrichment.fct_mention_assessment` | Versioned assessment. Merge on `assessment_id`. | `assessment_version`, `assessment_status`, `relevance`, `intent`, `intent_score`, `fit`, `engagement`, `freshness`, `authenticity`, `confidence`, `priority`, `det_*` and `llm_*` components, `score_basis`, `model_generated`, `reasons_json` |
| `operations.fct_alert_decision` | Insert-only routing log. | `alert_key`, `content_id`, `destination`, `payload_version`, `routing_policy_version`, `payload_json`, `routed_at` |
| `operations.alert_delivery_attempt` | Every delivery attempt. | `attempt_key`, `alert_key`, `attempt_number`, `status`, `http_status`, `error_message` |
| `operations.fct_alert_outbox` | One routing decision with its current delivery state. | `alert_key`, `destination`, `status`, `attempt_count`, `retries`, `delivered_at`, `last_error`, `delivery_lag_minutes` |
| `operations.fct_reply_draft` | Draft-only replies. No publishing fields. | `draft_id`, `content_id`, `approach`, `draft_body`, `evidence_json`, `policy_checks_json`, `status`, `reviewer`, `reviewed_at`, `approved_at` |
| `operations.fct_human_feedback` | Reviewer outcomes. | `outcome`, `corrected_relevance`, `corrected_intent`, `is_false_positive`, `notes`, the assessment it refers to |
| `marts.mart_mention_queue` | The current review queue. | `source_url`, `matched_text`, `rule_ids`, all score components, `reasons_json`, `queue_state`, `delivery_status`, `feedback_outcome`, `draft_status` |
| `marts.mart_share_of_voice_monthly` | Monthly counts by term, source and `all`. | `mentions`, `organic_mentions`, `self_mentions`, `distinct_authors`, `organic_distinct_authors`, `denominator_organic_mentions`, `share_of_voice`, `days_collected`, `coverage_ratio`, `contributing_sources` |
| `marts.mart_pipeline_health` | One row per source, the model and delivery. | `health_status`, `health_reasons_json`, `last_success_at`, `source_cursor`, `high_watermark`, `volume_24h`, `avg_daily_volume_7d`, `error_count_24h`, `median_classification_lag_minutes`, `median_delivery_lag_minutes`, `duplicates_removed`, `superseded_versions` |

Extra source fields belong in `payload_json` (raw), `metrics_json` and `metadata_json` (staging), never in new canonical columns.

## How an item is processed

1. **Collect.** A collector reads the bounded window, follows pagination and rate limits, keeps a minimal field set, and writes rows keyed by `event_key = sha256(source, external_id, content_hash)`, where the content hash leaves out engagement counters (score, points, comment counts). Reading the same record again gives the same key; the collector skips keys already stored, so raw rows are never rewritten (engagement counters are those at first collection). An edited title or body gets a new key and becomes a new version. Each run writes a `run_summary` row with its counts, errors and high watermark.
2. **Normalise.** `stg_content_item` keeps the latest version per source record, cleans HTML, builds a stable `content_id = md5(source || ':' || external_id)`, and removes redacted records.
3. **De-duplicate.** Records are de-duplicated by source ID first. The only secondary rule is a Reddit cross-post whose original is also present; it is marked `is_syndicated_duplicate` and excluded. The same link shared on two platforms counts as two conversations.
4. **Match.** `fct_term_match` finds each phrase on word boundaries (case-insensitive), within the term's platform scope and validity dates, and records the exact text. Context rules then accept or reject the match: `phrase_boundary.v1`, `phrase_boundary+context.v1`, `rejected.context_missing.v1` or `rejected.excluded_context.v1`.
5. **Exclude.** Before any model work, `fct_mention_candidate` applies the first matching rule: no accepted match, syndicated duplicate, excluded author, excluded or out-of-scope community, unsupported type, bot author, excluded term, stale content, or team/self content.
6. **Assess (optional model).** With `llm_enabled`, only eligible candidates go to the model, within the per-run budget. The answer must be JSON with scores in [0, 1], an intent from the taxonomy, and evidence snippets that appear verbatim in the content. A failed call is `error`; a failed validation is `invalid_response`. Both keep every model score NULL. The assessment then uses the rule-based values, labelled `rules_fallback_after_model_failure`, and is not routed unless `route_on_llm_error`. The pipeline never invents a model score.
7. **Score.** Components are stored separately:

   | Component | Rule-based value (scoring v1) | From the model when `llm_assessed` |
   | --- | --- | --- |
   | relevance | `category_relevance` of the best accepted match, +0.05 for a second term, +0.05 for a satisfied context rule (max 1) | yes |
   | intent / intent_score | Best keyword in `intent_taxonomy`, else `general_discussion` 0.2 | intent (score from the taxonomy) |
   | fit | 0.5 + 0.15 per `fit_keywords` hit (max 1); 0 on a disqualifying keyword | yes |
   | engagement | `ln(1 + signal) / ln(1 + engagement_saturation)`, max 1. Signal is score or points plus twice the comments. | no |
   | freshness | `exp(-ln 2 × age_hours / freshness_half_life_hours)` | no |
   | authenticity | 1 − 0.2 for a missing author − 0.3 for text under 40 characters | no |
   | confidence | 0.5 + 0.15 intent keyword + 0.1 second term + 0.1 context rule + 0.1 for 200+ characters (max 0.95) | yes |

   `priority = Σ priority_weights[c] × component[c]`, rounded to 4 places. A custom check recomputes it on every run. `reasons_json` lists the human-readable reasons.
8. **Route.** An eligible assessment whose status is `deterministic` or `llm_assessed` routes when it meets `min_relevance`, `min_priority`, `min_confidence` and `min_intent_score`. Routing writes one row to `fct_alert_decision` with `alert_key = md5(content_id | destination | routing_policy_version)`. The row is never updated, so backfills, late arrivals and model re-assessments cannot alert twice.
9. **Deliver.** `alert_delivery_attempt` sends pending alerts for the current destination: highest priority first, at most `max_alerts_per_run`, one attempt per alert per run, up to `max_delivery_attempts` in total. Each request carries `Idempotency-Key: <alert_key>`. Delivery is at-least-once: if a run dies after sending but before recording the attempt, the next run sends again. Webhook receivers can drop the repeat by that key; Slack incoming webhooks ignore it. Transient HTTP errors are retried within the attempt (`http_max_retries`). Dry-run records one `dry_run` row and sends nothing. Delivery never touches the source.
10. **Review.** Reviewers record outcomes in `assets/operations/review/human_feedback.csv`, which feeds `fct_human_feedback`. Drafts are approved or rejected in `reply_draft_reviews.csv`. Feedback is evidence for evaluation and threshold reviews. Nothing reads it to change rules or scores automatically.

## Operations

### Schedules

`pipeline.yml` has no `schedule`, so nothing runs on a timer until you choose to. Assets are tagged by tier:

| Tier | Tag | What runs | Suggested cadence |
| --- | --- | --- | --- |
| Collection | `collect` | `config.settings` and the raw collectors | every `reddit_poll_minutes` (15 min) |
| Enrichment and routing | `enrich` | config, staging, matching, candidates, model, assessments, routing, delivery, outbox, feedback, drafts, queue, health | hourly or daily (your alert latency) |
| Reporting | `report` | share of voice and health | weekly (plus the digest agent on weekdays) |

With cron, CI or any scheduler:

```bash
bash scripts/run_tier.sh collect 15 --environment production --force --var demo_mode=false
bash scripts/run_tier.sh enrich --environment production --force --var demo_mode=false
bash scripts/run_tier.sh report --environment production --force --var demo_mode=false
```

Run the whole pipeline once before scheduling tiers, so every table exists.

In Bruin Cloud a pipeline has one schedule. The simplest setup is to add `schedule: "*/15 * * * *"` (or `hourly`) to `pipeline.yml`, so each run collects its own interval and processes it. The per-run cost is small because enrichment only touches matched content. Cloud schedules cannot filter by tag, so for separate cadences run the tiers from your own scheduler with `scripts/run_tier.sh`. See [bruin-cloud-setup.md](bruin-cloud-setup.md).

### Backfills and watermarks

- The collection window is Bruin's interval `[start, end)`, widened by `late_arrival_hours`, and capped at `source_window_max_days`. Backfill in chunks:
  ```bash
  for day in 2026-01-01 2026-01-02 2026-01-03; do
    next=$(python3 -c "import datetime,sys; print(datetime.date.fromisoformat(sys.argv[1]) + datetime.timedelta(days=1))" "$day")
    bruin run . --var demo_mode=false --start-date "$day" --end-date "$next"
  done
  ```
- Re-running a window is safe. Identical records add nothing, and alerts are keyed by content, not by run.
- Reddit search counts back from now and caps results, so deep Reddit backfills are incomplete; Hacker News, GitHub and Stack Overflow filter by date on the server.
- Backfilled content older than `alert_max_age_hours` is recorded as `expired` in the outbox instead of being sent.
- Each run records its high watermark (newest `published_at` seen) and source cursor in `operations.fct_source_run`. `mart_pipeline_health.watermark_lag_hours` shows how far behind a source is.

### Health

`marts.mart_pipeline_health` has one row per source, plus `model` and `delivery`. `health_status` is `ok`, `attention` (see `health_reasons_json`: `stale`, `abnormal_volume`, `errors`, `model_errors`, `lag`, `delivery_failed`) or `disabled`. The optional pipeline-health agent reports only `attention` rows and never triggers a backfill.

### Evaluation

`fixtures/evaluation_cases.json` lists expected outcomes for the demo data: true positives, false positives, ambiguous terms, competitor context, excluded self, bot, duplicate and job-post content, a redaction and an out-of-window record. After a default demo run:

```bash
python3 tests/evaluate_demo.py
```

For production, compare human feedback with assessments:

```sql
SELECT assessed_intent, corrected_intent, is_false_positive, COUNT(*)
FROM operations.fct_human_feedback
GROUP BY ALL;
```

Change thresholds or rules on purpose, bump `scoring_version` or `matcher_version`, and compare the old and new assessment versions side by side. Both stay in `fct_mention_assessment`.

### Invariant checks

`bash scripts/run_checks.sh` runs the cross-table queries in `checks/`: no duplicate raw rows or alerts, no double delivery, no model scores after a model failure, explainable queue items, review-only drafts, and applied redactions.

### Redaction

`bash scripts/redact.sh <source> <external_id> "<reason>"`, then `bruin run .`. See [sources-and-privacy.md](sources-and-privacy.md#deletion-and-redaction).

## Safety model

| Risk | Control |
| --- | --- |
| Posting, DMs or CRM changes | No adapter can post or write back. `allow_mutations=true` fails validation. Drafts have no publishing fields. |
| Accidental notification spam | Dry-run by default; per-run cap; alert keys stable across backfills; expiry for old content; changing the destination stops delivery to the old one (the new destination gets its own alerts for items younger than `alert_max_age_hours`). |
| Duplicate delivery | Delivered alerts are never re-sent. Idempotency-Key header. A custom check fails the run if any alert is delivered twice. |
| Invented model output | Validated JSON only; evidence must be verbatim; failures keep NULL scores; `model_generated` and `score_basis` labels; routing ignores failed items by default. |
| Prompt injection from content | Content is sent as quoted data and the prompt says to ignore instructions in it. The model can only return scores; it cannot act. |
| Privacy | Public or authorised sources; minimal fields; no author handle in alerts or model calls; no profile enrichment; redaction script. |
| Secrets exposure | Secrets only in generic connections, from environment variables or Cloud secrets; never in variables, prompts, plans or state files. `config.settings` stores only whether each is present. |
| Silent data gaps | Run summaries, partial-error counts, health statuses, coverage in share of voice. |

## Bruin Cloud agents

Version-controlled agent files live in `agents/`. Installing the template never creates or activates an agent.

| File | Purpose |
| --- | --- |
| `agents/mention-ops.prompt.md` | Read-only operational agent over the approved marts. |
| `agents/weekday-mention-digest.plan.yml` | Draft weekday digest: 24 hours plus a late-arrival lookback, remembers delivered alert keys, explicit "no new items" reply. |
| `agents/pipeline-health-monitor.plan.yml` | Draft daily health check; alerts only on problems; never triggers a backfill. |
| `agents/mention-researcher.prompt.md` | On-demand research brief for one content ID, with confidence labels and source links. |
| `agents/reply-drafter.prompt.md` | On-demand draft for one content ID, returned as a `needs_review` row for `reply_draft_submissions.csv`. It cannot publish. |

Setup, least-privilege connections and draft-first activation: [bruin-cloud-setup.md](bruin-cloud-setup.md) and [agents/README.md](../agents/README.md).

## Porting to another warehouse

The SQL assets use ANSI SQL plus the macros in `macros/social_listening.sql`. To run on Postgres, BigQuery, Snowflake or ClickHouse:

1. Set `default.type` in `pipeline.yml` to the platform's SQL asset type (for example `pg.sql`, `bq.sql`, `sf.sql` or `clickhouse.sql`) and change the seeds' `type` to the matching seed type.
2. Declare the warehouse connection as `social-listening-warehouse` and remove `max_concurrent_assets` if the warehouse handles concurrent writers.
3. Reimplement the macros for the dialect:

   | Macro | DuckDB | Postgres | BigQuery | Snowflake | ClickHouse |
   | --- | --- | --- | --- | --- | --- |
   | `sl_hash` | `md5(x)` | `md5(x)` | `TO_HEX(MD5(x))` | `MD5(x)` | `lower(hex(MD5(x)))` |
   | `sl_json_str` | `json_extract_string` | `x #>> '{path}'` | `JSON_VALUE` | `x:path::string` | `JSONExtractString` |
   | `sl_json_object` | `json_object` | `json_build_object` | `TO_JSON_STRING(STRUCT(...))` | `OBJECT_CONSTRUCT` | `toJSONString(map(...))` |
   | `sl_json_array_agg` / `sl_json_array_of` | `to_json(list(...))` | `json_agg` / `json_build_array` | `TO_JSON_STRING(ARRAY_AGG(...))` | `ARRAY_AGG` / `ARRAY_CONSTRUCT_COMPACT` | `toJSONString(groupArray(...))` |
   | `sl_string_agg` | `string_agg` | `string_agg` | `STRING_AGG` | `LISTAGG` | `arrayStringConcat(groupUniqArray(...))` |
   | `sl_regex_*` | `regexp_matches` / `regexp_extract` / `regexp_replace` | `~` / `substring(... from ...)` / `regexp_replace(..., 'g')` | `REGEXP_CONTAINS` / `REGEXP_EXTRACT` / `REGEXP_REPLACE` | `REGEXP_LIKE` / `REGEXP_SUBSTR` / `REGEXP_REPLACE` | `match` / `extract` / `replaceRegexpAll` |
   | `sl_hours_between` | `date_diff('second', a, b)/3600` | `extract(epoch from b - a)/3600` | `TIMESTAMP_DIFF(b, a, SECOND)/3600` | `DATEDIFF('second', a, b)/3600` | `dateDiff('second', a, b)/3600` |
   | `sl_now_utc` | `timezone('UTC', current_timestamp)` | `now() at time zone 'utc'` | `CURRENT_TIMESTAMP()` | `SYSDATE()` | `now('UTC')` |

4. Replace the `CREATE TABLE IF NOT EXISTS` pre-hooks on `fct_mention_assessment` and `fct_alert_decision` with the dialect's DDL.
5. Replace the `VALUES` lists in `macros/terms.sql` for BigQuery (`UNNEST([STRUCT(...)])`), and `INTERVAL n HOUR` literals where the syntax differs.
6. Add `bruin-sdk[<platform>]` to `requirements.txt`. The Python assets read through `bruin.get_connection`, so they need no other change.
7. Run the demo against the new warehouse (`demo_mode=true`), then `python3 tests/evaluate_demo.py` and `bash scripts/run_checks.sh`.

## Common failure modes and troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `Invalid social-listening configuration:` followed by a list | A variable failed validation. `bruin validate` checks asset definitions only; these policy checks run in `config.settings` at the start of `bruin run`. | Fix each listed item. The message names the variable or connection. |
| `Your DuckDB version … is not yet supported by MotherDuck` | The Python environment has a newer DuckDB than MotherDuck supports. | Keep the `duckdb==` pin in `requirements.txt` at a supported release. |
| Run hangs or exits with `The operation is cancelled` | Bruin asks for confirmation on environments named like "prod". | Add `--force` for unattended runs. |
| `requires a value for the generic connection 'sl-…'` | A live source or delivery is enabled without its secret. | Export the `SL_…` variable, or add the connection in Bruin Cloud. |
| `Could not set lock on file "….duckdb"` | Another process has the DuckDB file open (for example a DuckDB shell or IDE). | Close it. Keep `max_concurrent_assets: 1` on the DuckDB connection. |
| `collection window … is longer than source_window_max_days` | The backfill range is too long for one run. | Run it in daily chunks. |
| `reddit collection failed: HTTP 401` | Wrong Reddit credentials, or a generic User-Agent. | Check the app, the secret and the User-Agent format. |
| Health shows `stale` | No successful collection within `stale_after_minutes`. | Check the collection schedule and the source's run summary in `operations.fct_source_run`. |
| Health shows `errors` with partial errors | A community or query failed, or `max_pages_per_source` was reached. | Read `partial_errors_json` in `fct_source_run`; narrow the window or raise the page limit. |
| Nothing is routed | Thresholds too strict, or the content has no actionable intent. | Look at `queue_state = 'below_threshold'` rows and their components in `mart_mention_queue`. |
| Too many false positives | Ambiguous phrases. | Add a `term_rules` entry with `requires_context_any` or `excludes_context_any`, or add `excluded_terms`. |
| Items stuck in `needs_review_model` | The model failed or is over budget. | Check `llm_error_message`; raise `llm_max_items_per_run`; fix the provider settings. |
| Outbox shows `failed_permanent` | The webhook rejected every attempt. | Check `last_error` and `last_http_status`, fix the receiver, then bump `routing_policy_version` only if you want the items re-alerted. |
| Nothing delivered after turning off dry-run | Alerts for content older than `alert_max_age_hours` are expired. | Expected. New content will alert. |
| First run of a Python asset is slow | uv installs `bruin-sdk` into a cached environment. | Later runs reuse it. |

## Testing and CI

```bash
bash tests/run_demo.sh            # unit tests + full end-to-end demo suite
python3 -m unittest discover -s tests -t .   # unit tests only (standard library)
```

The unit tests cover settings validation, window boundaries, HTTP retries and rate limits, Reddit and Hacker News pagination, window splitting, de-duplication and event keys, every collector's parsing, model output validation and failures, and delivery planning, retries and idempotency. `tests/e2e_demo.py` runs the real pipeline and checks idempotent reruns, the evaluation set, model failures, live delivery against a local receiver that fails twice, policy validation, and a bounded backfill.

In the Bruin repository, `.github/workflows/social-listening-template.yml` builds Bruin, checks Python formatting and linting, and runs `bruin validate`, the unit tests and the DuckDB fixture pipeline.
