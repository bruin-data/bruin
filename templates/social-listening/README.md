# Social listening

Find public conversations that mention your brand, competitors or topics, qualify them with explainable scores, send a review queue to Slack or a webhook, and draft replies that a person reviews and posts by hand.

The template is warehouse-first and safe by default. It only reads public or authorised sources, it never posts, sends DMs, creates CRM records or changes anything on a source platform, and every alert says why it was raised.

- Sources: Reddit (official OAuth API) and Hacker News (Algolia HN Search API), plus off-by-default GitHub, Stack Overflow and authorised export files (LinkedIn, X, Quora).
- Warehouse: DuckDB locally, MotherDuck in the cloud. The SQL is portable through one macro file.
- Optional: a structured LLM assessment, and draft-only Bruin Cloud agents for a daily digest, pipeline health, research and reply drafting.

Full documentation: [docs/index.md](docs/index.md).

## Quick start (no credentials)

```bash
bruin init social-listening
cd social-listening
bruin validate .
bruin run .
```

`bruin init` adds a DuckDB connection (`social-listening-warehouse`, file `social-listening.duckdb`) and empty secret placeholders to your project's `.bruin.yml`. The demo runs in `demo_mode`: collectors read recorded API responses from `fixtures/` through the same pagination and parsing code used for live sources. Notifications run in dry-run mode, so nothing leaves your machine.

Look at the results:

```bash
bruin query --connection social-listening-warehouse --query "
  SELECT source, external_id, queue_state, priority, intent, matched_text, rule_ids, source_url
  FROM marts.mart_mention_queue ORDER BY priority DESC"

bruin query --connection social-listening-warehouse --query "SELECT * FROM marts.mart_pipeline_health"
bruin query --connection social-listening-warehouse --query "SELECT * FROM operations.fct_alert_outbox"
```

Run it again: nothing is duplicated. `bash tests/run_demo.sh` runs the unit tests and the full end-to-end suite.

## Configure

Every policy choice is a typed custom variable in `pipeline.yml` with a safe default. Override any of them per run:

```bash
bruin run . \
  --var 'brand_name="Your Brand"' \
  --var 'competitors=["Other Co", "Third Tool"]' \
  --var 'tracked_terms=["your brand alternative"]' \
  --var 'min_priority=0.75'
```

Values are JSON, so strings need quotes. `config.settings` validates every value before anything runs and fails with a list of problems (for example, reply drafts without human approval, a polling interval under 5 minutes, or an enabled source without its credentials). The full variable reference is in [docs/index.md](docs/index.md#configuration-reference).

The variables you will change first:

| Variable | Default | What it does |
| --- | --- | --- |
| `brand_name`, `brand_aliases` | `Example Co`, `["exampleco"]` | Brand phrases. |
| `competitors` | `["Rival Suite"]` | One tracked term per competitor. |
| `tracked_terms`, `term_rules` | see `pipeline.yml` | Topics, and rules for ambiguous phrases. |
| `demo_mode` | `true` | Read fixtures instead of calling APIs. |
| `reddit_communities_include` / `_exclude` | `[]` | Limit Reddit to, or away from, communities. |
| `min_relevance`, `min_priority`, `min_confidence`, `min_intent_score` | `0.70`, `0.65`, `0.50`, `0.50` | Routing thresholds. |
| `notification_destination`, `notification_dry_run` | `slack_webhook`, `true` | Where alerts go and whether they are really sent. |
| `llm_enabled` | `false` | Optional model assessment. |
| `reply_drafts_enabled`, `require_human_approval` | `false`, `true` | Draft-only replies; approval cannot be turned off. |

## Go live

1. Register a Reddit app and set the secrets as environment variables (the project `.bruin.yml` reads them): `SL_REDDIT_CLIENT_ID`, `SL_REDDIT_CLIENT_SECRET`, `SL_REDDIT_USER_AGENT`. Hacker News needs none. See [.bruin.yml.example](.bruin.yml.example).
2. Run a bounded dry-run backfill and review the queue:
   ```bash
   bruin run . --var demo_mode=false --start-date 2026-01-01 --end-date 2026-01-02
   ```
3. Set `SL_SLACK_WEBHOOK_URL` (or `SL_WEBHOOK_URL` with `notification_destination="webhook"`) and run with `--var notification_dry_run=false`.
4. Schedule it. `pipeline.yml` has no schedule on purpose. See [docs/index.md#schedules](docs/index.md#schedules) for the collect, enrich and report cadence, and [docs/bruin-cloud-setup.md](docs/bruin-cloud-setup.md) for Bruin Cloud.

Secrets never go in custom variables, prompts or agent plans.

## Layout

```text
pipeline.yml          typed variables (policy); no secrets
.bruin.yml            demo connections merged by `bruin init`
.bruin.yml.example    production connections, including MotherDuck
assets/config/        settings validation, terms, intents, redaction list
assets/raw/           one Python collector per source (raw_<source>_content)
assets/staging/       stg_content_item
assets/enrichment/    term matches, candidates, optional LLM, assessments
assets/operations/    routing, delivery, outbox, feedback, reply drafts
assets/marts/         review queue, share of voice, pipeline health
macros/               the SQL dialect seam and term rendering
checks/               cross-table invariant queries (scripts/run_checks.sh)
scripts/              Python library (standard library only), helper scripts
tests/                unit tests and the end-to-end demo suite
fixtures/             recorded API responses and the evaluation set
agents/               Bruin Cloud agent prompts and draft plans
docs/                 full documentation
```

## Safety model

- Public or authorised content only; no scraping, no private messages, no sensitive-attribute inference.
- Exclusions run before any model call. Model output is validated, labelled as model-generated, and never invented when the model fails.
- Alerts go through an idempotent outbox with bounded retries and a dry-run mode.
- Reply drafts are review-only records with no publishing fields. A person posts by hand.
- Mutation-capable integrations (posting, CRM write-back) are out of scope and blocked by `allow_mutations`.

See [docs/sources-and-privacy.md](docs/sources-and-privacy.md) for source terms, data minimisation and deletion.
