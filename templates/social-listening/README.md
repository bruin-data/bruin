# Social listening

Find relevant public conversations, qualify them transparently, route a review queue, and prepare reply drafts for human approval. This template is warehouse-first: it never auto-posts, sends messages, creates CRM records, or changes a third-party system.

## Quick start: zero-credential DuckDB demo

```shell
bruin init social-listening
cd social-listening
cp .bruin.yml.example .bruin.yml
bruin validate .
bruin run --config-file .bruin.yml --workers 1 .
```

The fixtures produce Reddit and Hacker News records, deterministic term matches, explainable assessments, a review queue, and a dry-run outbox. Re-run the same window to confirm unique raw IDs and alert keys.

```shell
bruin run --var 'tracked_terms=["example alternative","your term"]' .
bruin run --var 'competitors=["Example competitor"]' --var min_priority=0.75 .
```

## Configuration

All policy settings are typed in `pipeline.yml` and can be supplied locally as JSON values through `--var`. `brand_name`, `brand_description`, `competitors`, `tracked_terms`, source scopes, exclusions, intent taxonomy, thresholds, notification destination, and reply policy are all configuration—not code.

| Variable group | Safe default |
| --- | --- |
| `reddit_enabled`, `github_enabled`, `stackoverflow_enabled`, `slack_community_enabled` | Disabled |
| `hackernews_enabled` | Enabled for the fixture/demo path |
| `reddit_poll_minutes` / `source_lookback_hours` / `max_pages_per_source` | 15 / 48 / 5 (bounded) |
| `min_relevance` / `min_priority` / `min_confidence` | 0.70 / 0.65 / 0.60 |
| `llm_enabled` | Disabled; deterministic assessment remains auditable |
| `notification_destination` / `notification_dry_run` | `none` / `true` |
| `reply_drafts_enabled` / `require_human_approval` | `false` / `true` |
| `allow_mutations` | Always `false` in this template |

Run `python3 scripts/validate_config.py` before enabling an external source or destination. It rejects invalid bounds, disabled approval, mutation attempts, and missing required credentials. Secrets are environment variables or secret-backend values only: `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, `SOCIAL_LISTENING_WEBHOOK_URL`, and `SLACK_WEBHOOK_URL` are examples; never put them in `pipeline.yml`.

## Sources and delivery

- Reddit: use the authorised OAuth or public API route, configured communities, documented rate limits, cursor and a bounded window.
- Hacker News: use the Algolia HN Search API with a page cap and watermark.
- Interfaces are included for authorised GitHub, Stack Exchange, and Slack community data. LinkedIn, X, and Quora require authorised APIs, user exports, or approved providers—never scrape or bypass controls.
- Delivery uses stable alert keys and a bounded-retry, idempotent outbox. Start with dry-run; webhooks do not modify the source content.

For production setup, deletion/redaction, operations, Cloud agents, and the portable data model, read [the complete documentation](docs/index.md).
