# Sources and privacy

This template only works with content you are allowed to read, and it stores as little about people as it can. This page lists what each source supports, what is stored, and how to delete a record.

It is not legal advice. Check each platform's current terms and your own obligations before enabling a source in production.

## Principles

- Public or authorised content only. No scraping, no bypassing logins, paywalls, rate limits or robots rules, and no unofficial APIs.
- No private messages, private channels or closed groups.
- No inference of sensitive personal attributes (health, religion, politics, sexuality, ethnicity and similar). The model prompt forbids guessing identity, employer or location.
- Author and profile enrichment is off (`author_enrichment_enabled=false`) and no adapter ships for it.
- Nothing is written back to a source. The template never posts, replies, votes, sends DMs, or creates or updates CRM records. `allow_mutations=true` fails validation: mutation-capable adapters must be built as a separate, approval-gated integration.
- Model assessments and stale or missing data are labelled wherever they appear: `model_generated`, `score_basis`, `assessment_status` and `data_warnings` in alerts, and `health_status` in `marts.mart_pipeline_health`.

## Sources

| Source | Status | Access path | Credentials | Limits the collector respects |
| --- | --- | --- | --- | --- |
| Reddit | On (`reddit_enabled`) | Official OAuth API, application-only (client credentials) | `sl-reddit-client-id`, `sl-reddit-client-secret`, `sl-reddit-user-agent` | 1 request/second pacing, `X-Ratelimit-*` headers, `Retry-After`, `max_pages_per_source`, bounded window |
| Hacker News | On (`hackernews_enabled`) | Algolia HN Search API (`search_by_date`) | None | 4 requests/second pacing, 1,000-hit cap handled by splitting the window, `max_pages_per_source` |
| GitHub | Off (`github_enabled`) | REST search API, `is:public` only | `sl-github-token` (public read-only) | 2.1 s pacing (30 searches/minute), 1,000-result cap handled by splitting |
| Stack Overflow | Off (`stackoverflow_enabled`) | Stack Exchange API 2.3 `search/advanced` | Optional `sl-stackexchange-key` | 0.5 s pacing, the API's `backoff` field |
| Authorised exports | Off (`authorised_export_enabled`) | A CSV you provide at `authorised_export_path` | None | Reads a local file only |
| Slack community | Interface only | Not implemented; `slack_community_enabled=true` fails validation | — | See `scripts/social_listening/sources/slack_community.py` |

### Reddit

- Register an app under an account you control and use a descriptive User-Agent (`<platform>:<app id>:<version> (by /u/<account>)`). Reddit blocks generic agents.
- Posts come from search (all of Reddit, or each community in `reddit_communities_include`). Comments are only read from included communities, because Reddit search does not index comments, and only comments containing a tracked phrase are stored.
- Communities in `reddit_communities_exclude` are dropped before storage.
- Stored fields: `id`, `name`, `subreddit`, `author`, `title`, `selftext`/`body`, `permalink`, `url`, `created_utc`, `score`, `num_comments`, `parent_id`, `link_id`, `over_18`, `stickied`, `distinguished`, `removed_by_category`, `crosspost_parent`, `is_self`. Account IDs, flair, avatars and awards are not stored.
- Honour deletions: when a post or comment is deleted or removed on Reddit, delete it here too (see below). Removed content that is re-collected is marked `is_removed` and never matched.

### Hacker News

- Content is public. The Algolia API needs no key; keep request volume modest.
- Stored fields: `objectID`, `_tags`, `title`, `url`, `story_text`, `comment_text`, `author`, `points`, `num_comments`, `parent_id`, `story_id`, `story_title`, `created_at_i`.

### GitHub and Stack Overflow

- Both are off by default. GitHub search is limited to public repositories; use a fine-grained token with no write scopes.
- Stack Overflow content is licensed CC BY-SA. Keep the link and the author's display name when you show it.

### LinkedIn, X and Quora

There is no supported collector for these platforms. The template does not scrape them and does not ship or endorse unofficial integrations. Supported paths:

1. An official API under your own agreement with the platform. Write a collector that yields `RawRecord`s (copy `sources/hackernews.py`) and add a `raw.raw_<source>_content` asset.
2. An export you are entitled to (your own page's data, a data request, or an approved data provider). Save it as CSV with the columns `platform, external_id, url, author, title, body, published_at, content_type[, metrics_json]`, set `authorised_export_path`, and enable `authorised_export_enabled`. `fixtures/exports/sample_export.csv` shows the format.

### CRM enrichment and write-back

Not included. If you add one, build it as a separate, explicit adapter that reads approved queue rows, requires `allow_mutations: true` in its own configuration and a recorded human approval for each write, and never runs inside this pipeline's default flow.

## What is stored

| Table | Personal data | Notes |
| --- | --- | --- |
| `raw.raw_<source>_content` | Public handle, public text | Immutable record versions; the minimal field lists above. |
| `staging.stg_content_item` | Public handle, public text | Latest version only; redacted records removed. |
| `enrichment.*` | None beyond the content ID and matched text | Scores and reasons. |
| `operations.fct_alert_decision` / `fct_alert_outbox` | Title and a 280-character snippet in `payload_json` | No author handle is sent in alerts. |
| `operations.fct_human_feedback`, `fct_reply_draft` | Reviewer names you enter | Keep reviewer identifiers to what your team needs. |

Model calls (when `llm_enabled=true`) send the title, body (up to 4,000 characters), source, community and matched terms of eligible candidates only. They never include the author handle. Check your provider's data-retention terms.

## Retention

Nothing is deleted automatically. Choose a retention period and schedule a cleanup, for example deleting raw versions older than 180 days:

```sql
DELETE FROM raw.raw_reddit_content WHERE collected_at < CURRENT_DATE - INTERVAL 180 DAY;
```

Run the pipeline afterwards so staging and marts are rebuilt.

## Deletion and redaction

Delete a record by source and external ID when the author deletes it, the platform removes it, or someone asks:

```bash
bash scripts/redact.sh reddit t3_abc123 "author deleted the post"
bruin run .
```

The script:

1. appends the request to `assets/config/redaction_requests.csv`, so `staging.stg_content_item` drops the record on every future run even if a collector sees it again (commit this file);
2. deletes the raw versions, the model result, the assessment, the routing decision and the delivery attempts for that content ID.

The next run rebuilds staging, matches, candidates, the queue, share of voice and drafts without it. `checks/redactions_are_applied.sql` confirms nothing is left.

External IDs by source: Reddit `t3_<id>` (post) or `t1_<id>` (comment); Hacker News the numeric item ID; GitHub the numeric issue ID; Stack Overflow the question ID; exports `<platform>:<external_id>`.

Alerts that were already delivered live in Slack or the webhook receiver. Use `operations.fct_alert_outbox` to find them and delete them there by hand.
