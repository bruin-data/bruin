# AI coding usage to DuckDB

This Bruin template ingests AI coding usage from the Anthropic and Cursor Admin APIs into DuckDB, normalizes both platforms to a shared user/day model, and builds organization-wide summaries.

## Data model

The three raw assets have no dependencies on one another, so Bruin schedules the Anthropic and Cursor API requests in parallel:

- `raw.claude_code_usage`: Per-actor daily Claude Code usage from Anthropic.
- `raw.cursor_daily_usage`: Per-user daily Cursor activity and productivity metrics.
- `raw.cursor_usage_events`: Per-request Cursor token, model, and cost details.

Each source has a typed staging model:

- `staging.claude_code_usage`
- `staging.cursor_daily_usage`
- `staging.cursor_usage_events`

The marts provide progressively broader reporting tables:

- `marts.anthropic_usage_by_user_day`: One Anthropic record per user and day.
- `marts.cursor_usage_by_user_day`: One Cursor record per user and day.
- `marts.ai_coding_usage_by_user_day`: One normalized record per user, day, and platform.
- `marts.ai_coding_user_daily_summary`: One cross-platform record per user and day.
- `marts.ai_coding_daily_summary`: Organization-wide daily usage.
- `marts.ai_coding_user_summary`: Per-user totals across loaded history.

## Prerequisites

Anthropic Claude Code usage requires an organization Admin API key beginning with `sk-ant-admin`. A standard key beginning with `sk-ant-api` cannot access organization usage reports. Create an Admin key in the [Anthropic Console](https://console.anthropic.com/settings/admin-keys).

Cursor analytics requires a **team Admin API key** created in Cursor Dashboard → Settings → Cursor Admin API Keys. Other Cursor keys may use the same prefix but cannot access the team analytics endpoints. See the [Cursor Admin API documentation](https://docs.cursor.com/en/account/teams/admin-api).

Export both credentials in your shell. The generated `.bruin.yml` contains only environment-variable references.

```shell
export ANTHROPIC_ADMIN_API_KEY="sk-ant-admin..."
export CURSOR_ADMIN_API_KEY="crsr_..."
```

The template adds these connections to `.bruin.yml`:

```yaml
default_environment: default
environments:
  default:
    connections:
      anthropic:
        - name: anthropic-default
          api_key: ${ANTHROPIC_ADMIN_API_KEY}
      cursor:
        - name: cursor-default
          api_key: ${CURSOR_ADMIN_API_KEY}
      duckdb:
        - name: duckdb-default
          path: claude_code_usage.duckdb
        - name: duckdb-dashboard
          path: claude_code_usage.duckdb
          read_only: true
```

Change both DuckDB paths if you want the database file stored elsewhere. The pipeline writes through `duckdb-default`; DAC uses the parallel-safe, read-only `duckdb-dashboard` connection.

## Run the pipeline

Validate and run the previous UTC day's data:

```shell
bruin validate .
bruin run .
```

Use an explicit interval to backfill historical data:

```shell
bruin run . --start-date 2025-01-01 --end-date 2025-01-30
```

Cursor limits each analytics request to 30 days. For longer backfills, run multiple non-overlapping intervals of at most 30 days. The per-platform marts use interval-aware materialization, so each run updates only its requested dates while preserving previously loaded history. Both APIs return UTC data.

## Query the models

Open the database with DuckDB:

```shell
duckdb claude_code_usage.duckdb
```

Query per-user/day consumption across platforms:

```sql
SELECT
  usage_date,
  user_id,
  platform_count,
  platforms_used,
  sessions,
  requests,
  total_tokens,
  estimated_cost_usd
FROM marts.ai_coding_user_daily_summary
ORDER BY usage_date DESC, user_id;
```

Query the organization-wide daily summary:

```sql
SELECT *
FROM marts.ai_coding_daily_summary
ORDER BY usage_date DESC;
```

## Open the dashboard

The template includes a DAC dashboard with date and platform filters, headline adoption and consumption metrics, daily trends, platform comparisons, and a per-user table. Install the DAC CLI, then run these commands from the generated pipeline directory after the Bruin pipeline has populated DuckDB:

```shell
dac validate --dir dashboards
dac check --dir dashboards
dac serve --dir dashboards --open
```

To produce a standalone static dashboard with query results baked in:

```shell
dac build --dir dashboards --dashboard "AI Coding Usage" --output build
```

## Metric notes

- User email addresses are lowercased so the same person can be joined across Anthropic and Cursor. Anthropic API actors retain their API key name and use `user_type = 'api_key'`.
- Anthropic costs and Cursor token-based event costs are converted from cents to US dollars.
- Cursor request totals prefer daily Composer, chat, and agent counts; usage-event counts are used when daily request totals are unavailable.
- Cursor total line changes and accepted AI line changes are kept separately.
- Anthropic usage covers the first-party Anthropic API and does not include Claude Code traffic routed through Amazon Bedrock or Google Vertex AI.
