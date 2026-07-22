/* @bruin

name: marts.cursor_usage_by_user_day
type: duckdb.sql

description: One normalized Cursor consumption and productivity record per user and UTC day.

materialization:
  type: table
  strategy: time_interval
  incremental_key: usage_date
  time_granularity: date

depends:
  - staging.cursor_daily_usage
  - staging.cursor_usage_events

columns:
  - name: usage_date
    type: date
    description: UTC usage date.
    primary_key: true
    checks:
      - name: not_null
  - name: user_id
    type: varchar
    description: Lowercase Cursor team member email address.
    primary_key: true
    checks:
      - name: not_null
  - name: requests
    type: hugeint
    description: Cursor requests, preferring the daily activity total when available.
    checks:
      - name: non_negative
  - name: total_tokens
    type: hugeint
    description: Input, output, cache-read, and cache-creation tokens from usage events.
    checks:
      - name: non_negative
  - name: estimated_cost_usd
    type: double
    description: Token-based Cursor cost in US dollars.
    checks:
      - name: non_negative

@bruin */

WITH daily_rollup AS (
  SELECT
    usage_date,
    user_id,
    BOOL_OR(is_active) AS is_active,
    SUM(total_lines_added) AS lines_added,
    SUM(total_lines_removed) AS lines_removed,
    SUM(ai_lines_added) AS ai_lines_added,
    SUM(ai_lines_removed) AS ai_lines_removed,
    SUM(apply_operations) AS apply_operations,
    SUM(suggestions_accepted) AS suggestions_accepted,
    SUM(suggestions_rejected) AS suggestions_rejected,
    SUM(tabs_shown) AS tabs_shown,
    SUM(tabs_accepted) AS tabs_accepted,
    SUM(composer_requests) AS composer_requests,
    SUM(chat_requests) AS chat_requests,
    SUM(agent_requests) AS agent_requests,
    SUM(total_requests) AS total_requests,
    SUM(cmdk_usages) AS cmdk_usages,
    SUM(subscription_included_requests) AS subscription_included_requests,
    SUM(api_key_requests) AS api_key_requests,
    SUM(usage_based_requests) AS usage_based_requests,
    SUM(bugbot_usages) AS bugbot_usages,
    ARG_MAX(primary_model, total_requests) AS primary_model
  FROM staging.cursor_daily_usage
  GROUP BY usage_date, user_id
),
event_rollup AS (
  SELECT
    usage_date,
    user_id,
    COUNT(*) AS event_count,
    SUM(input_tokens) AS input_tokens,
    SUM(output_tokens) AS output_tokens,
    SUM(cache_read_tokens) AS cache_read_tokens,
    SUM(cache_creation_tokens) AS cache_creation_tokens,
    SUM(total_tokens) AS total_tokens,
    SUM(estimated_cost_usd) AS estimated_cost_usd,
    SUM(request_cost_units) AS request_cost_units,
    MODE(model) AS primary_model
  FROM staging.cursor_usage_events
  GROUP BY usage_date, user_id
)

SELECT
  COALESCE(daily_rollup.usage_date, event_rollup.usage_date) AS usage_date,
  COALESCE(daily_rollup.user_id, event_rollup.user_id) AS user_id,
  'user' AS user_type,
  'cursor' AS platform,
  COALESCE(daily_rollup.is_active, FALSE)
    OR event_rollup.user_id IS NOT NULL
    OR COALESCE(daily_rollup.total_requests, 0) > 0 AS is_active,
  CAST(0 AS HUGEINT) AS sessions,
  CASE
    WHEN COALESCE(daily_rollup.total_requests, 0) > 0 THEN daily_rollup.total_requests
    ELSE COALESCE(event_rollup.event_count, 0)
  END AS requests,
  COALESCE(event_rollup.input_tokens, 0) AS input_tokens,
  COALESCE(event_rollup.output_tokens, 0) AS output_tokens,
  COALESCE(event_rollup.cache_read_tokens, 0) AS cache_read_tokens,
  COALESCE(event_rollup.cache_creation_tokens, 0) AS cache_creation_tokens,
  COALESCE(event_rollup.total_tokens, 0) AS total_tokens,
  COALESCE(event_rollup.estimated_cost_usd, 0.0) AS estimated_cost_usd,
  COALESCE(daily_rollup.lines_added, 0) AS lines_added,
  COALESCE(daily_rollup.lines_removed, 0) AS lines_removed,
  COALESCE(daily_rollup.lines_added, 0) - COALESCE(daily_rollup.lines_removed, 0) AS net_lines_changed,
  COALESCE(daily_rollup.ai_lines_added, 0) AS ai_lines_added,
  COALESCE(daily_rollup.ai_lines_removed, 0) AS ai_lines_removed,
  CAST(0 AS HUGEINT) AS commits_by_ai,
  CAST(0 AS HUGEINT) AS pull_requests_by_ai,
  COALESCE(daily_rollup.primary_model, event_rollup.primary_model) AS primary_model,
  COALESCE(daily_rollup.apply_operations, 0) AS apply_operations,
  COALESCE(daily_rollup.suggestions_accepted, 0) AS suggestions_accepted,
  COALESCE(daily_rollup.suggestions_rejected, 0) AS suggestions_rejected,
  COALESCE(daily_rollup.tabs_shown, 0) AS tabs_shown,
  COALESCE(daily_rollup.tabs_accepted, 0) AS tabs_accepted,
  COALESCE(daily_rollup.composer_requests, 0) AS composer_requests,
  COALESCE(daily_rollup.chat_requests, 0) AS chat_requests,
  COALESCE(daily_rollup.agent_requests, 0) AS agent_requests,
  COALESCE(daily_rollup.subscription_included_requests, 0) AS subscription_included_requests,
  COALESCE(daily_rollup.api_key_requests, 0) AS api_key_requests,
  COALESCE(daily_rollup.usage_based_requests, 0) AS usage_based_requests,
  COALESCE(event_rollup.request_cost_units, 0.0) AS request_cost_units
FROM daily_rollup
FULL OUTER JOIN event_rollup
  ON daily_rollup.usage_date = event_rollup.usage_date
  AND daily_rollup.user_id = event_rollup.user_id
WHERE COALESCE(daily_rollup.usage_date, event_rollup.usage_date)
  BETWEEN CAST('{{ start_date }}' AS DATE) AND CAST('{{ end_date }}' AS DATE);
