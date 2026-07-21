/* @bruin

name: marts.ai_coding_user_summary
type: duckdb.sql

description: Cross-platform AI coding adoption and consumption metrics across loaded history for each user or API key.

materialization:
  type: table

depends:
  - marts.ai_coding_user_daily_summary
  - marts.ai_coding_usage_by_user_day

columns:
  - name: user_id
    type: varchar
    description: Normalized user email or API key name.
    primary_key: true
    checks:
      - name: not_null
  - name: user_type
    type: varchar
    description: User or API key actor classification.
    primary_key: true
    checks:
      - name: not_null
  - name: active_days
    type: bigint
    description: Distinct active days across loaded history.
    checks:
      - name: positive
  - name: total_tokens
    type: hugeint
    description: Total tokens across platforms and loaded history.
    checks:
      - name: non_negative
  - name: estimated_cost_usd
    type: double
    description: Estimated cost across platforms and loaded history in US dollars.
    checks:
      - name: non_negative

@bruin */

WITH platform_rollup AS (
  SELECT
    user_id,
    user_type,
    STRING_AGG(DISTINCT platform, ',' ORDER BY platform) AS platforms_used
  FROM marts.ai_coding_usage_by_user_day
  GROUP BY user_id, user_type
)

SELECT
  daily.user_id,
  daily.user_type,
  MIN(daily.usage_date) AS first_active_date,
  MAX(daily.usage_date) AS last_active_date,
  COUNT(*) FILTER (WHERE daily.is_active) AS active_days,
  platform_rollup.platforms_used,
  MAX(daily.platform_count) AS max_platforms_in_one_day,
  SUM(daily.sessions) AS sessions,
  SUM(daily.requests) AS requests,
  SUM(daily.input_tokens) AS input_tokens,
  SUM(daily.output_tokens) AS output_tokens,
  SUM(daily.cache_read_tokens) AS cache_read_tokens,
  SUM(daily.cache_creation_tokens) AS cache_creation_tokens,
  SUM(daily.total_tokens) AS total_tokens,
  SUM(daily.estimated_cost_usd) AS estimated_cost_usd,
  SUM(daily.lines_added) AS lines_added,
  SUM(daily.lines_removed) AS lines_removed,
  SUM(daily.net_lines_changed) AS net_lines_changed,
  SUM(daily.ai_lines_added) AS ai_lines_added,
  SUM(daily.ai_lines_removed) AS ai_lines_removed,
  SUM(daily.commits_by_ai) AS commits_by_ai,
  SUM(daily.pull_requests_by_ai) AS pull_requests_by_ai,
  SUM(daily.total_tokens)::DOUBLE
    / NULLIF(COUNT(*) FILTER (WHERE daily.is_active), 0) AS tokens_per_active_day,
  SUM(daily.estimated_cost_usd)
    / NULLIF(COUNT(*) FILTER (WHERE daily.is_active), 0) AS estimated_cost_per_active_day_usd
FROM marts.ai_coding_user_daily_summary AS daily
INNER JOIN platform_rollup
  ON daily.user_id = platform_rollup.user_id
  AND daily.user_type = platform_rollup.user_type
GROUP BY daily.user_id, daily.user_type, platform_rollup.platforms_used;
