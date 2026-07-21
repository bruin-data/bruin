/* @bruin

name: marts.ai_coding_daily_summary
type: duckdb.sql

description: Organization-wide daily AI coding adoption, consumption, productivity, and cost metrics.

materialization:
  type: table

depends:
  - marts.ai_coding_usage_by_user_day

columns:
  - name: usage_date
    type: date
    description: UTC usage date.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: active_users
    type: bigint
    description: Distinct active users or API keys across all platforms.
    checks:
      - name: non_negative
  - name: total_tokens
    type: hugeint
    description: Total tokens across platforms.
    checks:
      - name: non_negative
  - name: estimated_cost_usd
    type: double
    description: Estimated cost across platforms in US dollars.
    checks:
      - name: non_negative

@bruin */

SELECT
  usage_date,
  COUNT(DISTINCT user_type || ':' || user_id) FILTER (WHERE is_active) AS active_users,
  COUNT(DISTINCT platform) AS platform_count,
  STRING_AGG(DISTINCT platform, ',' ORDER BY platform) AS platforms_used,
  SUM(sessions) AS sessions,
  SUM(requests) AS requests,
  SUM(input_tokens) AS input_tokens,
  SUM(output_tokens) AS output_tokens,
  SUM(cache_read_tokens) AS cache_read_tokens,
  SUM(cache_creation_tokens) AS cache_creation_tokens,
  SUM(total_tokens) AS total_tokens,
  SUM(estimated_cost_usd) AS estimated_cost_usd,
  SUM(lines_added) AS lines_added,
  SUM(lines_removed) AS lines_removed,
  SUM(net_lines_changed) AS net_lines_changed,
  SUM(ai_lines_added) AS ai_lines_added,
  SUM(ai_lines_removed) AS ai_lines_removed,
  SUM(commits_by_ai) AS commits_by_ai,
  SUM(pull_requests_by_ai) AS pull_requests_by_ai
FROM marts.ai_coding_usage_by_user_day
GROUP BY usage_date;
