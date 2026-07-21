/* @bruin

name: marts.ai_coding_user_daily_summary
type: duckdb.sql

description: One cross-platform AI coding consumption summary per user and UTC day.

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
  - name: platform_count
    type: bigint
    description: Number of AI coding platforms used that day.
    checks:
      - name: positive
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
  user_id,
  user_type,
  COUNT(*) AS platform_count,
  STRING_AGG(platform, ',' ORDER BY platform) AS platforms_used,
  BOOL_OR(is_active) AS is_active,
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
  SUM(pull_requests_by_ai) AS pull_requests_by_ai,
  STRING_AGG(DISTINCT primary_model, ',' ORDER BY primary_model)
    FILTER (WHERE primary_model IS NOT NULL AND primary_model <> '') AS models_used
FROM marts.ai_coding_usage_by_user_day
GROUP BY usage_date, user_id, user_type;
