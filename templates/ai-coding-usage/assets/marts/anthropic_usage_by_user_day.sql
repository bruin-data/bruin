/* @bruin

name: marts.anthropic_usage_by_user_day
type: duckdb.sql

description: One normalized Anthropic Claude Code consumption record per actor and UTC day.

materialization:
  type: table
  strategy: time_interval
  incremental_key: usage_date
  time_granularity: date

depends:
  - staging.claude_code_usage

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
  - name: sessions
    type: hugeint
    description: Claude Code sessions.
    checks:
      - name: non_negative
  - name: total_tokens
    type: hugeint
    description: Input, output, cache-read, and cache-creation tokens combined.
    checks:
      - name: non_negative
  - name: estimated_cost_usd
    type: double
    description: Estimated Anthropic cost in US dollars.
    checks:
      - name: non_negative

@bruin */

SELECT
  usage_date,
  user_id,
  user_type,
  'anthropic' AS platform,
  TRUE AS is_active,
  SUM(num_sessions) AS sessions,
  CAST(0 AS HUGEINT) AS requests,
  SUM(total_input_tokens) AS input_tokens,
  SUM(total_output_tokens) AS output_tokens,
  SUM(total_cache_read_tokens) AS cache_read_tokens,
  SUM(total_cache_creation_tokens) AS cache_creation_tokens,
  SUM(total_tokens) AS total_tokens,
  SUM(estimated_cost_usd) AS estimated_cost_usd,
  SUM(lines_added) AS lines_added,
  SUM(lines_removed) AS lines_removed,
  SUM(net_lines_changed) AS net_lines_changed,
  SUM(lines_added) AS ai_lines_added,
  SUM(lines_removed) AS ai_lines_removed,
  SUM(commits_by_claude_code) AS commits_by_ai,
  SUM(pull_requests_by_claude_code) AS pull_requests_by_ai,
  ARG_MAX(models_used, num_sessions) AS primary_model,
  COUNT(DISTINCT terminal_type) AS terminal_count
FROM staging.claude_code_usage
WHERE usage_date BETWEEN CAST('{{ start_date }}' AS DATE) AND CAST('{{ end_date }}' AS DATE)
GROUP BY usage_date, user_id, user_type;
