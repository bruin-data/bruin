/* @bruin

name: marts.ai_coding_usage_by_user_day
type: duckdb.sql

description: Cross-platform fact table with one record per user, UTC day, and AI coding platform.

materialization:
  type: table

depends:
  - marts.anthropic_usage_by_user_day
  - marts.cursor_usage_by_user_day

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
  - name: platform
    type: varchar
    description: Anthropic or Cursor.
    primary_key: true
    checks:
      - name: not_null
  - name: total_tokens
    type: hugeint
    description: Input, output, cache-read, and cache-creation tokens combined.
    checks:
      - name: non_negative
  - name: estimated_cost_usd
    type: double
    description: Estimated platform cost in US dollars.
    checks:
      - name: non_negative

@bruin */

SELECT
  usage_date,
  user_id,
  user_type,
  platform,
  is_active,
  sessions,
  requests,
  input_tokens,
  output_tokens,
  cache_read_tokens,
  cache_creation_tokens,
  total_tokens,
  estimated_cost_usd,
  lines_added,
  lines_removed,
  net_lines_changed,
  ai_lines_added,
  ai_lines_removed,
  commits_by_ai,
  pull_requests_by_ai,
  primary_model
FROM marts.anthropic_usage_by_user_day

UNION ALL

SELECT
  usage_date,
  user_id,
  user_type,
  platform,
  is_active,
  sessions,
  requests,
  input_tokens,
  output_tokens,
  cache_read_tokens,
  cache_creation_tokens,
  total_tokens,
  estimated_cost_usd,
  lines_added,
  lines_removed,
  net_lines_changed,
  ai_lines_added,
  ai_lines_removed,
  commits_by_ai,
  pull_requests_by_ai,
  primary_model
FROM marts.cursor_usage_by_user_day
WHERE is_active;
