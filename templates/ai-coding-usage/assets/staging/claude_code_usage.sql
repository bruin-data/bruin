/* @bruin

name: staging.claude_code_usage
type: duckdb.sql

description: Typed Anthropic Claude Code usage records with normalized user identifiers and reusable token, cost, and code-change metrics.

materialization:
  type: table

depends:
  - raw.claude_code_usage

columns:
  - name: usage_date
    type: date
    description: UTC date covered by the usage record.
    primary_key: true
    checks:
      - name: not_null
  - name: actor_type
    type: varchar
    description: Whether the actor is a user or an API key.
    primary_key: true
    checks:
      - name: not_null
  - name: actor_id
    type: varchar
    description: User email address or API key name.
    primary_key: true
    checks:
      - name: not_null
  - name: user_id
    type: varchar
    description: Normalized user email or API key name used across platform models.
    checks:
      - name: not_null
  - name: user_type
    type: varchar
    description: User or API key actor classification.
  - name: terminal_type
    type: varchar
    description: Terminal or environment where Claude Code was used.
    primary_key: true
  - name: total_tokens
    type: bigint
    description: Input, output, cache-read, and cache-creation tokens combined.
    checks:
      - name: non_negative
  - name: estimated_cost_usd
    type: double
    description: Estimated Claude Code cost in US dollars.
    checks:
      - name: non_negative
  - name: cache_read_rate
    type: double
    description: Cache-read tokens divided by input plus cache-read tokens.
  - name: net_lines_changed
    type: bigint
    description: Lines added minus lines removed.

@bruin */

WITH typed AS (
  SELECT
    TRY_CAST(date AS DATE) AS usage_date,
    CAST(actor_type AS VARCHAR) AS actor_type,
    CAST(actor_id AS VARCHAR) AS actor_id,
    CAST(organization_id AS VARCHAR) AS organization_id,
    CAST(customer_type AS VARCHAR) AS customer_type,
    CAST(terminal_type AS VARCHAR) AS terminal_type,
    COALESCE(TRY_CAST(num_sessions AS BIGINT), 0) AS num_sessions,
    COALESCE(TRY_CAST(lines_added AS BIGINT), 0) AS lines_added,
    COALESCE(TRY_CAST(lines_removed AS BIGINT), 0) AS lines_removed,
    COALESCE(TRY_CAST(commits_by_claude_code AS BIGINT), 0) AS commits_by_claude_code,
    COALESCE(TRY_CAST(pull_requests_by_claude_code AS BIGINT), 0) AS pull_requests_by_claude_code,
    COALESCE(TRY_CAST(total_input_tokens AS BIGINT), 0) AS total_input_tokens,
    COALESCE(TRY_CAST(total_output_tokens AS BIGINT), 0) AS total_output_tokens,
    COALESCE(TRY_CAST(total_cache_read_tokens AS BIGINT), 0) AS total_cache_read_tokens,
    COALESCE(TRY_CAST(total_cache_creation_tokens AS BIGINT), 0) AS total_cache_creation_tokens,
    COALESCE(TRY_CAST(total_estimated_cost_cents AS DOUBLE), 0.0) AS total_estimated_cost_cents,
    CAST(models_used AS VARCHAR) AS models_used
  FROM raw.claude_code_usage
)

SELECT
  usage_date,
  actor_type,
  actor_id,
  CASE
    WHEN actor_type = 'user_actor' THEN LOWER(TRIM(actor_id))
    ELSE actor_id
  END AS user_id,
  CASE
    WHEN actor_type = 'user_actor' THEN 'user'
    WHEN actor_type = 'api_actor' THEN 'api_key'
    ELSE actor_type
  END AS user_type,
  organization_id,
  customer_type,
  terminal_type,
  num_sessions,
  lines_added,
  lines_removed,
  lines_added - lines_removed AS net_lines_changed,
  commits_by_claude_code,
  pull_requests_by_claude_code,
  total_input_tokens,
  total_output_tokens,
  total_cache_read_tokens,
  total_cache_creation_tokens,
  total_input_tokens
    + total_output_tokens
    + total_cache_read_tokens
    + total_cache_creation_tokens AS total_tokens,
  total_estimated_cost_cents / 100.0 AS estimated_cost_usd,
  total_cache_read_tokens::DOUBLE
    / NULLIF(total_input_tokens + total_cache_read_tokens, 0) AS cache_read_rate,
  models_used
FROM typed;
