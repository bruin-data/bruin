/* @bruin

name: marts.ai_coding_usage_by_user_model_day
type: duckdb.sql

description: Cross-platform token and cost usage by user, UTC day, platform, and model or source-provided model set.

materialization:
  type: table
  strategy: time_interval
  incremental_key: usage_date
  time_granularity: date

depends:
  - staging.claude_code_usage
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
  - name: model
    type: varchar
    description: Exact model, source-provided model set, or unknown when no model was reported.
    primary_key: true
    checks:
      - name: not_null
  - name: model_attribution
    type: varchar
    description: Whether metrics are attributed to an exact model, a model set, or an unknown model.
    checks:
      - name: not_null
  - name: total_tokens
    type: hugeint
    description: Input, output, cache-read, and cache-creation tokens combined.
    checks:
      - name: non_negative
  - name: estimated_cost_usd
    type: double
    description: Estimated model or model-set cost in US dollars.
    checks:
      - name: non_negative

@bruin */

WITH anthropic_labeled AS (
  SELECT
    usage_date,
    user_id,
    user_type,
    CASE
      WHEN models_used IS NULL OR TRIM(models_used) = '' THEN 'unknown'
      ELSE TRIM(models_used)
    END AS model,
    CASE
      WHEN models_used IS NULL OR TRIM(models_used) = '' THEN 'unknown'
      WHEN STRPOS(models_used, ',') > 0 THEN 'model_set'
      ELSE 'exact'
    END AS model_attribution,
    num_sessions AS sessions,
    total_input_tokens AS input_tokens,
    total_output_tokens AS output_tokens,
    total_cache_read_tokens AS cache_read_tokens,
    total_cache_creation_tokens AS cache_creation_tokens,
    total_tokens,
    estimated_cost_usd
  FROM staging.claude_code_usage
  WHERE usage_date BETWEEN CAST('{{ start_date }}' AS DATE) AND CAST('{{ end_date }}' AS DATE)
),
anthropic_rollup AS (
  SELECT
    usage_date,
    user_id,
    user_type,
    'anthropic' AS platform,
    model,
    model_attribution,
    SUM(sessions) AS sessions,
    CAST(0 AS HUGEINT) AS requests,
    SUM(input_tokens) AS input_tokens,
    SUM(output_tokens) AS output_tokens,
    SUM(cache_read_tokens) AS cache_read_tokens,
    SUM(cache_creation_tokens) AS cache_creation_tokens,
    SUM(total_tokens) AS total_tokens,
    SUM(estimated_cost_usd) AS estimated_cost_usd
  FROM anthropic_labeled
  GROUP BY usage_date, user_id, user_type, model, model_attribution
),
cursor_rollup AS (
  SELECT
    usage_date,
    user_id,
    'user' AS user_type,
    'cursor' AS platform,
    CASE
      WHEN model IS NULL OR TRIM(model) = '' THEN 'unknown'
      ELSE TRIM(model)
    END AS model,
    CASE
      WHEN model IS NULL OR TRIM(model) = '' THEN 'unknown'
      ELSE 'exact'
    END AS model_attribution,
    CAST(0 AS HUGEINT) AS sessions,
    CAST(COUNT(*) AS HUGEINT) AS requests,
    SUM(input_tokens) AS input_tokens,
    SUM(output_tokens) AS output_tokens,
    SUM(cache_read_tokens) AS cache_read_tokens,
    SUM(cache_creation_tokens) AS cache_creation_tokens,
    SUM(total_tokens) AS total_tokens,
    SUM(estimated_cost_usd) AS estimated_cost_usd
  FROM staging.cursor_usage_events
  WHERE usage_date BETWEEN CAST('{{ start_date }}' AS DATE) AND CAST('{{ end_date }}' AS DATE)
  GROUP BY usage_date, user_id, model, model_attribution
)

SELECT * FROM anthropic_rollup
UNION ALL
SELECT * FROM cursor_rollup;
