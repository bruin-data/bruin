/* @bruin

name: staging.cursor_usage_events
type: duckdb.sql

description: Typed Cursor request events with token and USD cost fields extracted from the tokenUsage JSON object.

materialization:
  type: table

depends:
  - raw.cursor_usage_events

columns:
  - name: event_timestamp
    type: timestamp
    description: UTC event timestamp.
    checks:
      - name: not_null
  - name: usage_date
    type: date
    description: UTC event date.
    checks:
      - name: not_null
  - name: user_id
    type: varchar
    description: Lowercase Cursor team member email address.
    checks:
      - name: not_null
  - name: total_tokens
    type: bigint
    description: Input, output, cache-read, and cache-write tokens combined.
    checks:
      - name: non_negative
  - name: estimated_cost_usd
    type: double
    description: Token-based request cost converted from cents to US dollars.
    checks:
      - name: non_negative

@bruin */

WITH typed AS (
  SELECT
    CASE
      WHEN TRY_CAST("timestamp" AS BIGINT) IS NOT NULL
        THEN TO_TIMESTAMP(TRY_CAST("timestamp" AS BIGINT) / 1000.0)
      ELSE TRY_CAST("timestamp" AS TIMESTAMPTZ)
    END AS event_timestamp,
    LOWER(TRIM(CAST("userEmail" AS VARCHAR))) AS user_id,
    CAST(model AS VARCHAR) AS model,
    CAST(kind AS VARCHAR) AS usage_kind,
    COALESCE(TRY_CAST("maxMode" AS BOOLEAN), FALSE) AS is_max_mode,
    COALESCE(TRY_CAST("requestsCosts" AS DOUBLE), 0.0) AS request_cost_units,
    COALESCE(TRY_CAST("isTokenBasedCall" AS BOOLEAN), FALSE) AS is_token_based_call,
    COALESCE(TRY_CAST("isFreeBugbot" AS BOOLEAN), FALSE) AS is_free_bugbot,
    TRY_CAST("tokenUsage" AS JSON) AS token_usage
  FROM raw.cursor_usage_events
)

SELECT
  event_timestamp,
  CAST(event_timestamp AS DATE) AS usage_date,
  user_id,
  model,
  usage_kind,
  is_max_mode,
  request_cost_units,
  is_token_based_call,
  is_free_bugbot,
  COALESCE(TRY_CAST(JSON_EXTRACT_STRING(token_usage, '$.inputTokens') AS BIGINT), 0) AS input_tokens,
  COALESCE(TRY_CAST(JSON_EXTRACT_STRING(token_usage, '$.outputTokens') AS BIGINT), 0) AS output_tokens,
  COALESCE(TRY_CAST(JSON_EXTRACT_STRING(token_usage, '$.cacheReadTokens') AS BIGINT), 0) AS cache_read_tokens,
  COALESCE(TRY_CAST(JSON_EXTRACT_STRING(token_usage, '$.cacheWriteTokens') AS BIGINT), 0) AS cache_creation_tokens,
  COALESCE(TRY_CAST(JSON_EXTRACT_STRING(token_usage, '$.inputTokens') AS BIGINT), 0)
    + COALESCE(TRY_CAST(JSON_EXTRACT_STRING(token_usage, '$.outputTokens') AS BIGINT), 0)
    + COALESCE(TRY_CAST(JSON_EXTRACT_STRING(token_usage, '$.cacheReadTokens') AS BIGINT), 0)
    + COALESCE(TRY_CAST(JSON_EXTRACT_STRING(token_usage, '$.cacheWriteTokens') AS BIGINT), 0) AS total_tokens,
  COALESCE(TRY_CAST(JSON_EXTRACT_STRING(token_usage, '$.totalCents') AS DOUBLE), 0.0)
    / 100.0 AS estimated_cost_usd
FROM typed;
