/* @bruin
name: operations.fct_source_run
description: |
  One row per source collection run, read from the run_summary rows every raw
  asset writes (including disabled sources). Carries the window, status,
  request/retry/rate-limit/error counts, record count and high watermark.
tags: [enrich, health]
depends:
  - raw.raw_reddit_content
  - raw.raw_hackernews_content
  - raw.raw_github_content
  - raw.raw_stackoverflow_content
  - raw.raw_authorised_export_content
materialization:
  type: table
columns:
  - name: run_key
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: source
    type: varchar
    checks:
      - name: not_null
  - name: run_status
    type: varchar
    checks:
      - name: accepted_values
        value: [ok, partial, failed, disabled]
custom_checks:
  - name: every enabled source collected recently
    description: Freshness check. Fails when a source whose latest run is enabled has had no successful collection within stale_after_minutes (for example when only the enrich tier is scheduled). Sources whose latest run is disabled are ignored. Non-blocking, so enrichment still runs; marts.mart_pipeline_health shows which source.
    blocking: false
    query: |
      WITH ranked AS (
        SELECT source, run_status, collected_at,
               ROW_NUMBER() OVER (PARTITION BY source ORDER BY collected_at DESC, run_key) AS rn
        FROM operations.fct_source_run
      )
      SELECT COUNT(*) FROM (
        SELECT l.source
        FROM ranked l
        LEFT JOIN operations.fct_source_run r
          ON r.source = l.source AND r.run_status IN ('ok', 'partial')
        WHERE l.rn = 1 AND l.run_status <> 'disabled'
        GROUP BY l.source
        HAVING COALESCE(MAX(r.collected_at), TIMESTAMP '1970-01-01')
               < CAST(timezone('UTC', current_timestamp) AS TIMESTAMP) - INTERVAL {{ var.stale_after_minutes }} MINUTE
      )
    value: 0
@bruin */

WITH summaries AS (
    {%- for table in ['reddit', 'hackernews', 'github', 'stackoverflow', 'authorised_export'] %}
    SELECT source, ingest_run_id, payload_json, CAST(collected_at AS TIMESTAMP) AS collected_at,
           CAST(window_start AS TIMESTAMP) AS window_start, CAST(window_end AS TIMESTAMP) AS window_end, collection_mode
    FROM raw.raw_{{ table }}_content
    WHERE record_kind = 'run_summary'
    {%- if not loop.last %}
    UNION ALL
    {%- endif %}
    {%- endfor %}
)

SELECT
    {{ sl_hash("source || '|' || ingest_run_id") }} AS run_key,
    source,
    ingest_run_id,
    collection_mode,
    collected_at,
    window_start,
    window_end,
    {{ sl_json_str('payload_json', '$.status') }} AS run_status,
    {{ sl_json_str('payload_json', '$.fatal_error') }} AS fatal_error,
    CAST({{ sl_json_num('payload_json', '$.pages') }} AS INTEGER) AS pages,
    CAST({{ sl_json_num('payload_json', '$.records') }} AS INTEGER) AS records,
    CAST({{ sl_json_num('payload_json', '$.requests') }} AS INTEGER) AS requests,
    CAST({{ sl_json_num('payload_json', '$.retries') }} AS INTEGER) AS retries,
    CAST({{ sl_json_num('payload_json', '$.rate_limit_waits') }} AS INTEGER) AS rate_limit_waits,
    CAST({{ sl_json_num('payload_json', '$.http_errors') }} AS INTEGER) AS http_errors,
    CAST({{ sl_json_num('payload_json', '$.duplicates_in_run') }} AS INTEGER) AS duplicates_in_run,
    CAST({{ sl_json_num('payload_json', '$.skipped_outside_window') }} AS INTEGER) AS skipped_outside_window,
    CAST(json_array_length(json_extract(payload_json, '$.partial_errors')) AS INTEGER) AS partial_error_count,
    CAST(json_extract(payload_json, '$.partial_errors') AS VARCHAR) AS partial_errors_json,
    CAST(replace(replace({{ sl_json_str('payload_json', '$.high_watermark') }}, 'T', ' '), '+00:00', '') AS TIMESTAMP) AS high_watermark,
    {{ sl_json_str('payload_json', '$.cursor') }} AS source_cursor
FROM summaries
