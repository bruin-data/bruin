/* @bruin
name: marts.mart_pipeline_health
description: |
  One row per component (each source, the model step and delivery) with the
  signals an operator needs: last successful run, cursor and high watermark,
  24-hour volume against the 7-day daily average, error counts, classification
  and delivery lag, and de-duplication counts. health_status is `ok` unless one
  of the reasons in health_reasons applies:

    stale            no successful collection within stale_after_minutes
    abnormal_volume  24-hour volume above volume_anomaly_ratio x the 7-day daily
                     average, or zero when that average is at least 5
    errors           failed runs, HTTP errors or partial errors in 24 hours
    model_errors     model calls that failed or returned invalid output in 24 hours
    delivery_failed  alerts in failed_permanent or retrying
    lag              median classification lag above stale_after_minutes

  Times are measured against the run's interval end (data_as_of).
tags: [enrich, report, health, mart]
depends:
  - operations.fct_source_run
  - operations.fct_alert_outbox
  - enrichment.llm_assessment_result
  - staging.stg_content_item
  - enrichment.fct_mention_assessment
materialization:
  type: table
columns:
  - name: component
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: health_status
    type: varchar
    checks:
      - name: accepted_values
        value: [ok, attention, disabled]
@bruin */

{%- set as_of = sl_ts(end_datetime) %}

WITH runs AS (
    SELECT * FROM operations.fct_source_run
),

latest_run AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY source ORDER BY collected_at DESC, run_key) AS rn FROM runs
    ) ranked
    WHERE rn = 1
),

run_stats AS (
    SELECT
        source,
        MAX(CASE WHEN run_status IN ('ok', 'partial') THEN collected_at END) AS last_success_at,
        MAX(high_watermark) AS high_watermark,
        SUM(CASE WHEN collected_at > {{ as_of }} - INTERVAL 24 HOUR AND run_status = 'failed' THEN 1 ELSE 0 END) AS failed_runs_24h,
        SUM(CASE WHEN collected_at > {{ as_of }} - INTERVAL 24 HOUR THEN COALESCE(http_errors, 0) + COALESCE(partial_error_count, 0) ELSE 0 END) AS errors_24h,
        SUM(CASE WHEN collected_at > {{ as_of }} - INTERVAL 24 HOUR THEN COALESCE(rate_limit_waits, 0) ELSE 0 END) AS rate_limit_waits_24h,
        SUM(CASE WHEN collected_at > {{ as_of }} - INTERVAL 24 HOUR THEN COALESCE(duplicates_in_run, 0) ELSE 0 END) AS in_run_duplicates_24h
    FROM runs
    GROUP BY source
),

content AS (
    SELECT
        source,
        SUM(CASE WHEN published_at > {{ as_of }} - INTERVAL 24 HOUR THEN 1 ELSE 0 END) AS items_24h,
        SUM(CASE WHEN published_at > {{ as_of }} - INTERVAL 7 DAY THEN 1 ELSE 0 END) / 7.0 AS avg_daily_items_7d,
        COUNT(*) AS items_total,
        SUM(version_count) AS raw_versions_total,
        SUM(CASE WHEN is_syndicated_duplicate THEN 1 ELSE 0 END) AS syndicated_duplicates
    FROM staging.stg_content_item
    GROUP BY source
),

classification AS (
    SELECT
        s.source,
        {{ sl_median(sl_minutes_between("s.first_collected_at", "a.first_assessed_at")) }} AS median_classification_lag_minutes
    FROM enrichment.fct_mention_assessment a
    JOIN staging.stg_content_item s ON s.content_id = a.content_id
    GROUP BY s.source
),

source_rows AS (
    SELECT
        'source:' || l.source AS component,
        'source' AS component_type,
        l.run_status AS last_run_status,
        l.collection_mode,
        l.collected_at AS last_run_at,
        r.last_success_at,
        l.source_cursor,
        r.high_watermark,
        {{ sl_hours_between("r.high_watermark", as_of) }} AS watermark_lag_hours,
        CAST(COALESCE(c.items_24h, 0) AS INTEGER) AS volume_24h,
        ROUND(COALESCE(c.avg_daily_items_7d, 0), 2) AS avg_daily_volume_7d,
        CAST(r.failed_runs_24h + r.errors_24h AS INTEGER) AS error_count_24h,
        CAST(r.rate_limit_waits_24h AS INTEGER) AS rate_limit_waits_24h,
        ROUND(cl.median_classification_lag_minutes, 1) AS median_classification_lag_minutes,
        CAST(NULL AS DOUBLE) AS median_delivery_lag_minutes,
        CAST(COALESCE(c.raw_versions_total, 0) - COALESCE(c.items_total, 0) AS INTEGER) AS superseded_versions,
        CAST(COALESCE(c.syndicated_duplicates, 0) + r.in_run_duplicates_24h AS INTEGER) AS duplicates_removed,
        CAST(NULL AS INTEGER) AS pending_alerts,
        CAST(NULL AS INTEGER) AS failed_alerts,
        CASE WHEN l.run_status = 'disabled' THEN 'disabled' END AS forced_status,
        {{ sl_json_array_of([
            "CASE WHEN l.run_status <> 'disabled' AND (r.last_success_at IS NULL OR " ~ sl_minutes_between("r.last_success_at", as_of) ~ " > " ~ var.stale_after_minutes ~ ") THEN 'stale' END",
            "CASE WHEN l.run_status <> 'disabled' AND ((COALESCE(c.avg_daily_items_7d, 0) > 0 AND COALESCE(c.items_24h, 0) > " ~ var.volume_anomaly_ratio ~ " * c.avg_daily_items_7d AND COALESCE(c.items_24h, 0) >= 10) OR (COALESCE(c.avg_daily_items_7d, 0) >= 5 AND COALESCE(c.items_24h, 0) = 0)) THEN 'abnormal_volume' END",
            "CASE WHEN r.failed_runs_24h + r.errors_24h > 0 THEN 'errors' END",
            "CASE WHEN cl.median_classification_lag_minutes > " ~ var.stale_after_minutes ~ " THEN 'lag' END"
        ]) }} AS health_reasons_json
    FROM latest_run l
    JOIN run_stats r ON r.source = l.source
    LEFT JOIN content c ON c.source = l.source
    LEFT JOIN classification cl ON cl.source = l.source
),

llm_runs AS (
    SELECT
        MAX(assessed_at) AS last_run_at,
        SUM(CASE WHEN record_kind = 'result' AND status IN ('error', 'invalid_response') AND assessed_at > {{ sl_now_utc() }} - INTERVAL 24 HOUR THEN 1 ELSE 0 END) AS model_errors_24h,
        SUM(CASE WHEN record_kind = 'result' AND status = 'ok' THEN 1 ELSE 0 END) AS ok_results,
        MAX(CASE WHEN record_kind = 'run_summary' THEN status END) AS any_status
    FROM enrichment.llm_assessment_result
),

model_row AS (
    SELECT
        'model' AS component,
        'model' AS component_type,
        CASE WHEN {{ sl_bool(var.llm_enabled) }} THEN 'enabled' ELSE 'disabled' END AS last_run_status,
        CAST(NULL AS VARCHAR) AS collection_mode,
        last_run_at,
        CAST(NULL AS TIMESTAMP) AS last_success_at,
        CAST(NULL AS VARCHAR) AS source_cursor,
        CAST(NULL AS TIMESTAMP) AS high_watermark,
        CAST(NULL AS DOUBLE) AS watermark_lag_hours,
        CAST(ok_results AS INTEGER) AS volume_24h,
        CAST(NULL AS DOUBLE) AS avg_daily_volume_7d,
        CAST(model_errors_24h AS INTEGER) AS error_count_24h,
        CAST(NULL AS INTEGER) AS rate_limit_waits_24h,
        CAST(NULL AS DOUBLE) AS median_classification_lag_minutes,
        CAST(NULL AS DOUBLE) AS median_delivery_lag_minutes,
        CAST(NULL AS INTEGER) AS superseded_versions,
        CAST(NULL AS INTEGER) AS duplicates_removed,
        CAST(NULL AS INTEGER) AS pending_alerts,
        CAST(NULL AS INTEGER) AS failed_alerts,
        CASE WHEN NOT {{ sl_bool(var.llm_enabled) }} THEN 'disabled' END AS forced_status,
        {{ sl_json_array_of(["CASE WHEN model_errors_24h > 0 THEN 'model_errors' END"]) }} AS health_reasons_json
    FROM llm_runs
),

delivery AS (
    SELECT
        MAX(last_attempt_at) AS last_run_at,
        MAX(delivered_at) AS last_success_at,
        SUM(CASE WHEN status IN ('pending', 'retrying') THEN 1 ELSE 0 END) AS pending_alerts,
        SUM(CASE WHEN status IN ('failed_permanent', 'retrying') THEN 1 ELSE 0 END) AS failed_alerts,
        SUM(CASE WHEN delivered_at > {{ sl_now_utc() }} - INTERVAL 24 HOUR THEN 1 ELSE 0 END) AS delivered_24h,
        {{ sl_median("delivery_lag_minutes") }} AS median_delivery_lag_minutes,
        COUNT(*) AS alerts_total
    FROM operations.fct_alert_outbox
),

delivery_row AS (
    SELECT
        'delivery' AS component,
        'delivery' AS component_type,
        CASE
            WHEN {{ sl_str(var.notification_destination) }} = 'none' THEN 'disabled'
            WHEN {{ sl_bool(var.notification_dry_run) }} THEN 'dry_run'
            ELSE 'live'
        END AS last_run_status,
        CAST(NULL AS VARCHAR) AS collection_mode,
        last_run_at,
        last_success_at,
        CAST(NULL AS VARCHAR) AS source_cursor,
        CAST(NULL AS TIMESTAMP) AS high_watermark,
        CAST(NULL AS DOUBLE) AS watermark_lag_hours,
        CAST(delivered_24h AS INTEGER) AS volume_24h,
        CAST(NULL AS DOUBLE) AS avg_daily_volume_7d,
        CAST(failed_alerts AS INTEGER) AS error_count_24h,
        CAST(NULL AS INTEGER) AS rate_limit_waits_24h,
        CAST(NULL AS DOUBLE) AS median_classification_lag_minutes,
        ROUND(median_delivery_lag_minutes, 1) AS median_delivery_lag_minutes,
        CAST(NULL AS INTEGER) AS superseded_versions,
        CAST(NULL AS INTEGER) AS duplicates_removed,
        CAST(pending_alerts AS INTEGER) AS pending_alerts,
        CAST(failed_alerts AS INTEGER) AS failed_alerts,
        CASE WHEN {{ sl_str(var.notification_destination) }} = 'none' THEN 'disabled' END AS forced_status,
        {{ sl_json_array_of(["CASE WHEN failed_alerts > 0 THEN 'delivery_failed' END"]) }} AS health_reasons_json
    FROM delivery
),

all_rows AS (
    SELECT * FROM source_rows
    UNION ALL
    SELECT * FROM model_row
    UNION ALL
    SELECT * FROM delivery_row
)

SELECT
    component,
    component_type,
    COALESCE(forced_status, CASE WHEN health_reasons_json = '[]' THEN 'ok' ELSE 'attention' END) AS health_status,
    health_reasons_json,
    last_run_status,
    collection_mode,
    last_run_at,
    last_success_at,
    source_cursor,
    high_watermark,
    ROUND(watermark_lag_hours, 2) AS watermark_lag_hours,
    volume_24h,
    avg_daily_volume_7d,
    error_count_24h,
    rate_limit_waits_24h,
    median_classification_lag_minutes,
    median_delivery_lag_minutes,
    superseded_versions,
    duplicates_removed,
    pending_alerts,
    failed_alerts,
    {{ as_of }} AS data_as_of
FROM all_rows
