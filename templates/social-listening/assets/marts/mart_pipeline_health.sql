/* @bruin
name: marts.mart_pipeline_health
type: duckdb.sql
materialization: {type: table}
depends: [raw.raw_reddit_content, raw.raw_hackernews_content, enrichment.fct_mention_assessment, operations.fct_alert_outbox]
columns:
  - name: source
    type: string
    primary_key: true
    checks: [{name: not_null}, {name: unique}]
@bruin */
WITH source_stats AS (
  SELECT source, MAX(collected_at) AS last_collected_at, MAX(source_cursor) AS latest_cursor, COUNT(*) AS raw_record_count
  FROM (SELECT * FROM raw.raw_reddit_content UNION ALL SELECT * FROM raw.raw_hackernews_content)
  GROUP BY source
), assessment_stats AS (
  SELECT COUNT(*) FILTER (WHERE status = 'llm_unavailable') AS unavailable_assessments,
    MAX(assessed_at) AS latest_assessment_at FROM enrichment.fct_mention_assessment
), outbox_stats AS (
  SELECT COUNT(*) FILTER (WHERE status = 'failed') AS delivery_error_count,
    COUNT(*) FILTER (WHERE status = 'delivered') AS delivered_count FROM operations.fct_alert_outbox
)
SELECT source_stats.source, source_stats.last_collected_at, source_stats.latest_cursor, source_stats.raw_record_count,
  EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - source_stats.last_collected_at)) / 60 AS source_freshness_minutes,
  assessment_stats.unavailable_assessments, outbox_stats.delivery_error_count, outbox_stats.delivered_count,
  EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - assessment_stats.latest_assessment_at)) / 60 AS classification_lag_minutes,
  0.0 AS dedupe_rate
FROM source_stats CROSS JOIN assessment_stats CROSS JOIN outbox_stats;
