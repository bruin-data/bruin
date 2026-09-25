/* @bruin
name: marts.mart_mention_queue
type: duckdb.sql
materialization: {type: table}
depends: [staging.stg_content_item, enrichment.fct_term_match, enrichment.fct_mention_assessment, operations.fct_alert_outbox]
columns:
  - name: content_id
    type: string
    primary_key: true
    checks: [{name: not_null}, {name: unique}]
  - name: source_url
    type: string
    checks: [{name: not_null}]
@bruin */
WITH latest_assessment AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY assessed_at DESC) AS row_num
  FROM enrichment.fct_mention_assessment
), latest_alert AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY created_at DESC) AS row_num
  FROM operations.fct_alert_outbox
), latest_match AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY matched_at DESC, term_id) AS row_num
  FROM enrichment.fct_term_match
)
SELECT item.content_id, item.source, item.content_type, item.source_url, item.author, item.title, item.body, item.published_at,
  match.matched_text, match.deterministic_rule_id, match.matcher_version,
  assessment.relevance_score, assessment.intent_fit_score, assessment.fit_score, assessment.engagement_score,
  assessment.freshness_score, assessment.authenticity_score, assessment.priority_score, assessment.confidence_score,
  assessment.score_components_json, assessment.reasons_json, assessment.status AS assessment_status,
  alert.alert_key, alert.destination, alert.status AS routing_status, alert.delivered_at
FROM staging.stg_content_item item
JOIN latest_match match USING (content_id)
JOIN latest_assessment assessment USING (content_id)
LEFT JOIN latest_alert alert USING (content_id)
WHERE match.row_num = 1 AND assessment.row_num = 1 AND (alert.row_num = 1 OR alert.row_num IS NULL);
