/* @bruin
name: operations.fct_alert_outbox
type: duckdb.sql
materialization: {type: table}
depends: [enrichment.fct_mention_assessment]
columns:
  - name: alert_key
    type: string
    primary_key: true
    checks: [{name: not_null}, {name: unique}]
  - name: status
    type: string
    checks: [{name: accepted_values, value: [pending, dry_run, delivered, failed]}]
@bruin */

SELECT sha256(assessment.content_id || ':{{ var.notification_destination }}:v1') AS alert_key,
  assessment.content_id, '{{ var.notification_destination }}' AS destination, 'v1' AS payload_version,
  json_object('content_id', assessment.content_id, 'priority', assessment.priority_score, 'assessment_status', assessment.status) AS payload_json,
  CASE WHEN '{{ var.notification_destination }}' = 'none' THEN 'pending'
       WHEN {{ var.notification_dry_run }} THEN 'dry_run' ELSE 'pending' END AS status,
  0 AS retry_count, CAST(NULL AS TIMESTAMP) AS delivered_at, CURRENT_TIMESTAMP AS created_at, CURRENT_TIMESTAMP AS updated_at
FROM enrichment.fct_mention_assessment assessment
WHERE assessment.priority_score >= {{ var.min_priority }} AND assessment.relevance_score >= {{ var.min_relevance }};
