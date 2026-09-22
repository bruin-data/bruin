/* @bruin
name: operations.fct_reply_draft
type: duckdb.sql
materialization: {type: table}
depends: [enrichment.fct_mention_assessment]
columns:
  - name: draft_id
    type: string
    primary_key: true
    checks: [{name: not_null}, {name: unique}]
  - name: status
    type: string
    checks: [{name: accepted_values, value: [needs_review, rejected, approved]}]
@bruin */

SELECT sha256(content_id || ':reply-draft-v1') AS draft_id, content_id,
  'Acknowledge the question, provide factual help, and avoid unsupported claims.' AS approach,
  'Draft only: a human must review and post manually.' AS body,
  reasons_json AS evidence_json, CAST(NULL AS VARCHAR) AS reviewer,
  'needs_review' AS status, CAST(NULL AS TIMESTAMP) AS approved_at, CURRENT_TIMESTAMP AS created_at
FROM enrichment.fct_mention_assessment
WHERE {{ var.reply_drafts_enabled }} AND {{ var.require_human_approval }};
