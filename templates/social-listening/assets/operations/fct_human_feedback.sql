/* @bruin
name: operations.fct_human_feedback
type: duckdb.sql
materialization: {type: table}
depends: [enrichment.fct_mention_assessment]
columns:
  - name: feedback_id
    type: string
    primary_key: true
    checks: [{name: not_null}, {name: unique}]
@bruin */
SELECT CAST(NULL AS VARCHAR) AS feedback_id, CAST(NULL AS VARCHAR) AS content_id, CAST(NULL AS VARCHAR) AS reviewer,
  CAST(NULL AS VARCHAR) AS outcome, CAST(NULL AS VARCHAR) AS corrected_classification, CAST(NULL AS BOOLEAN) AS is_false_positive,
  CAST(NULL AS VARCHAR) AS notes, CAST(NULL AS TIMESTAMP) AS submitted_at
WHERE FALSE;
