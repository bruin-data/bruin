/* @bruin
name: operations.fct_human_feedback
description: |
  Reviewer outcomes joined to the content they describe and to the assessment
  that was current when the feedback was loaded: outcome, corrected intent and
  relevance, false-positive flag and notes. Use it to measure precision and to
  review thresholds. It is read-only evidence: no asset uses it to change
  rules, scores or source data, and it is never sent anywhere.
tags: [enrich, review]
depends:
  - operations.review_feedback_input
  - enrichment.fct_mention_assessment
  - staging.stg_content_item
materialization:
  type: table
columns:
  - name: feedback_id
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: content_id
    type: varchar
    checks:
      - name: not_null
  - name: outcome
    type: varchar
    checks:
      - name: accepted_values
        value: [useful, not_useful, replied_manually, ignored, escalated]
  - name: is_false_positive
    type: boolean
@bruin */

{%- set assessment_version = sl_assessment_version(var) | trim %}

SELECT
    f.feedback_id,
    {{ sl_hash("f.source || ':' || f.external_id") }} AS content_id,
    f.source,
    f.external_id,
    s.content_id IS NOT NULL AS content_known,
    f.reviewer,
    CAST(f.reviewed_at AS TIMESTAMP) AS reviewed_at,
    f.outcome,
    f.corrected_relevance,
    f.corrected_intent,
    COALESCE(f.is_false_positive, FALSE) AS is_false_positive,
    f.notes,
    a.assessment_id,
    a.assessment_version,
    a.assessment_status,
    a.relevance AS assessed_relevance,
    a.intent AS assessed_intent,
    a.priority AS assessed_priority,
    (a.intent IS NOT NULL AND f.corrected_intent IS NOT NULL AND a.intent <> f.corrected_intent) AS intent_disagrees
FROM operations.review_feedback_input f
LEFT JOIN staging.stg_content_item s
    ON s.source = f.source AND s.external_id = f.external_id
LEFT JOIN enrichment.fct_mention_assessment a
    ON a.content_id = s.content_id AND a.assessment_version = {{ sl_str(assessment_version) }}
