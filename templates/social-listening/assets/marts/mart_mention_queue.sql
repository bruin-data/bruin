/* @bruin
name: marts.mart_mention_queue
description: |
  The review queue: every eligible candidate under the current assessment
  version, with everything a reviewer needs to judge it without opening
  another table: source link, matched text and deterministic rule, assessment
  reasons, each score component, whether the assessment is model-generated,
  routing and delivery state, latest human feedback and reply-draft status.

  queue_state:
    routed               crossed the thresholds; see delivery_status
    below_threshold      assessed, did not cross min_relevance / min_priority / min_confidence / min_intent_score
    needs_review_model   the model failed or is pending; rule-based values shown
tags: [enrich, report, mart]
depends:
  - operations.fct_alert_outbox
  - operations.fct_human_feedback
  - operations.fct_reply_draft
  - enrichment.fct_term_match
  - staging.stg_content_item
  - enrichment.fct_mention_assessment
materialization:
  type: table
columns:
  - name: content_id
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: source_url
    type: varchar
    checks:
      - name: not_null
  - name: matched_text
    type: varchar
    checks:
      - name: not_null
  - name: rule_ids
    type: varchar
    checks:
      - name: not_null
  - name: reasons_json
    type: varchar
    checks:
      - name: not_null
  - name: priority
    type: double
    checks:
      - name: not_null
  - name: queue_state
    type: varchar
    checks:
      - name: accepted_values
        value: [routed, below_threshold, needs_review_model]
@bruin */

{%- set assessment_version = sl_assessment_version(var) | trim %}

WITH evidence AS (
    SELECT
        content_id,
        {{ sl_string_agg("CASE WHEN is_accepted THEN matched_text END", " | ") }} AS matched_text,
        {{ sl_string_agg("CASE WHEN is_accepted THEN rule_id END", ", ") }} AS rule_ids,
        {{ sl_string_agg("CASE WHEN is_accepted THEN term_id END", ", ") }} AS term_ids,
        {{ sl_string_agg("CASE WHEN is_accepted THEN category END", ", ") }} AS categories,
        {{ sl_string_agg("CASE WHEN NOT is_accepted THEN term_id || ':' || rule_id END", ", ") }} AS rejected_matches
    FROM enrichment.fct_term_match
    GROUP BY content_id
),

feedback AS (
    SELECT content_id, outcome, is_false_positive, reviewer, reviewed_at, notes
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY reviewed_at DESC, feedback_id DESC) AS rn
        FROM operations.fct_human_feedback
    ) ranked
    WHERE rn = 1
),

drafts AS (
    SELECT content_id, {{ sl_string_agg("status", ", ") }} AS draft_status, COUNT(*) AS draft_count
    FROM operations.fct_reply_draft
    GROUP BY content_id
)

SELECT
    a.content_id,
    s.source,
    s.external_id,
    s.platform,
    s.content_type,
    s.community,
    s.url AS source_url,
    s.title,
    substr(COALESCE(s.body, ''), 1, 500) AS snippet,
    s.author,
    s.published_at,
    s.collected_at,
    e.matched_text,
    e.rule_ids,
    e.term_ids,
    e.categories,
    e.rejected_matches,
    a.assessment_id,
    a.assessment_version,
    a.assessment_status,
    a.score_basis,
    a.model_generated,
    a.model_id,
    a.prompt_version,
    a.intent,
    a.relevance,
    a.intent_score,
    a.fit,
    a.engagement,
    a.freshness,
    a.authenticity,
    a.confidence,
    a.priority,
    a.det_relevance,
    a.det_intent,
    a.llm_relevance,
    a.llm_intent,
    a.llm_evidence_json,
    a.llm_error_message,
    a.reasons_json,
    a.llm_reasons_json,
    a.is_team_content,
    CASE
        WHEN o.alert_key IS NOT NULL THEN 'routed'
        WHEN a.assessment_status IN ('llm_error', 'llm_invalid_response', 'llm_pending') THEN 'needs_review_model'
        ELSE 'below_threshold'
    END AS queue_state,
    o.alert_key,
    o.destination,
    o.status AS delivery_status,
    o.attempt_count AS delivery_attempts,
    o.delivered_at,
    f.outcome AS feedback_outcome,
    f.is_false_positive AS feedback_false_positive,
    f.reviewer AS feedback_reviewer,
    f.reviewed_at AS feedback_reviewed_at,
    d.draft_status,
    COALESCE(d.draft_count, 0) AS draft_count,
    {{ sl_ts(end_datetime) }} AS data_as_of
FROM enrichment.fct_mention_assessment a
JOIN staging.stg_content_item s ON s.content_id = a.content_id
JOIN evidence e ON e.content_id = a.content_id
LEFT JOIN operations.fct_alert_outbox o
    ON o.content_id = a.content_id
   AND o.destination = {{ sl_str(var.notification_destination) }}
   AND o.routing_policy_version = {{ sl_str(var.routing_policy_version) }}
LEFT JOIN feedback f ON f.content_id = a.content_id
LEFT JOIN drafts d ON d.content_id = a.content_id
WHERE a.assessment_version = {{ sl_str(assessment_version) }}
  AND a.is_eligible
