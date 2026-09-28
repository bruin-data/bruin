/* @bruin
name: operations.fct_reply_draft
description: |
  Draft-only replies for human review. Empty unless reply_drafts_enabled and
  require_human_approval are both true (config.settings rejects one without
  the other). Two origins:

  * template: a starting point for each routed item whose intent is in
    reply_eligible_intents. The text opens with reply_disclosure and contains
    reviewer prompts in [brackets] that must be replaced before posting.
  * submission: drafts a person or the reply-drafter agent added to
    assets/operations/review/reply_draft_submissions.csv.

  status is needs_review until a row in reply_draft_reviews.csv approves or
  rejects it; blocked_policy when a policy check fails. There are no
  publishing fields: a person posts approved text manually on the source
  platform. This table is never read by a delivery or source adapter.
tags: [enrich, review, replies]
depends:
  - operations.fct_alert_outbox
  - operations.review_reply_draft_review
  - operations.review_reply_draft_submission
  - enrichment.fct_mention_assessment
  - enrichment.fct_term_match
  - staging.stg_content_item
materialization:
  type: table
columns:
  - name: draft_id
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: content_id
    type: varchar
    checks:
      - name: not_null
      - name: relationships
    foreign_key:
      table: staging.stg_content_item
      column: content_id
  - name: status
    type: varchar
    checks:
      - name: accepted_values
        value: [needs_review, approved, rejected, changes_requested, blocked_policy]
  - name: requires_human_approval
    type: boolean
    checks:
      - name: accepted_values
        value: [true]
custom_checks:
  - name: approved drafts have a reviewer and a review time
    query: SELECT COUNT(*) FROM operations.fct_reply_draft WHERE status = 'approved' AND (reviewer IS NULL OR reviewed_at IS NULL)
    value: 0
@bruin */

{%- set disclosure = var.reply_disclosure | replace('{brand}', var.brand_name) %}

WITH routed AS (
    SELECT o.content_id, o.alert_key, a.intent, a.priority, s.source, s.external_id, s.url, s.title
    FROM operations.fct_alert_outbox o
    JOIN enrichment.fct_mention_assessment a ON a.assessment_id = o.assessment_id
    JOIN staging.stg_content_item s ON s.content_id = o.content_id
    WHERE o.destination = {{ sl_str(var.notification_destination) }}
      AND o.routing_policy_version = {{ sl_str(var.routing_policy_version) }}
      AND a.intent IN {{ sl_in_list(var.reply_eligible_intents) }}
),

evidence AS (
    SELECT content_id, {{ sl_json_array_agg("matched_text", "matched_text") }} AS evidence_json
    FROM enrichment.fct_term_match
    WHERE is_accepted
    GROUP BY content_id
),

templated AS (
    SELECT
        r.content_id,
        r.source,
        r.external_id,
        r.url,
        CASE r.intent
            WHEN 'seeking_recommendation' THEN 'share_option_with_disclosure'
            WHEN 'comparison' THEN 'factual_comparison'
            ELSE 'answer_question'
        END AS approach,
        CASE r.intent
            WHEN 'seeking_recommendation' THEN {{ sl_str(disclosure ~ ' If it helps, ' ~ var.brand_name ~ ' is one option teams use for this. [Reviewer: say in one sentence why it fits what they described, or delete this draft if it does not.] Happy to answer questions here in the thread.') }}
            WHEN 'comparison' THEN {{ sl_str(disclosure ~ ' [Reviewer: list the differences they asked about, using only facts you can link to. Do not criticise the other product.]') }}
            ELSE {{ sl_str(disclosure ~ ' [Reviewer: answer the question directly first. Link documentation only if it answers it.]') }}
        END AS draft_body,
        e.evidence_json,
        'template' AS draft_origin,
        'pipeline' AS submitted_by,
        CAST(NULL AS TIMESTAMP) AS submitted_at
    FROM routed r
    LEFT JOIN evidence e ON e.content_id = r.content_id
),

submitted AS (
    SELECT
        {{ sl_hash("x.source || ':' || x.external_id") }} AS content_id,
        x.source,
        x.external_id,
        s.url,
        x.approach,
        x.draft_body,
        x.evidence_json,
        'submission' AS draft_origin,
        x.submitted_by,
        CAST(x.submitted_at AS TIMESTAMP) AS submitted_at
    FROM operations.review_reply_draft_submission x
    JOIN staging.stg_content_item s ON s.source = x.source AND s.external_id = x.external_id
),

drafts AS (
    SELECT * FROM templated
    UNION ALL
    SELECT * FROM submitted
),

checked AS (
    SELECT
        {{ sl_hash("d.content_id || '|' || d.approach || '|' || d.draft_origin || '|' || d.draft_body") }} AS draft_id,
        d.*,
        {{ sl_contains("lower(d.draft_body)", sl_str(disclosure | lower)) }} AS has_disclosure,
        (FALSE
        {%- for claim in var.reply_claims_to_avoid %}
         OR {{ sl_contains("lower(d.draft_body)", sl_str(claim | lower)) }}
        {%- endfor %}) AS contains_avoided_claim,
        {{ sl_regex_match("lower(d.draft_body)", "'(dm me|direct message|message me privately|email me at)'") }} AS asks_private_contact
    FROM drafts d
)

SELECT
    c.draft_id,
    c.content_id,
    c.source,
    c.external_id,
    c.url AS source_url,
    c.approach,
    c.draft_body,
    c.evidence_json,
    c.draft_origin,
    c.submitted_by,
    {{ sl_json_object([
        'has_disclosure', 'c.has_disclosure',
        'contains_avoided_claim', 'c.contains_avoided_claim',
        'asks_private_contact', 'c.asks_private_contact',
        'publishing', "'manual_only'"
    ]) }} AS policy_checks_json,
    TRUE AS requires_human_approval,
    CASE
        WHEN NOT c.has_disclosure OR c.contains_avoided_claim OR c.asks_private_contact THEN 'blocked_policy'
        ELSE COALESCE(r.decision, 'needs_review')
    END AS status,
    r.reviewer,
    CAST(r.reviewed_at AS TIMESTAMP) AS reviewed_at,
    CASE WHEN r.decision = 'approved' THEN CAST(r.reviewed_at AS TIMESTAMP) END AS approved_at,
    r.notes AS review_notes,
    COALESCE(c.submitted_at, {{ sl_now_utc() }}) AS created_at
FROM checked c
LEFT JOIN operations.review_reply_draft_review r ON r.draft_id = c.draft_id
WHERE {{ sl_bool(var.reply_drafts_enabled) }} AND {{ sl_bool(var.require_human_approval) }}
