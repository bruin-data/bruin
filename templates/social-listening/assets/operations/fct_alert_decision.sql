/* @bruin
name: operations.fct_alert_decision
description: |
  Insert-only routing log: one row per assessment that crossed the routing
  thresholds (min_relevance, min_priority, min_confidence, min_intent_score) for the configured
  destination. alert_key is a hash of content ID, destination and
  routing_policy_version, so a backfill, a late-arriving duplicate or a
  re-run can never create a second alert for the same item. Existing rows are
  never updated; delivery state lives in operations.alert_delivery_attempt and
  is summarised in operations.fct_alert_outbox.

  Routed statuses: deterministic and llm_assessed. Model failures
  (llm_error, llm_invalid_response) route only when route_on_llm_error is true;
  llm_pending never routes.
tags: [enrich, routing]
depends:
  - enrichment.fct_mention_assessment
  - staging.stg_content_item
  - enrichment.fct_term_match
materialization:
  type: table
  strategy: merge
hooks:
  pre:
    - query: CREATE SCHEMA IF NOT EXISTS operations
    - query: |
        CREATE TABLE IF NOT EXISTS operations.fct_alert_decision (
          alert_key VARCHAR PRIMARY KEY, content_id VARCHAR, assessment_id VARCHAR, destination VARCHAR,
          routing_policy_version VARCHAR, payload_version VARCHAR, priority DOUBLE, relevance DOUBLE,
          confidence DOUBLE, assessment_status VARCHAR, model_generated BOOLEAN, published_at TIMESTAMP,
          payload_json VARCHAR, routed_at TIMESTAMP
        )
    # Redacted content, and content deleted from raw (retention), leaves this table too.
    # Switching demo_mode does not delete anything: raw keeps both modes.
    - query: DELETE FROM operations.fct_alert_decision WHERE content_id IN (SELECT md5(source || ':' || external_id) FROM config.redaction_request) OR content_id NOT IN (SELECT md5(source || ':' || external_id) FROM raw.raw_reddit_content UNION SELECT md5(source || ':' || external_id) FROM raw.raw_hackernews_content UNION SELECT md5(source || ':' || external_id) FROM raw.raw_github_content UNION SELECT md5(source || ':' || external_id) FROM raw.raw_stackoverflow_content UNION SELECT md5(source || ':' || external_id) FROM raw.raw_authorised_export_content)
columns:
  - name: alert_key
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: content_id
    type: varchar
    checks:
      - name: not_null
  - name: assessment_id
    type: varchar
    checks:
      - name: not_null
  - name: destination
    type: varchar
    checks:
      - name: accepted_values
        value: [none, webhook, slack_webhook]
  - name: routing_policy_version
    type: varchar
  - name: payload_version
    type: varchar
  - name: priority
    type: double
  - name: relevance
    type: double
  - name: confidence
    type: double
  - name: assessment_status
    type: varchar
  - name: model_generated
    type: boolean
  - name: published_at
    type: timestamp
  - name: payload_json
    type: varchar
    checks:
      - name: not_null
  - name: routed_at
    type: timestamp
    checks:
      - name: not_null
custom_checks:
  - name: at most one alert per content item and destination
    query: |
      SELECT COUNT(*) FROM (
        SELECT content_id, destination, routing_policy_version FROM operations.fct_alert_decision
        GROUP BY 1, 2, 3 HAVING COUNT(*) > 1
      )
    value: 0
@bruin */

{%- set assessment_version = sl_assessment_version(var) | trim %}

WITH routable AS (
    SELECT a.*, s.source, s.content_type, s.url, s.title, s.body, s.published_at, s.community
    FROM enrichment.fct_mention_assessment a
    JOIN staging.stg_content_item s ON s.content_id = a.content_id
    WHERE a.assessment_version = {{ sl_str(assessment_version) }}
      AND a.is_eligible
      AND (
            a.assessment_status IN ('deterministic', 'llm_assessed')
         OR ({{ sl_bool(var.route_on_llm_error) }} AND a.assessment_status IN ('llm_error', 'llm_invalid_response'))
      )
      AND a.relevance >= {{ var.min_relevance }}
      AND a.priority >= {{ var.min_priority }}
      AND a.confidence >= {{ var.min_confidence }}
      AND a.intent_score >= {{ var.min_intent_score }}
),

evidence AS (
    SELECT
        content_id,
        {{ sl_string_agg("CASE WHEN is_accepted THEN matched_text END", ", ") }} AS matched_text,
        {{ sl_string_agg("CASE WHEN is_accepted THEN rule_id END", ", ") }} AS rule_ids,
        {{ sl_string_agg("CASE WHEN is_accepted THEN category END", ", ") }} AS categories
    FROM enrichment.fct_term_match
    GROUP BY content_id
),

keyed AS (
    SELECT
        {{ sl_hash("r.content_id || '|' || " ~ sl_str(var.notification_destination) ~ " || '|' || " ~ sl_str(var.routing_policy_version)) }} AS alert_key,
        r.*,
        e.matched_text,
        e.rule_ids,
        e.categories
    FROM routable r
    LEFT JOIN evidence e ON e.content_id = r.content_id
)

SELECT
    alert_key,
    content_id,
    assessment_id,
    {{ sl_str(var.notification_destination) }} AS destination,
    {{ sl_str(var.routing_policy_version) }} AS routing_policy_version,
    {{ sl_str(var.payload_version) }} AS payload_version,
    priority,
    relevance,
    confidence,
    assessment_status,
    model_generated,
    published_at,
    {{ sl_json_object([
        'alert_key', 'alert_key',
        'payload_version', sl_str(var.payload_version),
        'content_id', 'content_id',
        'source', 'source',
        'content_type', 'content_type',
        'community', 'community',
        'url', 'url',
        'title', 'title',
        'snippet', 'substr(COALESCE(body, \'\'), 1, 280)',
        'matched_text', 'matched_text',
        'rule_ids', 'rule_ids',
        'categories', 'categories',
        'intent', 'intent',
        'relevance', 'relevance',
        'priority', 'priority',
        'confidence', 'confidence',
        'components', sl_json_object_nested(['relevance', 'relevance', 'intent', 'intent_score', 'fit', 'fit', 'engagement', 'engagement', 'freshness', 'freshness', 'authenticity', 'authenticity']),
        'assessment_status', 'assessment_status',
        'model_generated', 'model_generated',
        'data_warnings', "CASE WHEN model_generated THEN 'model-generated assessment' WHEN assessment_status <> 'deterministic' THEN 'model unavailable; rule-based score' END",
        'published_at', sl_iso_utc('published_at')
    ]) }} AS payload_json,
    {{ sl_now_utc() }} AS routed_at
FROM keyed
