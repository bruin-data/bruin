/* @bruin
name: enrichment.fct_mention_assessment
description: |
  Versioned, explainable assessment of every candidate. Each of the six
  priority components is stored separately, together with the deterministic
  value, the model value (when a model ran) and the value actually used:

    priority = w.relevance * relevance + w.intent * intent_score + w.fit * fit
             + w.engagement * engagement + w.freshness * freshness
             + w.authenticity * authenticity        (weights: priority_weights)

  assessment_status:
    deterministic         rules only (llm_enabled = false)
    llm_assessed          model result validated and used for relevance, intent, fit, confidence
    llm_error             model call failed; rule-based values kept, model columns NULL
    llm_invalid_response  model answer failed validation; same handling as llm_error
    llm_pending           model enabled but not attempted yet (per-run budget)
    excluded              an exclusion rule applied; never sent to a model

  assessment_version combines scoring_version, matcher_version and the model
  and prompt versions, so changing any of them writes a new version row while
  earlier versions stay queryable. Re-running the same version updates it in
  place and keeps first_assessed_at.
tags: [enrich]
depends:
  - enrichment.fct_mention_candidate
  - enrichment.llm_assessment_result
  - config.dim_intent_rule
  - enrichment.fct_term_match
  - staging.stg_content_item
materialization:
  type: table
  strategy: merge
hooks:
  pre:
    - query: CREATE SCHEMA IF NOT EXISTS enrichment
    - query: |
        CREATE TABLE IF NOT EXISTS enrichment.fct_mention_assessment (
          assessment_id VARCHAR PRIMARY KEY, content_id VARCHAR, assessment_version VARCHAR,
          scoring_version VARCHAR, matcher_version VARCHAR, model_id VARCHAR, prompt_version VARCHAR,
          assessment_status VARCHAR, is_eligible BOOLEAN, exclusion_reason VARCHAR, is_team_content BOOLEAN,
          model_generated BOOLEAN, score_basis VARCHAR,
          det_relevance DOUBLE, det_intent VARCHAR, det_intent_keyword VARCHAR, det_fit DOUBLE, det_confidence DOUBLE,
          llm_relevance DOUBLE, llm_intent VARCHAR, llm_fit DOUBLE, llm_confidence DOUBLE,
          llm_reasons_json VARCHAR, llm_evidence_json VARCHAR, llm_error_message VARCHAR,
          relevance DOUBLE, intent VARCHAR, intent_score DOUBLE, fit DOUBLE, engagement DOUBLE,
          freshness DOUBLE, authenticity DOUBLE, confidence DOUBLE, priority DOUBLE,
          reasons_json VARCHAR, first_assessed_at TIMESTAMP, assessed_at TIMESTAMP
        )
    # Content that left staging (redaction, retention clean-up) leaves the history too.
    - query: DELETE FROM enrichment.fct_mention_assessment WHERE content_id NOT IN (SELECT content_id FROM staging.stg_content_item)
columns:
  - name: assessment_id
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: content_id
    type: varchar
    update_on_merge: false
    checks:
      - name: not_null
  - name: assessment_version
    type: varchar
    update_on_merge: false
    checks:
      - name: not_null
  - name: scoring_version
    type: varchar
  - name: matcher_version
    type: varchar
  - name: model_id
    type: varchar
    update_on_merge: true
  - name: prompt_version
    type: varchar
  - name: assessment_status
    type: varchar
    update_on_merge: true
    checks:
      - name: accepted_values
        value: [deterministic, llm_assessed, llm_error, llm_invalid_response, llm_pending, excluded]
  - name: is_eligible
    type: boolean
    update_on_merge: true
  - name: exclusion_reason
    type: varchar
    update_on_merge: true
  - name: is_team_content
    type: boolean
    update_on_merge: true
  - name: model_generated
    type: boolean
    update_on_merge: true
    checks:
      - name: not_null
  - name: score_basis
    type: varchar
    update_on_merge: true
    checks:
      - name: accepted_values
        value: [rules, rules+model, rules_fallback_after_model_failure]
  - name: det_relevance
    type: double
    update_on_merge: true
  - name: det_intent
    type: varchar
    update_on_merge: true
  - name: det_intent_keyword
    type: varchar
    update_on_merge: true
  - name: det_fit
    type: double
    update_on_merge: true
  - name: det_confidence
    type: double
    update_on_merge: true
  - name: llm_relevance
    type: double
    update_on_merge: true
  - name: llm_intent
    type: varchar
    update_on_merge: true
  - name: llm_fit
    type: double
    update_on_merge: true
  - name: llm_confidence
    type: double
    update_on_merge: true
  - name: llm_reasons_json
    type: varchar
    update_on_merge: true
  - name: llm_evidence_json
    type: varchar
    update_on_merge: true
  - name: llm_error_message
    type: varchar
    update_on_merge: true
  - name: relevance
    type: double
    update_on_merge: true
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: intent
    type: varchar
    update_on_merge: true
  - name: intent_score
    type: double
    update_on_merge: true
  - name: fit
    type: double
    update_on_merge: true
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: engagement
    type: double
    update_on_merge: true
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: freshness
    type: double
    update_on_merge: true
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: authenticity
    type: double
    update_on_merge: true
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: confidence
    type: double
    update_on_merge: true
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: priority
    type: double
    update_on_merge: true
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: reasons_json
    type: varchar
    update_on_merge: true
    checks:
      - name: not_null
  - name: first_assessed_at
    type: timestamp
    update_on_merge: false
  - name: assessed_at
    type: timestamp
    update_on_merge: true
custom_checks:
  - name: model columns are empty unless a model result was used
    query: |
      SELECT COUNT(*) FROM enrichment.fct_mention_assessment
      WHERE assessment_status <> 'llm_assessed'
        AND (llm_relevance IS NOT NULL OR llm_intent IS NOT NULL OR llm_fit IS NOT NULL OR llm_confidence IS NOT NULL)
    value: 0
  - name: priority equals the weighted sum of its stored components
    query: |
      SELECT COUNT(*) FROM enrichment.fct_mention_assessment
      WHERE ABS(priority - ROUND(
            {{ var.priority_weights.relevance }} * relevance + {{ var.priority_weights.intent }} * intent_score
          + {{ var.priority_weights.fit }} * fit + {{ var.priority_weights.engagement }} * engagement
          + {{ var.priority_weights.freshness }} * freshness + {{ var.priority_weights.authenticity }} * authenticity, 4)) > 0.0001
    value: 0
@bruin */

{%- set llm_on = var.llm_enabled %}
{%- set model_id = sl_model_id(var) | trim %}
{%- set assessment_version = sl_assessment_version(var) | trim %}

WITH candidate AS (
    SELECT
        c.*,
        lower(COALESCE(c.title, '') || ' ' || COALESCE(c.body, '')) AS text_lower,
        length(COALESCE(c.title, '') || ' ' || COALESCE(c.body, '')) AS text_length
    FROM enrichment.fct_mention_candidate c
),

best_match AS (
    SELECT content_id, category, term_id, matched_text, rule_id, category_relevance
    FROM (
        SELECT
            m.*,
            CASE m.category
                WHEN 'brand' THEN {{ var.category_relevance.brand }}
                WHEN 'competitor' THEN {{ var.category_relevance.competitor }}
                ELSE {{ var.category_relevance.topic }}
            END AS category_relevance,
            ROW_NUMBER() OVER (
                PARTITION BY m.content_id
                ORDER BY CASE m.category WHEN 'brand' THEN {{ var.category_relevance.brand }} WHEN 'competitor' THEN {{ var.category_relevance.competitor }} ELSE {{ var.category_relevance.topic }} END DESC, m.term_id
            ) AS rn
        FROM enrichment.fct_term_match m
        WHERE m.is_accepted
    ) ranked
    WHERE rn = 1
),

context_bonus AS (
    SELECT content_id, MAX(CASE WHEN rule_id = 'phrase_boundary+context.v1' THEN 1 ELSE 0 END) AS has_context_rule
    FROM enrichment.fct_term_match
    WHERE is_accepted
    GROUP BY content_id
),

intent_hit AS (
    SELECT content_id, intent, keyword_lower, intent_score
    FROM (
        SELECT
            c.content_id,
            r.intent,
            r.keyword_lower,
            r.intent_score,
            ROW_NUMBER() OVER (PARTITION BY c.content_id ORDER BY r.intent_score DESC, r.taxonomy_rank, r.keyword_lower) AS rn
        FROM candidate c
        JOIN config.dim_intent_rule r ON {{ sl_contains("c.text_lower", "r.keyword_lower") }}
    ) ranked
    WHERE rn = 1
),

fit_hit AS (
    SELECT
        c.content_id,
        (0
        {%- for kw in var.fit_keywords %}
         + CASE WHEN {{ sl_contains("c.text_lower", sl_str(kw | lower)) }} THEN 1 ELSE 0 END
        {%- endfor %}) AS fit_keyword_hits,
        (FALSE
        {%- for kw in var.disqualifying_keywords %}
         OR {{ sl_contains("c.text_lower", sl_str(kw | lower)) }}
        {%- endfor %}) AS is_disqualified
    FROM candidate c
),

intent_scores AS (
    SELECT intent, MAX(intent_score) AS intent_score FROM config.dim_intent_rule GROUP BY intent
),

llm AS (
    SELECT *
    FROM enrichment.llm_assessment_result
    WHERE record_kind = 'result'
      AND provider = {{ sl_str(var.llm_provider) }}
      AND model_id = {{ sl_str(model_id) }}
      AND prompt_version = {{ sl_str(var.llm_prompt_version) }}
      AND {{ sl_bool(llm_on) }}
),

deterministic AS (
    SELECT
        c.content_id,
        c.is_eligible,
        c.exclusion_reason,
        c.is_team_content,
        c.published_at,
        c.age_hours,
        c.accepted_terms,
        b.category,
        b.matched_text,
        b.rule_id,
        LEAST(1.0, COALESCE(b.category_relevance, 0)
            + CASE WHEN c.accepted_terms >= 2 THEN 0.05 ELSE 0 END
            + CASE WHEN cb.has_context_rule = 1 THEN 0.05 ELSE 0 END) AS det_relevance,
        COALESCE(i.intent, 'general_discussion') AS det_intent,
        i.keyword_lower AS det_intent_keyword,
        COALESCE(i.intent_score, 0.2) AS det_intent_score,
        CASE WHEN f.is_disqualified THEN 0.0 ELSE LEAST(1.0, 0.5 + 0.15 * f.fit_keyword_hits) END AS det_fit,
        f.fit_keyword_hits,
        f.is_disqualified,
        LEAST(1.0, ln(1 + s.engagement_raw) / ln(1 + {{ var.engagement_saturation }})) AS engagement,
        s.engagement_raw,
        exp(-ln(2) * GREATEST(c.age_hours, 0) / {{ var.freshness_half_life_hours }}) AS freshness,
        GREATEST(0.0, 1.0
            - CASE WHEN c.author IS NULL OR c.author = '' THEN 0.2 ELSE 0 END
            - CASE WHEN c.text_length < 40 THEN 0.3 ELSE 0 END) AS authenticity,
        LEAST(0.95, 0.5
            + CASE WHEN i.intent IS NOT NULL THEN 0.15 ELSE 0 END
            + CASE WHEN c.accepted_terms >= 2 THEN 0.1 ELSE 0 END
            + CASE WHEN cb.has_context_rule = 1 THEN 0.1 ELSE 0 END
            + CASE WHEN c.text_length >= 200 THEN 0.1 ELSE 0 END) AS det_confidence
    FROM candidate c
    JOIN staging.stg_content_item s ON s.content_id = c.content_id
    LEFT JOIN best_match b ON b.content_id = c.content_id
    LEFT JOIN context_bonus cb ON cb.content_id = c.content_id
    LEFT JOIN intent_hit i ON i.content_id = c.content_id
    LEFT JOIN fit_hit f ON f.content_id = c.content_id
),

combined AS (
    SELECT
        d.*,
        l.status AS llm_status,
        l.llm_relevance,
        l.llm_intent,
        l.llm_fit,
        l.llm_confidence,
        l.reasons_json AS llm_reasons_json,
        l.evidence_json AS llm_evidence_json,
        l.error_message AS llm_error_message,
        CASE
            WHEN NOT d.is_eligible THEN 'excluded'
            WHEN NOT {{ sl_bool(llm_on) }} THEN 'deterministic'
            WHEN l.status = 'ok' THEN 'llm_assessed'
            WHEN l.status = 'error' THEN 'llm_error'
            WHEN l.status = 'invalid_response' THEN 'llm_invalid_response'
            ELSE 'llm_pending'
        END AS assessment_status
    FROM deterministic d
    LEFT JOIN llm l ON l.content_id = d.content_id
),

effective AS (
    SELECT
        c.*,
        (c.assessment_status = 'llm_assessed') AS use_model,
        CASE WHEN c.assessment_status = 'llm_assessed' THEN c.llm_relevance ELSE c.det_relevance END AS relevance,
        CASE WHEN c.assessment_status = 'llm_assessed' THEN c.llm_intent ELSE c.det_intent END AS intent,
        CASE WHEN c.assessment_status = 'llm_assessed' THEN COALESCE(isc.intent_score, 0.2) ELSE c.det_intent_score END AS intent_score,
        CASE WHEN c.assessment_status = 'llm_assessed' THEN c.llm_fit ELSE c.det_fit END AS fit,
        CASE WHEN c.assessment_status = 'llm_assessed' THEN c.llm_confidence ELSE c.det_confidence END AS confidence
    FROM combined c
    LEFT JOIN intent_scores isc ON isc.intent = c.llm_intent
)

SELECT
    {{ sl_hash("e.content_id || '|' || " ~ sl_str(assessment_version)) }} AS assessment_id,
    e.content_id,
    {{ sl_str(assessment_version) }} AS assessment_version,
    {{ sl_str(var.scoring_version) }} AS scoring_version,
    {{ sl_str(var.matcher_version) }} AS matcher_version,
    CASE WHEN {{ sl_bool(llm_on) }} THEN {{ sl_str(model_id) }} END AS model_id,
    CASE WHEN {{ sl_bool(llm_on) }} THEN {{ sl_str(var.llm_prompt_version) }} END AS prompt_version,
    e.assessment_status,
    e.is_eligible,
    e.exclusion_reason,
    e.is_team_content,
    e.use_model AS model_generated,
    CASE
        WHEN e.use_model THEN 'rules+model'
        WHEN e.assessment_status IN ('llm_error', 'llm_invalid_response') THEN 'rules_fallback_after_model_failure'
        ELSE 'rules'
    END AS score_basis,
    ROUND(e.det_relevance, 4) AS det_relevance,
    e.det_intent,
    e.det_intent_keyword,
    ROUND(e.det_fit, 4) AS det_fit,
    ROUND(e.det_confidence, 4) AS det_confidence,
    CASE WHEN e.use_model THEN e.llm_relevance END AS llm_relevance,
    CASE WHEN e.use_model THEN e.llm_intent END AS llm_intent,
    CASE WHEN e.use_model THEN e.llm_fit END AS llm_fit,
    CASE WHEN e.use_model THEN e.llm_confidence END AS llm_confidence,
    e.llm_reasons_json,
    e.llm_evidence_json,
    e.llm_error_message,
    ROUND(e.relevance, 4) AS relevance,
    e.intent,
    ROUND(e.intent_score, 4) AS intent_score,
    ROUND(e.fit, 4) AS fit,
    ROUND(e.engagement, 4) AS engagement,
    ROUND(e.freshness, 4) AS freshness,
    ROUND(e.authenticity, 4) AS authenticity,
    ROUND(e.confidence, 4) AS confidence,
    ROUND(
          {{ var.priority_weights.relevance }} * ROUND(e.relevance, 4)
        + {{ var.priority_weights.intent }} * ROUND(e.intent_score, 4)
        + {{ var.priority_weights.fit }} * ROUND(e.fit, 4)
        + {{ var.priority_weights.engagement }} * ROUND(e.engagement, 4)
        + {{ var.priority_weights.freshness }} * ROUND(e.freshness, 4)
        + {{ var.priority_weights.authenticity }} * ROUND(e.authenticity, 4), 4) AS priority,
    {{ sl_json_array_of([
        "'matched ' || COALESCE(e.category, '?') || ' term \"' || COALESCE(e.matched_text, '?') || '\" (' || COALESCE(e.rule_id, '?') || ')'",
        "CASE WHEN e.accepted_terms >= 2 THEN CAST(e.accepted_terms AS VARCHAR) || ' tracked terms matched' END",
        "CASE WHEN e.det_intent_keyword IS NOT NULL THEN 'rule intent ' || e.det_intent || ' from \"' || e.det_intent_keyword || '\"' ELSE 'no intent keyword; general_discussion' END",
        "CASE WHEN e.is_disqualified THEN 'disqualifying keyword present; fit 0' WHEN e.fit_keyword_hits > 0 THEN CAST(e.fit_keyword_hits AS VARCHAR) || ' fit keyword(s)' END",
        "'engagement signal ' || CAST(CAST(e.engagement_raw AS INTEGER) AS VARCHAR)",
        "'age ' || CAST(ROUND(e.age_hours, 1) AS VARCHAR) || 'h'",
        "CASE WHEN e.assessment_status = 'llm_assessed' THEN 'model-generated relevance, intent, fit and confidence (' || " ~ sl_str(model_id) ~ " || ', prompt ' || " ~ sl_str(var.llm_prompt_version) ~ " || ')' END",
        "CASE WHEN e.assessment_status IN ('llm_error', 'llm_invalid_response') THEN 'model unavailable (' || e.assessment_status || '); rule-based values shown, not routed unless route_on_llm_error' END",
        "CASE WHEN e.assessment_status = 'llm_pending' THEN 'model assessment pending; not routed yet' END",
        "CASE WHEN e.is_team_content THEN 'team or self content' END",
        "CASE WHEN NOT e.is_eligible THEN 'excluded: ' || e.exclusion_reason END"
    ]) }} AS reasons_json,
    {{ sl_now_utc() }} AS first_assessed_at,
    {{ sl_now_utc() }} AS assessed_at
FROM effective e
