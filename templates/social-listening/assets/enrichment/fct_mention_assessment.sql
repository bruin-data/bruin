/* @bruin
name: enrichment.fct_mention_assessment
type: duckdb.sql
materialization: {type: table}
depends: [enrichment.fct_term_match, staging.stg_content_item]
columns:
  - name: assessment_id
    type: string
    primary_key: true
    checks: [{name: not_null}, {name: unique}]
  - name: status
    type: string
    checks: [{name: accepted_values, value: [eligible, excluded, llm_unavailable, assessed]}]
  - name: priority_score
    type: double
    checks: [{name: non_negative}]
@bruin */

WITH ranked_matches AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY matched_at DESC, term_id) AS row_num
  FROM enrichment.fct_term_match
), excluded_authors AS (
  {% if var.excluded_authors|length == 0 %}
  SELECT CAST(NULL AS VARCHAR) AS author WHERE FALSE
  {% else %}
  {% for author in var.excluded_authors %}
  SELECT '{{ author | replace("'", "''") }}' AS author{% if not loop.last %} UNION ALL{% endif %}
  {% endfor %}
  {% endif %}
), team_authors AS (
  {% if var.team_authors|length == 0 %}
  SELECT CAST(NULL AS VARCHAR) AS author WHERE FALSE
  {% else %}
  {% for author in var.team_authors %}
  SELECT '{{ author | replace("'", "''") }}' AS author{% if not loop.last %} UNION ALL{% endif %}
  {% endfor %}
  {% endif %}
), candidates AS (
  SELECT match.content_id, item.author, item.published_at, item.title, item.body, item.metrics_json,
    lower(coalesce(item.title, '') || ' ' || coalesce(item.body, '')) AS content_text
  FROM ranked_matches match
  JOIN staging.stg_content_item item USING (content_id)
  WHERE match.row_num = 1
), exclusions AS (
  SELECT *, author IN (SELECT author FROM excluded_authors)
      OR author IN (SELECT author FROM team_authors) AS is_excluded
  FROM candidates
), components AS (
  SELECT *,
    CASE WHEN content_text LIKE '%looking for%' OR content_text LIKE '%recommend%' OR content_text LIKE '%alternative%' THEN 0.90 ELSE 0.55 END AS relevance_score,
    CASE WHEN content_text LIKE '%looking for%' OR content_text LIKE '%recommend%' THEN 0.85 ELSE 0.50 END AS intent_score,
    CASE WHEN is_excluded THEN 0.0 ELSE 0.80 END AS authenticity_score,
    LEAST(COALESCE(CAST(json_extract_string(metrics_json, '$.score') AS DOUBLE), 0) / 20.0, 1.0) AS engagement_score,
    CASE WHEN published_at >= CURRENT_TIMESTAMP - INTERVAL '{{ var.max_content_age_days }} days' THEN 1.0 ELSE 0.20 END AS freshness_score
  FROM exclusions
)
SELECT sha256(content_id || ':deterministic-1.0') AS assessment_id, content_id, 'deterministic-1.0' AS assessment_version,
  relevance_score, intent_score AS intent_fit_score, 0.75 AS fit_score, engagement_score, freshness_score, authenticity_score,
  ROUND((relevance_score * .35 + intent_score * .20 + engagement_score * .15 + freshness_score * .15 + authenticity_score * .15), 4) AS priority_score,
  CASE WHEN {{ var.llm_enabled }} THEN 0.0 ELSE 0.75 END AS confidence_score,
  json_object('relevance', relevance_score, 'intent', intent_score, 'engagement', engagement_score, 'freshness', freshness_score, 'authenticity', authenticity_score) AS score_components_json,
  json_object('rule', 'deterministic-1.0', 'evidence', substr(content_text, 1, 300)) AS reasons_json,
  CASE WHEN is_excluded THEN 'excluded' WHEN {{ var.llm_enabled }} THEN 'llm_unavailable' ELSE 'eligible' END AS status,
  CASE WHEN {{ var.llm_enabled }} THEN '{{ var.llm_model | replace("'", "''") }}' ELSE NULL END AS model_version,
  CASE WHEN {{ var.llm_enabled }} THEN 'configured-model-not-run-in-demo' ELSE NULL END AS prompt_version,
  CURRENT_TIMESTAMP AS assessed_at
FROM components;
