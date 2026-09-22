/* @bruin
name: enrichment.fct_term_match
type: duckdb.sql
materialization: {type: table}
depends: [staging.stg_content_item, config.dim_term]
columns:
  - name: match_id
    type: string
    primary_key: true
    checks: [{name: not_null}, {name: unique}]
  - name: content_id
    type: string
    checks: [{name: not_null}, {name: relationship, value: staging.stg_content_item.content_id}]
  - name: term_id
    type: string
    checks: [{name: not_null}, {name: relationship, value: config.dim_term.term_id}]
@bruin */

WITH excluded_terms AS (
  {% if var.excluded_terms|length == 0 %}
  SELECT CAST(NULL AS VARCHAR) AS phrase WHERE FALSE
  {% else %}
  {% for phrase in var.excluded_terms %}
  SELECT '{{ phrase | lower | replace("'", "''") }}' AS phrase{% if not loop.last %} UNION ALL{% endif %}
  {% endfor %}
  {% endif %}
)
SELECT
  sha256(item.content_id || ':' || term.term_id) AS match_id,
  item.content_id,
  term.term_id,
  term.phrase AS matched_text,
  'case_insensitive_substring_v1' AS deterministic_rule_id,
  'matcher-1.0.0' AS matcher_version,
  CURRENT_TIMESTAMP AS matched_at
FROM staging.stg_content_item item
JOIN config.dim_term term
  ON term.active
  AND lower(coalesce(item.title, '') || ' ' || coalesce(item.body, '')) LIKE '%' || lower(term.phrase) || '%'
WHERE NOT EXISTS (
  SELECT 1 FROM excluded_terms excluded
  WHERE lower(coalesce(item.title, '') || ' ' || coalesce(item.body, '')) LIKE '%' || excluded.phrase || '%'
);
