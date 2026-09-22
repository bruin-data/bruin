/* @bruin
name: config.dim_term
type: duckdb.sql
materialization: {type: table}
columns:
  - name: term_id
    type: string
    primary_key: true
    checks: [{name: not_null}, {name: unique}]
  - name: phrase
    type: string
    checks: [{name: not_null}]
  - name: category
    type: string
    checks: [{name: accepted_values, value: [tracked_term, brand, competitor]}]
  - name: active
    type: boolean
    checks: [{name: not_null}]
@bruin */

WITH configured_terms AS (
  {% for phrase in var.tracked_terms %}
  SELECT '{{ phrase | replace("'", "''") }}' AS phrase, 'tracked_term' AS category{% if not loop.last %} UNION ALL{% endif %}
  {% endfor %}
), brand AS (
  SELECT '{{ var.brand_name | replace("'", "''") }}' AS phrase, 'brand' AS category
), competitors AS (
  {% if var.competitors|length == 0 %}
  SELECT CAST(NULL AS VARCHAR) AS phrase, CAST(NULL AS VARCHAR) AS category WHERE FALSE
  {% else %}
  {% for phrase in var.competitors %}
  SELECT '{{ phrase | replace("'", "''") }}' AS phrase, 'competitor' AS category{% if not loop.last %} UNION ALL{% endif %}
  {% endfor %}
  {% endif %}
), ranked_terms AS (
  SELECT phrase, category,
    ROW_NUMBER() OVER (PARTITION BY lower(trim(phrase)) ORDER BY
      CASE category WHEN 'tracked_term' THEN 1 WHEN 'brand' THEN 2 ELSE 3 END) AS row_num
  FROM (SELECT * FROM configured_terms UNION ALL SELECT * FROM brand UNION ALL SELECT * FROM competitors)
  WHERE phrase IS NOT NULL AND trim(phrase) <> ''
)
SELECT sha256(lower(trim(phrase))) AS term_id, phrase, category, 'all' AS platform_scope,
  TRUE AS active, CURRENT_DATE AS valid_from, CAST(NULL AS DATE) AS valid_to
FROM ranked_terms
WHERE row_num = 1;
