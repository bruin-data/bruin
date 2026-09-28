/* @bruin
name: config.dim_term
description: |
  One row per tracked concept (the brand, each competitor, each tracked term
  and each term rule) with its phrases, category, platform scope, active
  status and validity dates.
tags: [enrich, config]
depends:
  - config.dim_term_phrase
materialization:
  type: table
columns:
  - name: term_id
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: category
    type: varchar
    checks:
      - name: accepted_values
        value: [brand, competitor, topic]
  - name: phrases_json
    type: varchar
  - name: platform_scope
    type: varchar
  - name: is_active
    type: boolean
  - name: valid_from
    type: date
  - name: valid_to
    type: date
@bruin */

SELECT
    term_id,
    MIN(term_name) AS term_name,
    MIN(category) AS category,
    {{ sl_json_array_agg('phrase', 'phrase') }} AS phrases_json,
    MIN(platform_scope) AS platform_scope,
    BOOL_OR(is_active) AS is_active,
    MIN(valid_from) AS valid_from,
    MAX(valid_to) AS valid_to,
    MIN(term_origin) AS term_origin
FROM config.dim_term_phrase
GROUP BY term_id
