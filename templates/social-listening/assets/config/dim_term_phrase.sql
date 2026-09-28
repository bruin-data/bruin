/* @bruin
name: config.dim_term_phrase
description: |
  One row per tracked phrase, rendered from brand_name, brand_aliases,
  competitors, tracked_terms and term_rules. phrase_pattern is the
  regex-escaped phrase used by the word-boundary matcher; platform_scope is a
  comma-delimited source list (empty means every source).
tags: [enrich, config]
depends:
  - config.settings
materialization:
  type: table
columns:
  - name: term_id
    type: varchar
    checks:
      - name: not_null
  - name: category
    type: varchar
    checks:
      - name: accepted_values
        value: [brand, competitor, topic]
  - name: phrase_lower
    type: varchar
    checks:
      - name: not_null
  - name: valid_from
    type: date
  - name: valid_to
    type: date
  - name: is_active
    type: boolean
custom_checks:
  - name: phrase is unique within each term
    query: |
      SELECT COUNT(*) FROM (
        SELECT term_id, phrase_lower FROM config.dim_term_phrase GROUP BY 1, 2 HAVING COUNT(*) > 1
      )
    value: 0
@bruin */

SELECT DISTINCT
    term_id,
    term_name,
    category,
    phrase,
    phrase_lower,
    {{ sl_regex_escape('phrase_lower') }} AS phrase_pattern,
    platform_scope,
    valid_from,
    valid_to,
    is_active,
    term_origin
FROM ({{ sl_term_phrase_rows(var) }}) AS phrases
