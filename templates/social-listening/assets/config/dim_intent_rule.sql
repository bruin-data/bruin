/* @bruin
name: config.dim_intent_rule
description: |
  Keyword rules for the deterministic intent classifier, rendered from
  intent_taxonomy. The highest-scoring matching intent wins; content with no
  match is `general_discussion` with a score of 0.2.
tags: [enrich, config]
depends:
  - config.settings
materialization:
  type: table
columns:
  - name: intent
    type: varchar
    checks:
      - name: not_null
  - name: keyword_lower
    type: varchar
    checks:
      - name: not_null
  - name: intent_score
    type: double
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
@bruin */

SELECT intent, keyword_lower, intent_score, taxonomy_rank
FROM ({{ sl_intent_rows(var) }}) AS rules
