/* @bruin
name: config.dim_term_context
description: |
  Context rules for ambiguous phrases from term_rules. A term with any
  `require` rows only matches when one of those phrases also appears in the
  content; any `exclude` phrase rejects the match.
tags: [enrich, config]
depends:
  - config.dim_term
materialization:
  type: table
columns:
  - name: term_id
    type: varchar
    checks:
      - name: not_null
      - name: relationships
    foreign_key:
      table: config.dim_term
      column: term_id
  - name: context_type
    type: varchar
    checks:
      - name: accepted_values
        value: [require, exclude]
@bruin */

SELECT term_id, context_type, phrase_lower
FROM ({{ sl_term_context_rows(var) }}) AS rules
