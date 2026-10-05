/* @bruin
name: enrichment.fct_term_match
description: |
  One row per content item and tracked term that the deterministic matcher
  considered. The matcher looks for the phrase on word boundaries
  (case-insensitive) within the term's platform scope and validity dates, then
  applies the term's context rules. Rejected ambiguous matches are kept with
  is_accepted = false so they can be audited.

  rule_id values (matcher v1):
    phrase_boundary.v1           phrase found, no context rule
    phrase_boundary+context.v1   phrase found and a required context phrase present
    rejected.context_missing.v1  phrase found, no required context phrase
    rejected.excluded_context.v1 phrase found alongside an excluded context phrase
tags: [enrich]
depends:
  - staging.stg_content_item
  - config.dim_term_phrase
  - config.dim_term_context
materialization:
  type: table
columns:
  - name: match_id
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
  - name: term_id
    type: varchar
    checks:
      - name: not_null
      - name: relationships
    foreign_key:
      table: config.dim_term
      column: term_id
  - name: matched_text
    type: varchar
    checks:
      - name: not_null
  - name: rule_id
    type: varchar
    checks:
      - name: accepted_values
        value: [phrase_boundary.v1, phrase_boundary+context.v1, rejected.context_missing.v1, rejected.excluded_context.v1]
  - name: is_accepted
    type: boolean
    checks:
      - name: not_null
  - name: matcher_version
    type: varchar
    checks:
      - name: not_null
@bruin */

WITH content AS (
    SELECT
        content_id,
        source,
        published_at,
        COALESCE(title, '') || ' ' || COALESCE(body, '') AS text_raw,
        lower(COALESCE(title, '') || ' ' || COALESCE(body, '')) AS text_lower
    FROM staging.stg_content_item
    WHERE NOT is_removed
),

candidates AS (
    SELECT
        c.content_id,
        c.text_lower,
        p.term_id,
        p.category,
        p.phrase,
        {{ sl_regex_extract("c.text_raw", "'(?i)(?:^|[^a-z0-9])(' || p.phrase_pattern || ')(?:$|[^a-z0-9])'", 1) }} AS matched_text
    FROM content c
    JOIN config.dim_term_phrase p
        -- Cheap substring pre-filter before the word-boundary regex.
        ON {{ sl_contains("c.text_lower", "p.phrase_lower") }}
       AND p.is_active
       AND (p.platform_scope = '' OR {{ sl_contains("p.platform_scope", "',' || c.source || ','") }})
       AND CAST(c.published_at AS DATE) BETWEEN p.valid_from AND p.valid_to
),

bounded AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY content_id, term_id ORDER BY length(phrase) DESC, phrase) AS phrase_rank
    FROM candidates
    WHERE matched_text IS NOT NULL AND matched_text <> ''
),

context AS (
    SELECT
        b.content_id,
        b.term_id,
        MAX(CASE WHEN x.context_type = 'require' THEN 1 ELSE 0 END) AS needs_context,
        MAX(CASE WHEN x.context_type = 'require' AND {{ sl_contains("b.text_lower", "x.phrase_lower") }} THEN 1 ELSE 0 END) AS has_required,
        MAX(CASE WHEN x.context_type = 'exclude' AND {{ sl_contains("b.text_lower", "x.phrase_lower") }} THEN 1 ELSE 0 END) AS has_excluded
    FROM bounded b
    LEFT JOIN config.dim_term_context x ON x.term_id = b.term_id
    WHERE b.phrase_rank = 1
    GROUP BY b.content_id, b.term_id
)

SELECT
    {{ sl_hash("b.content_id || '|' || b.term_id || '|' || " ~ sl_str(var.matcher_version)) }} AS match_id,
    b.content_id,
    b.term_id,
    b.category,
    b.phrase AS matched_phrase,
    b.matched_text,
    CASE
        WHEN x.has_excluded = 1 THEN 'rejected.excluded_context.v1'
        WHEN x.needs_context = 1 AND x.has_required = 0 THEN 'rejected.context_missing.v1'
        WHEN x.needs_context = 1 THEN 'phrase_boundary+context.v1'
        ELSE 'phrase_boundary.v1'
    END AS rule_id,
    (x.has_excluded = 0 AND (x.needs_context = 0 OR x.has_required = 1)) AS is_accepted,
    {{ sl_str(var.matcher_version) }} AS matcher_version,
    {{ sl_now_utc() }} AS matched_at
FROM bounded b
JOIN context x ON x.content_id = b.content_id AND x.term_id = b.term_id
WHERE b.phrase_rank = 1
