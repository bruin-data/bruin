/* @bruin

name: users
type: duckdb.sql
description: |
  Base user dimension for the `bruin-duckdb` sample DuckDB lineage pipeline. The
  asset materializes a small inline dataset of user records directly from SQL
  literals, so it behaves more like seed or reference data than a raw ingestion
  table.

  Each row represents a single user keyed by `id`, with first name, last name,
  country, and a creation timestamp used by downstream examples. The asset is
  intentionally simple: `people` projects the identity columns, `country`
  projects the geographic attribute, and `example` joins those downstream assets
  back together to demonstrate lineage.

  The current query is implemented with `UNION ALL` over hard-coded rows, so the
  dataset is expected to remain very small and to refresh as a full rebuild on
  each scheduled run of the pipeline. `name` and `last_name` represent
  person-identifying attributes and should be treated as PII-like in downstream
  use. One nuance in this template is that `created_at` is documented as a
  timestamp, while the SQL currently provides ISO date string literals instead
  of an explicit timestamp cast.
tags:
  - identity
  - dimension_table
  - demo
  - lineage_example
  - pii
domains:
  - identity
meta:
  downstream_assets: people,country
  expected_size: very small static demo dataset
  operational_notes: Data changes only when this SQL file is edited; the asset is rebuilt from scratch on each run; `created_at` is emitted as a string literal in the query body.
  refresh_cadence: daily
  row_grain: one row per user `id`
  source_kind: inline_sql_literals
  update_pattern: full_rebuild

materialization:
  type: table

columns:
  - name: id
    type: integer
    description: Just a number
    tags:
      - identifier
      - user_key
    primary_key: true
    domains:
      - identity
    meta:
      business_meaning: Stable row-level identifier for a user record in this demo dataset.
      expected_cardinality: unique
      semantic_type: identifier
      source_kind: inline_literal
    checks:
      - name: unique
      - name: not_null
  - name: name
    type: varchar
    description: Just a name
    tags:
      - pii
      - given_name
      - dimension
    domains:
      - identity
    meta:
      business_meaning: User given name captured as a descriptive attribute.
      expected_cardinality: non_unique
      semantic_type: dimension
      sensitivity: direct_identifier_like
      source_kind: inline_literal
  - name: last_name
    type: varchar
    description: Just a last name
    tags:
      - pii
      - surname
      - dimension
    domains:
      - identity
    meta:
      business_meaning: User family name or surname captured as a descriptive attribute.
      expected_cardinality: non_unique
      semantic_type: dimension
      sensitivity: direct_identifier_like
      source_kind: inline_literal
  - name: country
    type: varchar
    description: Just a country
    tags:
      - geography
      - dimension
    domains:
      - identity
      - geography
    meta:
      business_meaning: Free-text country label associated with the user record.
      expected_cardinality: low_cardinality
      notes: Values are country names, not ISO country codes.
      semantic_type: geographic_dimension
      source_kind: inline_literal
  - name: created_at
    type: timestamp
    description: Just a timestamp
    tags:
      - timestamp
      - audit
    domains:
      - identity
    meta:
      business_meaning: Creation timestamp for the sample user record.
      notes: SQL currently provides ISO date string literals rather than explicit timestamp expressions.
      semantic_type: timestamp
      source_kind: inline_literal
      units: timestamp
    checks:
      - name: not_null

@bruin */

SELECT 1 as id, 'John' as name, 'Doe' as last_name, 'USA' as country, '2021-01-01' as created_at
UNION ALL
SELECT 2 as id, 'Jane' as name, 'Smith' as last_name, 'Canada' as country, '2021-01-02' as created_at
UNION ALL
SELECT 3 as id, 'Jim' as name, 'Beam' as last_name, 'UK' as country, '2021-01-03' as created_at
UNION ALL
SELECT 4 as id, 'Jill' as name, 'Johnson' as last_name, 'Australia' as country, '2021-01-04' as created_at
