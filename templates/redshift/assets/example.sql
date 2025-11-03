/* @bruin

name: public.example
type: rs.sql
description: |
  # Example table
  This asset is an example table with some quality checks to help you get started.

  You can write Markdown here, it supports `inline codeblocks` or larger blocks of code. It supports **bold** and *italic* text.

materialization:
  type: table

columns:
  - name: id
    type: integer
    description: Just a number
    primary_key: true
    checks:
      - name: not_null
      - name: positive
  - name: country
    type: varchar
    description: the country
    primary_key: true
    checks:
      - name: not_null
  - name: name
    type: varchar
    description: Just a name
    update_on_merge: true
    checks:
      - name: unique
      - name: not_null

custom_checks:
  - name: match column counts
    value: 4
    query: SELECT COUNT(*) as count FROM public.example

@bruin */

SELECT
    1 AS id,
    'Spain' AS country,
    'Juan' AS name
UNION ALL
SELECT
    2 AS id,
    'Germany' AS country,
    'Markus' AS name
UNION ALL
SELECT
    3 AS id,
    'France' AS country,
    'Antoine' AS name
UNION ALL
SELECT
    4 AS id,
    'Poland' AS country,
    'Franciszek' AS name
