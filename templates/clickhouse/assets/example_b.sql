/* @bruin

type: clickhouse.sql

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
      - name: non_negative
  - name: country_name
    type: varchar
    description: the country
    checks:
      - name: not_null

@bruin */

SELECT
    1 AS id,
    'belgium' AS country_name
UNION ALL
SELECT
    2 AS id,
    'germany' AS country_name
UNION ALL
SELECT
    3 AS id,
    'denmark' AS country_name
