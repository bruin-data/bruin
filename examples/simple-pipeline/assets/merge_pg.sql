/* @bruin

name: test_merge
type: pg.sql

materialization:
  type: table
  strategy: merge

columns:
  - name: id
    type: integer
    description: Just a number
    primary_key: true
    checks:
      - name: not_null
      - name: positive
      - name: non_negative
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

@bruin */

SELECT
    1 AS id,
    'spain' AS country,
    'juan' AS name
UNION ALL
SELECT
    2 AS id,
    'germany' AS country,
    'markus' AS name
UNION ALL
SELECT
    3 AS id,
    'france' AS country,
    'antoine' AS name
UNION ALL
SELECT
    4 AS id,
    'poland' AS country,
    'franciszek' AS name
