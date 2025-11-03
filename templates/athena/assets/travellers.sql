/* @bruin

materialization:
  type: table

columns:
  - name: id
    type: integer
    description: identifier of the traveller
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
    'frank' AS name
UNION ALL
SELECT
    3 AS id,
    'germany' AS country,
    'franz' AS name
UNION ALL
SELECT
    4 AS id,
    'france' AS country,
    'louis' AS name
UNION ALL
SELECT
    5 AS id,
    'poland' AS country,
    'maciej' AS name
UNION ALL
SELECT
    6 AS id,
    'russia' AS country,
    'ivan' AS name
