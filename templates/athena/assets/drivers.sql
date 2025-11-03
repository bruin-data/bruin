/* @bruin

materialization:
  type: table
  strategy: append

columns:
  - name: id
    type: integer
    description: identifier of the driver
    primary_key: true
    checks:
      - name: not_null
      - name: positive
  - name: name
    type: varchar
    description: Just a name
    checks:
      - name: not_null

@bruin */

SELECT
    1 AS id,
    'valentino' AS name
UNION ALL
SELECT
    2 AS id,
    'alonso' AS name
UNION ALL
SELECT
    3 AS id,
    'senna' AS name
UNION ALL
SELECT
    4 AS id,
    'lewis' AS name
