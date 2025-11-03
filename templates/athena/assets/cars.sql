/* @bruin

materialization:
  type: table
  strategy: delete+insert
  incremental_key: id

columns:
  - name: id
    type: integer
    description: identifier of the car
    primary_key: true
    checks:
      - name: not_null
      - name: positive
  - name: plate
    type: varchar
    description: plate number
    checks:
      - name: not_null
      - name: unique

@bruin */

SELECT
    1 AS id,
    'XWE12312' AS name
UNION ALL
SELECT
    2 AS id,
    'TRE34535' AS name
UNION ALL
SELECT
    3 AS id,
    'OIY54654' AS name
