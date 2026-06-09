/* @bruin
name: bruin_test.bad_price
type: dremio.sql

materialization:
  type: table
  strategy: create+replace

columns:
  - name: id
    type: INTEGER
    primary_key: true
  - name: price
    type: DOUBLE
    description: "Negative value here must make the positive check fail"
    checks:
      - name: positive
@bruin */

SELECT 1 AS id, 10.0 AS price
UNION ALL
SELECT 2 AS id, -5.0 AS price
