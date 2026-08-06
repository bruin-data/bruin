/* @bruin
name: local.bruin_test.bad_price
type: spark.sql

materialization:
  type: table
  strategy: create+replace

columns:
  - name: id
    type: INT
  - name: price
    type: DOUBLE
    checks:
      - name: positive
@bruin */

SELECT 1 AS id, -5.0 AS price
