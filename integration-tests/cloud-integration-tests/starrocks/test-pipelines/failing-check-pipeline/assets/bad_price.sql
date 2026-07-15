/* @bruin
name: bruin_test.bad_price
type: starrocks.sql

materialization:
  type: table
  strategy: create+replace

columns:
  - name: id
    type: INT
    description: "Identifier"
  - name: price
    type: DOUBLE
    description: "Price that should be positive"
    checks:
      - name: positive
@bruin */

SELECT 1 AS id, -5.0 AS price
