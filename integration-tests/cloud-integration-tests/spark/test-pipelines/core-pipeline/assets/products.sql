/* @bruin
name: local.bruin_test.products
type: spark.sql

materialization:
  type: table
  strategy: create+replace
  partition_by: category
  cluster_by:
    - product_id

columns:
  - name: product_id
    type: INT
    checks:
      - name: not_null
      - name: unique
      - name: positive
      - name: min
        value: 1
      - name: max
        value: 4
  - name: product_name
    type: STRING
    checks:
      - name: not_null
      - name: pattern
        value: "^[A-Z].*"
  - name: price
    type: DOUBLE
    checks:
      - name: positive
  - name: category
    type: STRING
    checks:
      - name: accepted_values
        value: ["electronics", "accessories"]

custom_checks:
  - name: row count is four
    value: 4
    query: SELECT COUNT(*) FROM local.bruin_test.products
@bruin */

SELECT 1 AS product_id, 'Laptop' AS product_name, 999.99 AS price, 'electronics' AS category
UNION ALL
SELECT 2 AS product_id, 'Smartphone' AS product_name, 699.99 AS price, 'electronics' AS category
UNION ALL
SELECT 3 AS product_id, 'Headphones' AS product_name, 199.99 AS price, 'accessories' AS category
UNION ALL
SELECT 4 AS product_id, 'Monitor' AS product_name, 299.99 AS price, 'electronics' AS category
