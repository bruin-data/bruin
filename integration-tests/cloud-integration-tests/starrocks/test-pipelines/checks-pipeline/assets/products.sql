/* @bruin
name: bruin_test.products
type: starrocks.sql

materialization:
  type: table
  strategy: create+replace

columns:
  - name: product_id
    type: INT
    description: "Unique identifier for the product"
    checks:
      - name: not_null
      - name: unique
      - name: positive
  - name: product_name
    type: STRING
    description: "Name of the product"
    checks:
      - name: not_null
  - name: price
    type: DOUBLE
    description: "Price of the product in USD"
    checks:
      - name: positive
  - name: category
    type: STRING
    description: "Product category"
    checks:
      - name: accepted_values
        value: ["electronics", "accessories"]

custom_checks:
  - name: row count is four
    value: 4
    query: SELECT COUNT(*) FROM `bruin_test`.`products`
@bruin */

SELECT 1 AS product_id, 'Laptop' AS product_name, 999.99 AS price, 'electronics' AS category
UNION ALL
SELECT 2 AS product_id, 'Smartphone' AS product_name, 699.99 AS price, 'electronics' AS category
UNION ALL
SELECT 3 AS product_id, 'Headphones' AS product_name, 199.99 AS price, 'accessories' AS category
UNION ALL
SELECT 4 AS product_id, 'Monitor' AS product_name, 299.99 AS price, 'electronics' AS category
