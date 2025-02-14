/* @bruin
name: products
type: duckdb.sql

materialization:
  type: table

extends: []

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
    primary_key: true
    checks:
      - name: not_null
      - name: positive
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
    checks:
      - name: not_null
  - name: price
    type: FLOAT
    description: "Price of the product in USD"
    checks:
      - name: not_null
      - name: positive
  - name: stock
    type: INTEGER
    description: "Number of units in stock"
    checks:
      - name: non_negative
@bruin */

SELECT
    1 AS product_id, 'Laptop' AS product_name, 999.99 AS price, 10 AS stock
UNION ALL
SELECT
    2 AS product_id, 'Smartphone' AS product_name, 699.99 AS price, 50 AS stock
UNION ALL
SELECT
    3 AS product_id, 'Headphones' AS product_name, 199.99 AS price, 100 AS stock
UNION ALL
SELECT
    4 AS product_id, 'Monitor' AS product_name, 299.99 AS price, 25 AS stock;
