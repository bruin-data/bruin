/* @bruin
name: bruin_test.products__RUN_ID__
type: fw.sql

materialization:
  type: table

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
    primary_key: true
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
  - name: price
    type: FLOAT
    description: "Price of the product in USD"
    checks:
      - name: positive
  - name: stock
    type: INTEGER
    description: "Number of units in stock"
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
