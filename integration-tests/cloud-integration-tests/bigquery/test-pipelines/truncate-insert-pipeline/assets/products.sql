/* @bruin

name: cloud_integration_test.truncate_insert_materialization_products
type: bq.sql

materialization:
    type: table
    strategy: truncate+insert

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
  - name: price
    type: INTEGER
    description: "Price of the product in cents"
  - name: stock
    type: INTEGER
    description: "Number of units in stock"

@bruin */

SELECT 1 AS product_id, 'Laptop' AS product_name, 99999 AS price, 10 AS stock
UNION ALL
SELECT 2 AS product_id, 'Smartphone' AS product_name, 69999 AS price, 50 AS stock
UNION ALL
SELECT 3 AS product_id, 'Headphones' AS product_name, 19999 AS price, 100 AS stock

