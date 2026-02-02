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

SELECT 4 AS product_id, 'Monitor' AS product_name, 29999 AS price, 25 AS stock
UNION ALL
SELECT 5 AS product_id, 'Keyboard' AS product_name, 8999 AS price, 75 AS stock

