/* @bruin
name: products_individual
type: athena.sql

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
    1 AS PRODUCT_ID, 'Laptop' AS PRODUCT_NAME, 999.99 AS PRICE, 10 AS STOCK
UNION ALL
SELECT
    2 AS PRODUCT_ID, 'Smartphone' AS PRODUCT_NAME, 699.99 AS PRICE, 50 AS STOCK
UNION ALL
SELECT
    3 AS PRODUCT_ID, 'Headphones' AS PRODUCT_NAME, 199.99 AS PRICE, 100 AS STOCK
UNION ALL
SELECT
    4 AS PRODUCT_ID, 'Monitor' AS PRODUCT_NAME, 299.99 AS PRICE, 25 AS STOCK; 