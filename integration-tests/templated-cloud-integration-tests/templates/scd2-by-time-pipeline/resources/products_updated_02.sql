/* @bruin
name: test.products
materialization:
  type: table
  strategy: scd2_by_time
  incremental_key: dt

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
    primary_key: true
  - name: product_name
    type: VARCHAR
    primary_key: true
    description: "Name of the product"
  - name: dt
    type: DATE
    description: "incremental key"
  - name: stock
    type: INTEGER
    description: "Number of units in stock"
@bruin */
SELECT
    3 AS product_id,
    'Headphones' AS product_name,
    120 AS stock,
    DATE '2025-06-10' AS dt

