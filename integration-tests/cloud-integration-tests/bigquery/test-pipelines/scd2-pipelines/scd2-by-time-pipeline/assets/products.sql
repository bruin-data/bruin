/* @bruin
name: cloud_integration_test.scd2_by_time_products
type: bq.sql

materialization:
  type: table
  strategy: scd2_by_time
  incremental_key : dt

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
    primary_key: true
  - name: product_name
    type: VARCHAR
  - name: dt
    type: DATE
    description: "incremental key"
  - name: stock
    type: INTEGER
    description: "Number of units in stock"
@bruin */
SELECT
    1 AS product_id,
    'Laptop' AS product_name,
    100 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    2 AS product_id,
    'Smartphone' AS product_name,

    150 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    3 AS product_id,
    'Headphones' AS product_name,
    175 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    4 AS product_id,
    'Monitor' AS product_name,
    25 AS stock,
    DATE '2025-04-02' AS dt

