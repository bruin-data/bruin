/* @bruin
name: test.products
type: duckdb.sql

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
    900 AS stock,
    DATE '2025-06-02' AS dt
UNION ALL
SELECT
    5 AS product_id,
    'ps5' AS product_name,
    25 AS stock,
    DATE '2025-06-02' AS dt