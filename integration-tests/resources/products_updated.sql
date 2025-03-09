/* @bruin
name: products
type: duckdb.sql

materialization:
  type: table
  strategy: time_interval
  time_granularity: date
  incremental_key : dt

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
    1 AS product_id,
    'Laptop' AS product_name,
    699.99 AS price,
    100 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    2 AS product_id,
    'Smartphone' AS product_name,
    899.99 AS price,
    150 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    3 AS product_id,
    'Headphones' AS product_name,
    399.99 AS price,
    175 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    4 AS product_id,
    'Monitor' AS product_name,
    299.99 AS price,
    25 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    5 AS product_id,
    'Keyboard' AS product_name,
    49.99 AS price,
    75 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    6 AS product_id,
    'Mouse' AS product_name,
    39.99 AS price,
    100 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    7 AS product_id,
    'Tablet' AS product_name,
    599.99 AS price,
    75 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    8 AS product_id,
    'Smartwatch' AS product_name,
    249.99 AS price,
    30 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    9 AS product_id,
    'Gaming Console' AS product_name,
    699.99 AS price,
    250 AS stock,
    DATE '2025-04-02' AS dt;


