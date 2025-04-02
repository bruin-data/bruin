/* @bruin
name: products
type: duckdb.sql

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
    1 AS product_id,
    'Laptop' AS product_name,
    699 AS price,
    100 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    2 AS product_id,
    'Smartphone' AS product_name,
    899 AS price,
    150 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    3 AS product_id,
    'Headphones' AS product_name,
    399 AS price,
    175 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    4 AS product_id,
    'Monitor' AS product_name,
    299 AS price,
    25 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    5 AS product_id,
    'Keyboard' AS product_name,
    49 AS price,
    75 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    6 AS product_id,
    'Mouse' AS product_name,
    39 AS price,
    100 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    7 AS product_id,
    'Tablet' AS product_name,
    599 AS price,
    75 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    8 AS product_id,
    'Smartwatch' AS product_name,
    249 AS price,
    30 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    9 AS product_id,
    'Gaming Console' AS product_name,
    699 AS price,
    250 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    10 AS product_id,
    'Laptop2' AS product_name,
    699 AS price,
    100 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    11 AS product_id,
    'Smartphone2' AS product_name,
    899 AS price,
    150 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    12 AS product_id,
    'Headphones2' AS product_name,
    399 AS price,
    175 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    13 AS product_id,
    'Monitor2' AS product_name,
    299 AS price,
    25 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    14 AS product_id,
    'Keyboard2' AS product_name,
    49 AS price,
    75 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    15 AS product_id,
    'Mouse2' AS product_name,
    39 AS price,
    100 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    16 AS product_id,
    'Tablet2' AS product_name,
    599 AS price,
    75 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    17 AS product_id,
    'Smartwatch2' AS product_name,
    249 AS price,
    30 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    18 AS product_id,
    'Gaming Console2' AS product_name,
    699 AS price,
    250 AS stock,
    DATE '2025-04-02' AS dt;


