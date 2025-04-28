/* @bruin
name: products
type: duckdb.sql

materialization:
  type: table
  strategy: create+replace
  
interval_modifiers:
  start: -2h
  end: -2h


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
with t2 as (
    SELECT
        1 AS product_id,
        'Laptop' AS product_name,
        799 AS price,
        50 AS stock,
        TIMESTAMP '2025-04-02 08:00:00' AS dt
    UNION ALL
    SELECT
        2 AS product_id,
        'Smartphone' AS product_name,
        599 AS price,
        40 AS stock,
        TIMESTAMP '2025-04-02 09:00:00' AS dt
    UNION ALL
    SELECT
        3 AS product_id,
        'Headphones' AS product_name,
        399 AS price,
        100 AS stock,
        TIMESTAMP '2025-04-02 10:00:00' AS dt
    UNION ALL
    SELECT
        4 AS product_id,
        'Monitor' AS product_name,
        199 AS price,
        25 AS stock,
        TIMESTAMP '2025-04-02 11:00:00' AS dt
    UNION ALL
    SELECT
        5 AS product_id,
        'Keyboard' AS product_name,
        35 AS price,
        150 AS stock,
        TIMESTAMP '2025-04-02 12:00:00' AS dt
    UNION ALL
    SELECT
        6 AS product_id,
        'Mouse' AS product_name,
        29 AS price,
        120 AS stock,
        TIMESTAMP '2025-04-02 13:00:00' AS dt
    UNION ALL
    SELECT
        7 AS product_id,
        'Tablet' AS product_name,
        399 AS price,
        40 AS stock,
        TIMESTAMP '2025-04-02 14:00:00' AS dt
    UNION ALL
    SELECT
        8 AS product_id,
        'Smartwatch' AS product_name,
        249 AS price,
        70 AS stock,
        TIMESTAMP '2025-04-02 15:00:00' AS dt
    UNION ALL
    SELECT
        9 AS product_id,
        'Gaming Console' AS product_name,
        499.99 AS price,
        20 AS stock,
        TIMESTAMP '2025-04-02 16:00:00' AS dt

)
SELECT 
    product_id,
    product_name,
    stock,
    price,
    dt
FROM t2 
WHERE dt between '{{start_timestamp}}' and '{{end_timestamp}}'




