/* @bruin
name: test.products
type: duckdb.sql
materialization:
  type: table
  strategy: scd2_by_column
  incremental_key: updated_at

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for Product"
    primary_key: true
  - name: product_name
    type: VARCHAR
    description: "Name of the Product"
  - name: price
    type: FLOAT
    description: "Price of the Product"
  - name: updated_at
    type: TIMESTAMP
    description: "When the product was last updated"
@bruin */

SELECT 1 AS product_id, 'Laptop' AS product_name, 999.99 AS price, TIMESTAMP '2024-01-15 10:00:00' AS updated_at
UNION ALL
SELECT 2 AS product_id, 'Mouse' AS product_name, 29.99 AS price, TIMESTAMP '2024-01-15 10:00:00' AS updated_at
UNION ALL
SELECT 3 AS product_id, 'Keyboard' AS product_name, 79.99 AS price, TIMESTAMP '2024-01-15 10:00:00' AS updated_at
