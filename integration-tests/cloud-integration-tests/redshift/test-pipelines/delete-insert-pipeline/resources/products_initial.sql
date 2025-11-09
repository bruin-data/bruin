/* @bruin

name: public.products_delete_insert
type: rs.sql

materialization:
    type: table
    strategy: delete+insert
    incremental_key: dt

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
  - name: dt
    type: DATE
    description: "Date when the product was added"

@bruin */

SELECT 1 AS product_id, 'Laptop' AS product_name, 99999 AS price, DATE '2024-01-01' AS dt
UNION ALL
SELECT 2 AS product_id, 'Smartphone' AS product_name, 69999 AS price, DATE '2024-01-01' AS dt
UNION ALL
SELECT 3 AS product_id, 'Headphones' AS product_name, 19999 AS price, DATE '2024-01-02' AS dt
UNION ALL
SELECT 4 AS product_id, 'Tablet' AS product_name, 49999 AS price, DATE '2024-01-03' AS dt
UNION ALL
SELECT 5 AS product_id, 'OldTablet' AS product_name, 39999 AS price, DATE '2024-01-15' AS dt
UNION ALL
SELECT 6 AS product_id, 'OldMouse' AS product_name, 1999 AS price, DATE '2024-01-15' AS dt
