/* @bruin

name: public.products_merge
type: rs.sql

materialization:
    type: table
    strategy: merge

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
    primary_key: true
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
    update_on_merge: true
  - name: price
    type: INTEGER
    description: "Price of the product in USD"
    update_on_merge: true

@bruin */

SELECT 1 AS product_id, 'Laptop' AS product_name, 1299 AS price
UNION ALL
SELECT 2 AS product_id, 'Smartphone' AS product_name, 699 AS price
UNION ALL
SELECT 3 AS product_id, 'Headphones' AS product_name, 199 AS price
UNION ALL
SELECT 4 AS product_id, 'Monitor' AS product_name, 299 AS price
