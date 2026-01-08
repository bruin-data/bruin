/* @bruin

name: test.products_partitioned
type: databricks.sql

materialization:
    type: table
    strategy: create+replace
    partition_by: category

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
  - name: price
    type: FLOAT
    description: "Price of the product"
  - name: category
    type: VARCHAR
    description: "Product category for partitioning"

@bruin */

SELECT 1 AS product_id, 'Laptop' AS product_name, 999.99 AS price, 'Electronics' AS category
UNION ALL
SELECT 2 AS product_id, 'Smartphone' AS product_name, 699.99 AS price, 'Electronics' AS category
UNION ALL
SELECT 3 AS product_id, 'Desk' AS product_name, 299.99 AS price, 'Furniture' AS category
UNION ALL
SELECT 4 AS product_id, 'Chair' AS product_name, 199.99 AS price, 'Furniture' AS category
UNION ALL
SELECT 5 AS product_id, 'Notebook' AS product_name, 9.99 AS price, 'Stationery' AS category

