/* @bruin

name: test.products_view
type: databricks.sql

materialization:
    type: view

@bruin */

SELECT 
    1 AS product_id, 
    'Laptop' AS product_name, 
    999.99 AS price
UNION ALL
SELECT 
    2 AS product_id, 
    'Smartphone' AS product_name, 
    699.99 AS price
UNION ALL
SELECT 
    3 AS product_id, 
    'Headphones' AS product_name, 
    199.99 AS price

