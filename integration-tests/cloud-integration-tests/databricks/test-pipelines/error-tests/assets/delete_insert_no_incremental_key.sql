/* @bruin

name: test.delete_insert_no_key
type: databricks.sql

materialization:
    type: table
    strategy: delete+insert

columns:
  - name: product_id
    type: INTEGER
  - name: product_name
    type: VARCHAR

@bruin */

SELECT 1 AS product_id, 'Laptop' AS product_name

