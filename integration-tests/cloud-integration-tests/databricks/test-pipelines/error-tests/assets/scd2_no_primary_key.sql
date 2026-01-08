/* @bruin

name: test.scd2_no_pk
type: databricks.sql

materialization:
    type: table
    strategy: scd2_by_column

columns:
  - name: product_id
    type: INTEGER
    description: "No primary key defined - should fail"
  - name: product_name
    type: VARCHAR

@bruin */

SELECT 1 AS product_id, 'Laptop' AS product_name

