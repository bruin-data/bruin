/* @bruin

name: test.time_interval_no_key
type: databricks.sql

materialization:
    type: table
    strategy: time_interval
    time_granularity: date

columns:
  - name: product_id
    type: INTEGER
  - name: product_name
    type: VARCHAR

@bruin */

SELECT 1 AS product_id, 'Laptop' AS product_name

