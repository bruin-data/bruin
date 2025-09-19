/* @bruin
name: test_truncate_insert_strategy
type: duckdb.sql

materialization:
  type: table
  strategy: truncate+insert

@bruin */

SELECT 
    1 as id,
    'test_data' as description,
    CURRENT_DATE as created_date