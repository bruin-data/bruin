/* @bruin

name: valid_date
type: duckdb.sql
start_date: 2024-01-01
materialization:
   type: table
@bruin */

SELECT 2 as id, '{{ start_date }}' as start_date
