/* @bruin

name: invalid_date
type: duckdb.sql
start_date: "notvalid"
materialization:
   type: table
@bruin */

SELECT 1 as id, '{{ start_date }}' as start_date
