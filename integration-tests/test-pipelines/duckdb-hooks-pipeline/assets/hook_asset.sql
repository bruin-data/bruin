/* @bruin

name: hooks_test.main_table
type: duckdb.sql
materialization:
  type: table
hooks:
  pre:
    - query: "CREATE SCHEMA IF NOT EXISTS hooks_test"
    - query: "DROP TABLE IF EXISTS hooks_test.hook_log"
    - query: "CREATE TABLE hooks_test.hook_log (step INTEGER)"
    - query: "INSERT INTO hooks_test.hook_log VALUES (1)"
  post:
    - query: "INSERT INTO hooks_test.hook_log VALUES (2)"

@bruin */

SELECT 42 AS id
