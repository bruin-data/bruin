/* @bruin
name: first_run.orders
type: duckdb.sql
materialization:
  type: table
  strategy: merge

columns:
  - name: order_id
    type: INTEGER
    primary_key: true
  - name: status
    type: VARCHAR
    update_on_merge: true
@bruin */

SELECT 1 AS order_id, 'shipped' AS status
UNION ALL
SELECT 2 AS order_id, 'pending' AS status
