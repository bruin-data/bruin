/* @bruin
name: bruin_test.merge_orders__RUN_ID__
type: fabric.sql

materialization:
  type: table
  strategy: merge

# Opt into the native T-SQL MERGE and create the table from the columns on the
# first run, since refresh_restricted assets are never built by a full refresh.
parameters:
  fabric_native_merge: true
refresh_restricted: true

columns:
  - name: order_id
    type: INT
    primary_key: true
  - name: status
    type: VARCHAR(20)
    merge_sql: CASE WHEN source.updated_at > target.updated_at THEN source.status ELSE target.status END
  - name: amount
    type: DECIMAL(10, 2)
    merge_sql: CASE WHEN source.updated_at > target.updated_at THEN source.amount ELSE target.amount END
  - name: updated_at
    type: DATETIME2(6)
    merge_sql: CASE WHEN source.updated_at > target.updated_at THEN source.updated_at ELSE target.updated_at END
  - name: created_at
    type: DATETIME2(6)
@bruin */

-- A CTE body works because the query is staged with CTAS before the MERGE.
WITH orders AS (
    SELECT 1 AS order_id, 'placed' AS status, CAST(10.00 AS DECIMAL(10, 2)) AS amount, CAST('2026-01-02 00:00:00' AS DATETIME2(6)) AS updated_at, CAST('2026-01-01 00:00:00' AS DATETIME2(6)) AS created_at
    UNION ALL
    SELECT 2, 'placed', CAST(20.00 AS DECIMAL(10, 2)), CAST('2026-01-02 00:00:00' AS DATETIME2(6)), CAST('2026-01-01 00:00:00' AS DATETIME2(6))
)
SELECT order_id, status, amount, updated_at, created_at
FROM orders
