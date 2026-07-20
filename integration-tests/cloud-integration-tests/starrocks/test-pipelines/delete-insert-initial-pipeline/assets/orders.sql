/* @bruin
name: bruin_test.delete_insert_orders
type: starrocks.sql

materialization:
  type: table
  strategy: delete+insert
  incremental_key: order_id
@bruin */

SELECT 1 AS order_id, 'kept' AS order_status
UNION ALL
SELECT 2 AS order_id, 'will-update' AS order_status
