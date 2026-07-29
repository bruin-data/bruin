/* @bruin
name: local.bruin_test.delete_insert_orders
type: spark.sql

materialization:
  type: table
  strategy: delete+insert
  incremental_key: order_id
@bruin */

SELECT 2 AS order_id, 'updated' AS order_status
UNION ALL
SELECT 3 AS order_id, 'new' AS order_status
