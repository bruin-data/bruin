/* @bruin
name: local.bruin_test.scd2_time_inventory
type: spark.sql

materialization:
  type: table
  strategy: scd2_by_time
  incremental_key: changed_at
  partition_by: days(changed_at)
  cluster_by:
    - warehouse
    - item_id

columns:
  - name: item_id
    type: INT
    primary_key: true
  - name: warehouse
    type: STRING
  - name: quantity
    type: INT
  - name: changed_at
    type: TIMESTAMP
@bruin */

SELECT 1 AS item_id, 'east' AS warehouse, 10 AS quantity, TIMESTAMP '2026-03-01 00:00:00' AS changed_at
UNION ALL
SELECT 2 AS item_id, 'west' AS warehouse, 20 AS quantity, TIMESTAMP '2026-03-01 00:00:00' AS changed_at
UNION ALL
SELECT 3 AS item_id, 'east' AS warehouse, 30 AS quantity, TIMESTAMP '2026-03-01 00:00:00' AS changed_at
