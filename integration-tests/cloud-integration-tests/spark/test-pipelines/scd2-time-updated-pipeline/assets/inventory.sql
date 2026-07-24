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

SELECT 1 AS item_id, 'east' AS warehouse, 15 AS quantity, TIMESTAMP '2026-04-01 00:00:00' AS changed_at
UNION ALL
SELECT 2 AS item_id, 'west' AS warehouse, 999 AS quantity, TIMESTAMP '2026-02-01 00:00:00' AS changed_at
UNION ALL
SELECT 4 AS item_id, 'north' AS warehouse, 40 AS quantity, TIMESTAMP '2026-04-01 00:00:00' AS changed_at
