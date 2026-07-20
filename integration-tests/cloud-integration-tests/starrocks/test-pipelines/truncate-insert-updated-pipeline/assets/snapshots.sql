/* @bruin
name: bruin_test.truncate_insert_snapshots
type: starrocks.sql

materialization:
  type: table
  strategy: truncate+insert
@bruin */

SELECT 10 AS snapshot_id, 'replacement-one' AS snapshot_name
UNION ALL
SELECT 11 AS snapshot_id, 'replacement-two' AS snapshot_name
