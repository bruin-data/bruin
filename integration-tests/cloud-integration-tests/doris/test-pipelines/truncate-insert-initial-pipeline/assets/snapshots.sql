/* @bruin
name: bruin_test.truncate_insert_snapshots
type: doris.sql

materialization:
  type: table
  strategy: truncate+insert
@bruin */

SELECT 1 AS snapshot_id, 'old-one' AS snapshot_name
UNION ALL
SELECT 2 AS snapshot_id, 'old-two' AS snapshot_name
