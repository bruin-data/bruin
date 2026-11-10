/* @bruin
name: no_delay
type: duckdb.sql
rerun_cooldown: -1
materialization:
  type: table
@bruin */

-- This asset should have no retry delay
SELECT 3 as id, 'no_delay' as value