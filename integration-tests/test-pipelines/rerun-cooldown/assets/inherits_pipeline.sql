/* @bruin
name: inherits_pipeline
type: duckdb.sql
materialization:
  type: table
@bruin */

-- This asset should inherit rerun_cooldown from pipeline
SELECT 2 as id, 'inherits' as value