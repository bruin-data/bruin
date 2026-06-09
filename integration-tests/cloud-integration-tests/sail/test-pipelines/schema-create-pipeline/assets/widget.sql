/* @bruin
name: bruin_auto.widget
type: sail.sql

materialization:
  type: table
  strategy: create+replace
@bruin */

-- bruin_auto does not exist on a fresh Sail server; the run must create the
-- schema (via CREATE SCHEMA IF NOT EXISTS) before materializing this table.
SELECT 1 AS widget_id, 'gizmo' AS widget_name
