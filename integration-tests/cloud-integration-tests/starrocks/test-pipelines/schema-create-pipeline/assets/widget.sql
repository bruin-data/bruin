/* @bruin
name: bruin_auto.widget
type: starrocks.sql

materialization:
  type: table
  strategy: create+replace
@bruin */

SELECT 1 AS widget_id, 'gizmo' AS widget_name
