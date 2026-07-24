/* @bruin
name: local.bruin_auto.widget
type: spark.sql

materialization:
  type: table
  strategy: create+replace
@bruin */

SELECT 1 AS widget_id, 'gizmo' AS widget_name
