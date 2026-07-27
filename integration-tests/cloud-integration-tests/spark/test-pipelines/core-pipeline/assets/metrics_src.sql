/* @bruin
name: spark_catalog.bruin_view.metrics_src
type: spark.sql

materialization:
  type: table
  strategy: create+replace
@bruin */

SELECT 1 AS metric_id, 'visits' AS metric_name, 10 AS value
UNION ALL
SELECT 2 AS metric_id, 'signups' AS metric_name, 5 AS value
