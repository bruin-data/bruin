/* @bruin
name: spark_catalog.bruin_view.metrics_view
type: spark.sql

materialization:
  type: view

depends:
  - spark_catalog.bruin_view.metrics_src
@bruin */

SELECT metric_name, value
FROM spark_catalog.bruin_view.metrics_src
WHERE value > 6
