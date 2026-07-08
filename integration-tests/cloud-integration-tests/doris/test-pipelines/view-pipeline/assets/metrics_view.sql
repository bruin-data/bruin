/* @bruin
name: bruin_test.metrics_view
type: doris.sql

materialization:
  type: view

depends:
  - bruin_test.metrics_src
@bruin */

SELECT metric_name, value
FROM `bruin_test`.`metrics_src`
WHERE value > 6
