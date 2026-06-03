/* @bruin
name: bruin_test.staging
type: flight.sql

materialization:
  type: table
  strategy: truncate+insert
@bruin */

SELECT 1 AS id, 'a' AS label
UNION ALL
SELECT 2 AS id, 'b' AS label
UNION ALL
SELECT 3 AS id, 'c' AS label
UNION ALL
SELECT 4 AS id, 'd' AS label
