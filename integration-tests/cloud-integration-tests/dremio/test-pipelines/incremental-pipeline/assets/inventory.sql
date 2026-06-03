/* @bruin
name: bruin_test.inventory
type: flight.sql

materialization:
  type: table
  strategy: delete+insert
  incremental_key: sku
@bruin */

SELECT 'A1' AS sku, 100 AS quantity
UNION ALL
SELECT 'B2' AS sku, 200 AS quantity
UNION ALL
SELECT 'C3' AS sku, 300 AS quantity
