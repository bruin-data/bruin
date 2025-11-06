/* @bruin
name: test.inventory
type: duckdb.sql
materialization:
  type: table
  strategy: merge
columns:
  - name: item_id
    type: INTEGER
    primary_key: true
  - name: item_name
    type: VARCHAR
  - name: stock
    type: INTEGER
  - name: unit_price
    type: DOUBLE
@bruin */

SELECT 1 AS item_id, 'Widget' AS item_name, 100 AS stock, 1999.99 AS unit_price
UNION ALL
SELECT 2 AS item_id, 'Gadget' AS item_name, 50 AS stock, 799.99 AS unit_price
UNION ALL
SELECT 3 AS item_id, 'Tool' AS item_name, 25 AS stock, 299.99 AS unit_price
