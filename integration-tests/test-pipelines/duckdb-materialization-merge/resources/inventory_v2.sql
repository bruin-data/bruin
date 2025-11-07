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
    update_on_merge: true
  - name: stock
    type: INTEGER
  - name: unit_price
    type: DOUBLE
    update_on_merge: false
@bruin */

SELECT 1 AS item_id, 'Widget Pro' AS item_name, 100 AS stock, 1239.99 AS unit_price
UNION ALL
SELECT 2 AS item_id, 'Gadget' AS item_name, 200 AS stock, 749.99 AS unit_price
UNION ALL
SELECT 4 AS item_id, 'Accessory' AS item_name, 250 AS stock, 249.99 AS unit_price
