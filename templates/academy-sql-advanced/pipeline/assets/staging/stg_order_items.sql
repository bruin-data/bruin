/* @bruin
name: stg_order_items
type: duckdb.sql
description: "De-duplicated source order lines; orphan product keys remain visible for checks."
depends:
  - order_items
materialization:
  type: table
  strategy: create+replace
columns:
  - name: order_id
    type: integer
  - name: line_number
    type: integer
  - name: product_id
    type: integer
  - name: quantity
    type: integer
  - name: unit_price
    type: decimal
  - name: net_price
    type: decimal
  - name: unit_cost
    type: decimal
@bruin */

SELECT DISTINCT order_id, line_number, product_id, quantity, unit_price, net_price, unit_cost
FROM order_items
ORDER BY order_id, line_number;
