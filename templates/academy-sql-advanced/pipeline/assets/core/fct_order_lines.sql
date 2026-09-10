/* @bruin
name: fct_order_lines
type: duckdb.sql
description: "One row per distinct order line for the latest usable order record."
depends:
  - stg_orders
  - stg_order_items
  - stg_products
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
    foreign_key:
      table: products
      column: product_id
    checks:
      - name: relationships
  - name: customer_id
    type: integer
  - name: ordered_at
    type: timestamp
  - name: ordered_at_utc
    type: timestamptz
  - name: _loaded_at
    type: timestamp
  - name: order_status
    type: varchar
  - name: category_name
    type: varchar
  - name: quantity
    type: integer
  - name: net_price
    type: decimal
  - name: line_revenue
    type: decimal
@bruin */

SELECT
    i.order_id,
    i.line_number,
    i.product_id,
    o.customer_id,
    o.ordered_at,
    o.ordered_at_utc,
    o._loaded_at,
    o.order_status,
    COALESCE(p.category_name, 'Unknown') AS category_name,
    i.quantity,
    i.net_price,
    CAST(i.quantity * i.net_price AS DECIMAL(12, 2)) AS line_revenue
FROM stg_order_items AS i
INNER JOIN stg_orders AS o ON i.order_id = o.order_id
INNER JOIN stg_products AS p ON i.product_id = p.product_id
ORDER BY i.order_id, i.line_number;
