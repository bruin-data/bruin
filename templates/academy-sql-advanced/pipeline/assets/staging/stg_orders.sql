/* @bruin
name: stg_orders
type: duckdb.sql
description: "Latest non-null-status order record, with canonical status and store-local time."
depends:
  - orders
  - stores
materialization:
  type: table
  strategy: create+replace
columns:
  - name: order_id
    type: integer
  - name: customer_id
    type: integer
  - name: store_id
    type: integer
  - name: ordered_at
    type: timestamp
  - name: ordered_at_utc
    type: timestamptz
  - name: _loaded_at
    type: timestamp
  - name: currency_code
    type: varchar
  - name: order_status
    type: varchar
@bruin */

WITH ranked AS (
    SELECT o.*, ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY o._loaded_at DESC) AS rn
    FROM orders AS o
    WHERE o.order_status IS NOT NULL
), latest AS (
    SELECT r.*, s.timezone
    FROM ranked AS r
    LEFT JOIN stores AS s ON r.store_id = s.store_id
    WHERE r.rn = 1
)
SELECT
    order_id,
    customer_id,
    store_id,
    ordered_at,
    COALESCE(ordered_at_utc, ordered_at AT TIME ZONE timezone) AS ordered_at_utc,
    _loaded_at,
    currency_code,
    CASE WHEN lower(trim(order_status)) = 'complete' THEN 'completed'
         ELSE lower(trim(order_status)) END AS order_status
FROM latest
ORDER BY order_id;
