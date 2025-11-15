/* @bruin
name: duckdb.sales_per_customer
type: duckdb.sql

materialization:
  type: table

depends:
  - oracle_raw.customers
  - oracle_raw.orders
  - oracle_raw.order_items

columns:
  - name: customer_id
    type: integer
    description: "Customer identifier from Oracle"
  - name: full_name
    type: varchar
    description: "Customer full name for reporting"
  - name: total_revenue
    type: decimal(18,2)
    description: "Total revenue captured from Oracle order items"
  - name: order_count
    type: integer
    description: "Number of closed orders per customer"
  - name: last_order_date
    type: timestamp
    description: "Most recent order timestamp"
@bruin */

WITH line_items AS (
    SELECT
        oi.order_id,
        oi.quantity,
        oi.unit_price,
        oi.quantity * oi.unit_price AS line_revenue
    FROM oracle_raw.order_items oi
),
order_rollup AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.ordered_at,
        SUM(li.line_revenue) AS order_revenue
    FROM oracle_raw.orders o
    LEFT JOIN line_items li
        ON li.order_id = o.order_id
    GROUP BY 1,2,3
)
SELECT
    c.customer_id,
    c.full_name,
    COALESCE(SUM(orx.order_revenue), 0) AS total_revenue,
    COUNT(DISTINCT orx.order_id) AS order_count,
    MAX(orx.ordered_at) AS last_order_date
FROM oracle_raw.customers c
LEFT JOIN order_rollup orx
    ON orx.customer_id = c.customer_id
GROUP BY 1,2;
