/* @bruin
name: freshness.silver_orders
description: Silver orders with an intentional stale date filter for freshness investigation.
depends:
  - source.orders
tags:
  - self-heal-demo
  - freshness-check
materialization:
  type: table
columns:
  - name: order_id
    type: INTEGER
  - name: user_id
    type: INTEGER
  - name: transaction_date
    type: DATE
  - name: amount
    type: DOUBLE
  - name: status
    type: VARCHAR
@bruin */

SELECT
    order_id,
    user_id,
    transaction_date,
    amount,
    status
FROM source.orders
WHERE status <> 'cancelled'
  AND transaction_date < DATE '2025-01-03';
