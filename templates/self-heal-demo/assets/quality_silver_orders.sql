/* @bruin
name: quality.silver_orders
description: Silver orders with an intentional negative amount for quality check investigation.
depends:
  - source.orders
tags:
  - self-heal-demo
  - quality-check-investigate
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
    CASE WHEN order_id = 1003 THEN -amount ELSE amount END AS amount,
    status
FROM source.orders
WHERE status <> 'cancelled';
