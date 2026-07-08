/* @bruin
name: duplicate.silver_orders
description: Silver orders with an intentional duplicated order for duplicate investigation.
depends:
  - source.orders
tags:
  - self-heal-demo
  - duplicate-investigate
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

WITH typed_orders AS (
    SELECT
        order_id,
        user_id,
        transaction_date,
        amount,
        status
    FROM source.orders
    WHERE status <> 'cancelled'
)

SELECT * FROM typed_orders
UNION ALL
SELECT * FROM typed_orders WHERE order_id = 1002;
