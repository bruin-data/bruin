/* @bruin
name: quality.gold_order_report
description: Gold report that fails the positive amount check for order_id 1003.
depends:
  - quality.silver_orders
tags:
  - self-heal-demo
  - quality-check-investigate
materialization:
  type: table
columns:
  - name: order_id
    type: INTEGER
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: user_id
    type: INTEGER
  - name: transaction_date
    type: DATE
  - name: amount
    type: DOUBLE
    checks:
      - name: positive
  - name: status
    type: VARCHAR
@bruin */

SELECT
    order_id,
    user_id,
    transaction_date,
    amount,
    status
FROM quality.silver_orders;
