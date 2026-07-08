/* @bruin
name: duplicate.gold_order_report
description: Gold report that fails uniqueness because the duplicate scenario silver orders repeat order_id 1002.
depends:
  - duplicate.silver_orders
tags:
  - self-heal-demo
  - duplicate-investigate
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
  - name: status
    type: VARCHAR
@bruin */

SELECT
    order_id,
    user_id,
    transaction_date,
    amount,
    status
FROM duplicate.silver_orders;
