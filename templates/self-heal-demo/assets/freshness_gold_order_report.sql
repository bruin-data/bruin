/* @bruin
name: freshness.gold_order_report
description: Gold report that fails because the latest expected partition is missing.
depends:
  - freshness.silver_orders
tags:
  - self-heal-demo
  - freshness-check
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
    checks:
      - name: not_null
  - name: amount
    type: DOUBLE
    checks:
      - name: positive
  - name: status
    type: VARCHAR
custom_checks:
  - name: latest partition exists
    query: SELECT CASE WHEN max(transaction_date) = DATE '2025-01-03' THEN 1 ELSE 0 END FROM freshness.gold_order_report
    value: 1
@bruin */

SELECT
    order_id,
    user_id,
    transaction_date,
    amount,
    status
FROM freshness.silver_orders;
