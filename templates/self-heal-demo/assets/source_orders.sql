/* @bruin
name: source.orders
description: Clean source orders shared by the self-heal demo scenarios.
tags:
  - self-heal-demo
  - duplicate-investigate
  - quality-check-investigate
  - freshness-check
  - schema-drift-check
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
    checks:
      - name: not_null
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
@bruin */

SELECT 1001 AS order_id, 501 AS user_id, DATE '2025-01-01' AS transaction_date, 25.00 AS amount, 'paid' AS status
UNION ALL
SELECT 1002 AS order_id, 502 AS user_id, DATE '2025-01-01' AS transaction_date, 35.00 AS amount, 'paid' AS status
UNION ALL
SELECT 1003 AS order_id, 503 AS user_id, DATE '2025-01-02' AS transaction_date, 40.00 AS amount, 'paid' AS status
UNION ALL
SELECT 1004 AS order_id, 504 AS user_id, DATE '2025-01-03' AS transaction_date, 50.00 AS amount, 'paid' AS status;
