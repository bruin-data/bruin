/* @bruin
name: staging.orders
description: Clean order headers shared by the self-heal demo marts.
tags:
  - self-heal-demo
  - duplicate-investigate
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
  - name: customer_id
    type: INTEGER
    checks:
      - name: not_null
  - name: order_date
    type: DATE
  - name: product_id
    type: VARCHAR
  - name: gross_amount
    type: DOUBLE
    checks:
      - name: positive
  - name: currency
    type: VARCHAR
  - name: order_status
    type: VARCHAR
@bruin */

SELECT
    order_id,
    customer_id,
    order_date,
    product_id,
    amount AS gross_amount,
    currency,
    order_status
FROM raw.orders
WHERE order_status <> 'cancelled';
