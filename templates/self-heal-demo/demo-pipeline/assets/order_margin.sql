/* @bruin
name: finance.order_margin
description: Order-level revenue after post-checkout adjustments.
depends:
  - staging.orders
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
  - name: customer_id
    type: INTEGER
  - name: order_date
    type: DATE
  - name: gross_amount
    type: DOUBLE
    checks:
      - name: positive
  - name: adjustment_amount
    type: DOUBLE
  - name: net_amount
    type: DOUBLE
    checks:
      - name: positive
@bruin */

WITH adjustment_totals AS (
    SELECT
        customer_id,
        SUM(adjustment_amount) AS adjustment_amount
    FROM raw.order_adjustments
    WHERE adjustment_type IN ('refund', 'promotion', 'service_credit')
    GROUP BY 1
)

SELECT
    orders.order_id,
    orders.customer_id,
    orders.order_date,
    orders.gross_amount,
    COALESCE(adjustment_totals.adjustment_amount, 0) AS adjustment_amount,
    orders.gross_amount + COALESCE(adjustment_totals.adjustment_amount, 0) AS net_amount
FROM staging.orders AS orders
LEFT JOIN adjustment_totals
    ON orders.customer_id = adjustment_totals.customer_id;
