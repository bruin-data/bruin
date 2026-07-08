/* @bruin
name: schema_drift.silver_orders
description: Silver orders that still references amount after the upstream branch renamed it to gross_amount.
depends:
  - schema_drift.bronze_orders
tags:
  - self-heal-demo
  - schema-drift-check
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
    orders.amount AS amount,
    status
FROM schema_drift.bronze_orders AS orders;
