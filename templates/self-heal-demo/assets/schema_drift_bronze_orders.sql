/* @bruin
name: schema_drift.bronze_orders
description: Bronze orders after an upstream column rename from amount to gross_amount.
depends:
  - source.orders
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
  - name: gross_amount
    type: DOUBLE
  - name: status
    type: VARCHAR
@bruin */

SELECT
    order_id,
    user_id,
    transaction_date,
    amount AS gross_amount,
    status
FROM source.orders;
