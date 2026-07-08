/* @bruin
name: orders.status_snapshot
description: Current order status snapshot for customer operations.
depends:
  - staging.orders
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
  - name: customer_id
    type: INTEGER
  - name: order_date
    type: DATE
  - name: lifecycle_status
    type: VARCHAR
  - name: status_updated_at
    type: TIMESTAMP
@bruin */

WITH status_events AS (
    SELECT
        order_id,
        status,
        status_updated_at,
        is_current
    FROM raw.order_status_history
)

SELECT
    orders.order_id,
    orders.customer_id,
    orders.order_date,
    status_events.status AS lifecycle_status,
    status_events.status_updated_at
FROM staging.orders AS orders
LEFT JOIN status_events
    ON orders.order_id = status_events.order_id;
