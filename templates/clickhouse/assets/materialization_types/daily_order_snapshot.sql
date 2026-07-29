/* @bruin

name: daily_order_snapshot
type: clickhouse.sql
description: Demonstrates time_interval materialization. Bruin renders the requested run window into the SQL and replaces only rows selected by order_date for that interval, making partition-like date backfills and recurring incremental runs explicit.
tags:
  - layer:staging
  - domain:commerce
  - strategy:time-interval
domains:
  - commerce
meta:
  data_classification: internal
  incremental_contract: order_date-between-start-and-end-date

materialization:
  type: table
  strategy: time_interval
  incremental_key: order_date
  time_granularity: date

depends:
  - raw_orders
owner: data-foundations@example.com

columns:
  - name: order_id
    type: UInt64
    description: Unique order identifier.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: customer_id
    type: UInt64
    description: Customer who placed the order.
    checks:
      - name: not_null
  - name: order_date
    type: Date
    description: Date used to select and replace the requested run interval.
    checks:
      - name: not_null
  - name: order_status
    type: LowCardinality(String)
    description: Current order status.
  - name: amount
    type: Float64
    description: Order amount in USD.
    checks:
      - name: non_negative
  - name: updated_at
    type: DateTime
    description: Timestamp when the order was last updated.

@bruin */

SELECT
    order_id,
    customer_id,
    order_date,
    order_status,
    amount,
    updated_at
FROM raw_orders
WHERE order_date BETWEEN toDate('{{ start_date }}') AND toDate('{{ end_date }}')
