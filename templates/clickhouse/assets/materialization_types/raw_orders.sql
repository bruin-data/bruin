/* @bruin

name: raw_orders
type: clickhouse.sql
description: Demonstrates create+replace table materialization from deterministic inline SQL. It provides a governed upstream dependency with a declared key, typed columns, and built-in quality checks that downstream incremental-strategy examples can consume.
tags:
  - layer:raw
  - domain:commerce
  - source:demo
domains:
  - commerce
meta:
  data_classification: internal
  freshness_sla: daily
  source_system: deterministic-demo

materialization:
  type: table
  strategy: create+replace
owner: data-foundations@example.com

columns:
  - name: order_id
    type: UInt64
    description: Unique order identifier.
    primary_key: true
    checks:
      - name: not_null
      - name: positive
      - name: unique
  - name: customer_id
    type: UInt64
    description: Customer who placed the order.
    checks:
      - name: not_null
      - name: positive
  - name: order_date
    type: Date
    description: Date the order was placed.
    checks:
      - name: not_null
  - name: order_status
    type: LowCardinality(String)
    description: Current order status.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - paid
          - refunded
  - name: amount
    type: Float64
    description: Order amount in USD.
    checks:
      - name: non_negative
  - name: updated_at
    type: DateTime
    description: Timestamp at which the source order was last updated.
    checks:
      - name: not_null

@bruin */

SELECT
    toUInt64(1001) AS order_id,
    toUInt64(1) AS customer_id,
    toDate('2024-04-01') AS order_date,
    'paid' AS order_status,
    CAST(129.50 AS Float64) AS amount,
    toDateTime('2024-04-01 08:15:00', 'UTC') AS updated_at
UNION ALL
SELECT
    toUInt64(1002) AS order_id,
    toUInt64(2) AS customer_id,
    toDate('2024-04-02') AS order_date,
    'paid' AS order_status,
    CAST(320.00 AS Float64) AS amount,
    toDateTime('2024-04-02 09:30:00', 'UTC') AS updated_at
UNION ALL
SELECT
    toUInt64(1003) AS order_id,
    toUInt64(2) AS customer_id,
    toDate('2024-04-05') AS order_date,
    'refunded' AS order_status,
    CAST(45.00 AS Float64) AS amount,
    toDateTime('2024-04-06 11:00:00', 'UTC') AS updated_at
UNION ALL
SELECT
    toUInt64(1004) AS order_id,
    toUInt64(3) AS customer_id,
    toDate('2024-04-07') AS order_date,
    'paid' AS order_status,
    CAST(210.25 AS Float64) AS amount,
    toDateTime('2024-04-07 13:45:00', 'UTC') AS updated_at
UNION ALL
SELECT
    toUInt64(1005) AS order_id,
    toUInt64(4) AS customer_id,
    toDate('2024-04-09') AS order_date,
    'paid' AS order_status,
    CAST(80.00 AS Float64) AS amount,
    toDateTime('2024-04-09 14:20:00', 'UTC') AS updated_at
UNION ALL
SELECT
    toUInt64(1006) AS order_id,
    toUInt64(3) AS customer_id,
    toDate('2024-04-11') AS order_date,
    'paid' AS order_status,
    CAST(90.75 AS Float64) AS amount,
    toDateTime('2024-04-11 16:05:00', 'UTC') AS updated_at
UNION ALL
SELECT
    toUInt64(1007) AS order_id,
    toUInt64(5) AS customer_id,
    toDate('2024-04-12') AS order_date,
    'paid' AS order_status,
    CAST(55.50 AS Float64) AS amount,
    toDateTime('2024-04-12 17:40:00', 'UTC') AS updated_at
UNION ALL
SELECT
    toUInt64(1008) AS order_id,
    toUInt64(5) AS customer_id,
    toDate('2024-04-20') AS order_date,
    'paid' AS order_status,
    CAST(150.00 AS Float64) AS amount,
    toDateTime('2024-04-20 10:30:00', 'UTC') AS updated_at
