/* @bruin

name: order_change_log
type: clickhouse.sql
description: Demonstrates append materialization. Each run inserts a new immutable batch without deleting existing rows, including when an interval is rerun; the asset therefore makes the duplicate-handling responsibility and load timestamp visible by design.
tags:
  - layer:staging
  - domain:commerce
  - strategy:append
domains:
  - commerce
meta:
  data_classification: internal
  deduplication: caller-responsibility
  incremental_contract: order-date-between-start-and-end-date

materialization:
  type: table
  strategy: append

depends:
  - raw_orders
owner: data-foundations@example.com

columns:
  - name: change_id
    type: String
    description: Stable identifier for an order version in the append-only log.
    primary_key: true
    checks:
      - name: not_null
  - name: order_id
    type: UInt64
    description: Order identifier associated with the change.
    checks:
      - name: not_null
  - name: customer_id
    type: UInt64
    description: Customer who placed the order.
  - name: order_date
    type: Date
    description: Order date used to select the extraction interval.
  - name: order_status
    type: LowCardinality(String)
    description: Order status at the time of the extract.
  - name: amount
    type: Float64
    description: Order amount in USD.
  - name: updated_at
    type: DateTime
    description: Source update timestamp.
  - name: loaded_at
    type: DateTime
    description: ClickHouse time at which the append batch was produced.

@bruin */

SELECT
    concat(toString(order_id), '-', toString(updated_at)) AS change_id,
    order_id,
    customer_id,
    order_date,
    order_status,
    amount,
    updated_at,
    now('UTC') AS loaded_at
FROM raw_orders
WHERE order_date BETWEEN toDate('{{ start_date }}') AND toDate('{{ end_date }}')
