/* @bruin

name: postgres_order_daily_monitor
type: clickhouse.sql
description: Demonstrates a dependent ClickHouse view over an Ingestr target. The view stores no data of its own, remains current with its upstream table, and makes the PostgreSQL-to-ClickHouse lineage visible to Bruin.
tags:
  - layer:serving
  - domain:commerce
  - source:postgres
  - requires-postgres-default
domains:
  - commerce
meta:
  consumer: data-observability
  data_classification: internal

materialization:
  type: view

depends:
  - raw_postgres_orders
owner: data-foundations@example.com

columns:
  - name: order_date
    type: Date
    description: Date on which source orders were created.
  - name: order_count
    type: UInt64
    description: Orders ingested for the date.
  - name: newest_source_update
    type: DateTime
    description: Most recent source update timestamp loaded for the date.

@bruin */

SELECT
    order_date,
    count() AS order_count,
    max(updated_at) AS newest_source_update
FROM raw_postgres_orders
GROUP BY order_date
