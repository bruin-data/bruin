/* @bruin

name: order_events_contract
type: clickhouse.sql
description: Demonstrates schema-first ClickHouse DDL. The ddl strategy creates the declared table without a SELECT query; partition_by becomes the physical partition key, while the ordered primary-key columns define ClickHouse's composite sorting key.
tags:
  - layer:contract
  - domain:commerce
  - strategy:ddl
  - layout:partitioned
  - layout:sorted
domains:
  - commerce
meta:
  data_classification: internal
  physical_layout: partition-by-event-date-and-order-by-customer-received-event
  schema_contract: explicit

materialization:
  type: table
  strategy: ddl
  partition_by: event_date
owner: data-platform@example.com

columns:
  - name: event_date
    type: Date
    description: Date used as the ClickHouse partition key.
  - name: customer_id
    type: UInt64
    description: Customer associated with the event and the first sorting key.
    primary_key: true
  - name: received_at
    type: DateTime64(3, 'UTC')
    description: Ingestion timestamp and the second sorting key.
    primary_key: true
  - name: event_id
    type: UInt64
    description: Unique identifier for the event and the final sorting key.
    primary_key: true
  - name: event_type
    type: LowCardinality(String)
    description: Business event type.
  - name: payload
    type: Nullable(String)
    description: Optional serialized event payload.

@bruin */
