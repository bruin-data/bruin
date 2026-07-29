/* @bruin

name: pipeline_daily_snapshot
type: clickhouse.sql
description: Demonstrates truncate+insert materialization for a current-state operational snapshot. Every execution clears the prior target contents and inserts a single freshly computed result, rather than retaining historical versions.
tags:
  - layer:operations
  - domain:commerce
  - strategy:truncate-insert
domains:
  - commerce
meta:
  data_classification: internal
  freshness_sla: daily

materialization:
  type: table
  strategy: truncate+insert

depends:
  - country_revenue
owner: data-foundations@example.com

columns:
  - name: snapshot_date
    type: Date
    description: Business date represented by this pipeline snapshot.
    primary_key: true
    checks:
      - name: not_null
  - name: country_count
    type: UInt64
    description: Countries present in the country revenue mart.
    checks:
      - name: positive
  - name: total_paid_amount
    type: Float64
    description: Paid revenue across all countries.
    checks:
      - name: non_negative
  - name: captured_at
    type: DateTime
    description: Timestamp at which the snapshot was generated.

@bruin */

SELECT
    toDate('{{ end_date }}') AS snapshot_date,
    count() AS country_count,
    sum(total_paid_amount) AS total_paid_amount,
    now('UTC') AS captured_at
FROM country_revenue
