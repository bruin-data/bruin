/* @bruin

name: customer_order_summary
type: clickhouse.sql
description: Demonstrates delete+insert incremental materialization. For each run, Bruin uses customer_id as the incremental key to delete the affected target slice before inserting its recomputed replacement, while checks protect the intended one-row-per-key grain.
tags:
  - layer:mart
  - domain:commerce
  - strategy:delete-insert
domains:
  - commerce
meta:
  business_owner: commerce-analytics@example.com
  data_classification: confidential
  freshness_sla: daily

materialization:
  type: table
  strategy: delete+insert
  incremental_key: customer_id

depends:
  - raw_customers
  - daily_order_snapshot
owner: commerce-analytics@example.com

columns:
  - name: customer_id
    type: UInt64
    description: Customer identifier and grain of this mart.
    primary_key: true
    checks:
      - name: not_null
      - name: positive
      - name: unique
  - name: customer_name
    type: String
    description: Customer display name; carried forward for analyst convenience.
    tags:
      - pii
      - classification:confidential
    owner: data-governance@example.com
    checks:
      - name: not_null
  - name: country
    type: String
    description: Customer country.
    checks:
      - name: not_null
  - name: lifecycle_stage
    type: LowCardinality(String)
    description: Customer lifecycle segment from the customer dimension.
  - name: order_count
    type: UInt64
    description: All customer orders, including refunds.
    checks:
      - name: non_negative
  - name: paid_order_count
    type: UInt64
    description: Orders with a paid status.
    checks:
      - name: non_negative
  - name: total_paid_amount
    type: Float64
    description: Paid order revenue in USD.
    checks:
      - name: non_negative
  - name: first_order_date
    type: Nullable(Date)
    description: First observed order date.
  - name: latest_order_date
    type: Nullable(Date)
    description: Most recent observed order date.

custom_checks:
  - name: customer summary has one row per customer
    description: The delete+insert result must preserve the declared customer grain.
    value: 1
    query: |-
      SELECT count() = countDistinct(customer_id)
      FROM customer_order_summary

@bruin */

SELECT
    c.customer_id,
    c.customer_name,
    c.country,
    c.lifecycle_stage,
    count(o.order_id) AS order_count,
    countIf(o.order_status = 'paid') AS paid_order_count,
    sumIf(o.amount, o.order_status = 'paid') AS total_paid_amount,
    min(o.order_date) AS first_order_date,
    max(o.order_date) AS latest_order_date
FROM raw_customers AS c
LEFT JOIN daily_order_snapshot AS o
    ON c.customer_id = o.customer_id
GROUP BY
    c.customer_id,
    c.customer_name,
    c.country,
    c.lifecycle_stage
