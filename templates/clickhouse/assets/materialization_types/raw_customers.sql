/* @bruin

name: raw_customers
type: clickhouse.sql
description: Demonstrates create+replace table materialization from deterministic inline SQL. Each run rebuilds the complete target, while the declared primary key, checks, column ownership, sensitivity tags, retention metadata, and asset owner show governance at multiple levels.
tags:
  - layer:raw
  - domain:commerce
  - source:demo
domains:
  - commerce
meta:
  data_classification: confidential
  data_steward: data-governance@example.com
  freshness_sla: daily
  source_system: deterministic-demo

materialization:
  type: table
  strategy: create+replace
owner: data-foundations@example.com

columns:
  - name: customer_id
    type: UInt64
    description: Unique customer identifier.
    tags:
      - identifier
    primary_key: true
    owner: data-foundations@example.com
    checks:
      - name: not_null
      - name: positive
      - name: unique
  - name: customer_name
    type: String
    description: Customer display name; classified as confidential.
    tags:
      - pii
      - classification:confidential
    owner: data-governance@example.com
    meta:
      retention: 365 days
    checks:
      - name: not_null
  - name: country
    type: String
    description: Customer country.
    checks:
      - name: not_null
  - name: signup_date
    type: Date
    description: Date the customer signed up.
  - name: lifecycle_stage
    type: LowCardinality(String)
    description: Simple customer lifecycle segment.
    checks:
      - name: accepted_values
        value:
          - enterprise
          - growth
          - self_serve

@bruin */

SELECT
    1 AS customer_id,
    'Ada Lovelace' AS customer_name,
    'United Kingdom' AS country,
    toDate('2024-01-15') AS signup_date,
    'enterprise' AS lifecycle_stage
UNION ALL
SELECT
    2 AS customer_id,
    'Grace Hopper' AS customer_name,
    'United States' AS country,
    toDate('2024-02-03') AS signup_date,
    'enterprise' AS lifecycle_stage
UNION ALL
SELECT
    3 AS customer_id,
    'Katherine Johnson' AS customer_name,
    'United States' AS country,
    toDate('2024-02-20') AS signup_date,
    'growth' AS lifecycle_stage
UNION ALL
SELECT
    4 AS customer_id,
    'Hedy Lamarr' AS customer_name,
    'Austria' AS country,
    toDate('2024-03-11') AS signup_date,
    'growth' AS lifecycle_stage
UNION ALL
SELECT
    5 AS customer_id,
    'Mary Jackson' AS customer_name,
    'United States' AS country,
    toDate('2024-04-09') AS signup_date,
    'self_serve' AS lifecycle_stage
