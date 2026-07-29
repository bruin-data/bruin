/* @bruin

name: country_revenue
type: clickhouse.sql
description: Demonstrates a governed SQL mart with create+replace materialization. It combines SQL, Python, and CSV-seed dependencies, exposes their lineage, and pairs declared keys and column checks with custom checks and a mocked SQL unit test.
tags:
  - layer:mart
  - domain:commerce
  - critical
domains:
  - commerce
meta:
  business_owner: vp-commerce@example.com
  data_classification: internal
  freshness_sla: daily
  metric_definition: paid-order-revenue

materialization:
  type: table
  strategy: create+replace

depends:
  - customer_order_summary
  - country_targets
  - customer_regions
owner: commerce-analytics@example.com

columns:
  - name: country
    type: String
    description: Customer country and grain of the country revenue mart.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: sales_region
    type: LowCardinality(String)
    description: Commercial sales region supplied by the Python asset.
    checks:
      - name: not_null
  - name: monthly_revenue_target
    type: Float64
    description: Monthly paid-revenue target for the country in USD.
    checks:
      - name: positive
  - name: customer_count
    type: UInt64
    description: Number of customers represented in the country.
    checks:
      - name: positive
  - name: paid_order_count
    type: UInt64
    description: Paid orders attributed to the country.
    checks:
      - name: non_negative
  - name: total_paid_amount
    type: Float64
    description: Paid revenue in USD.
    checks:
      - name: non_negative
  - name: average_paid_amount_per_customer
    type: Float64
    description: Paid revenue divided by customer count.
    checks:
      - name: non_negative
  - name: target_attainment_ratio
    type: Float64
    description: Paid revenue divided by the monthly country target.
    checks:
      - name: non_negative

custom_checks:
  - name: country revenue is populated
    description: The demo model should always produce at least one country.
    value: 1
    query: |
      SELECT count() > 0
      FROM country_revenue
  - name: country target coverage
    description: Every country in the customer summary must be represented in the governed target seed.
    value: 0
    blocking: false
    query: |
      SELECT count()
      FROM customer_order_summary AS summary
      LEFT JOIN country_targets AS targets
        ON summary.country = targets.country
      WHERE targets.country IS NULL
unit_tests:
  - name: calculates_country_target_attainment
    inputs:
      - asset: customer_order_summary
        rows:
          - country: United Kingdom
            paid_order_count: 2
            total_paid_amount: 300
      - asset: country_targets
        rows:
          - country: United Kingdom
            monthly_revenue_target: 500
      - asset: customer_regions
        rows:
          - country: United Kingdom
            sales_region: EMEA
    expected:
      rows:
        - average_paid_amount_per_customer: 300
          country: United Kingdom
          customer_count: 1
          monthly_revenue_target: 500
          paid_order_count: 2
          sales_region: EMEA
          target_attainment_ratio: 0.6
          total_paid_amount: 300
      match: exact

@bruin */

SELECT
    summary.country AS country,
    regions.sales_region AS sales_region,
    targets.monthly_revenue_target AS monthly_revenue_target,
    summary.customer_count,
    summary.paid_order_count,
    summary.total_paid_amount,
    round(
        summary.total_paid_amount / nullIf(summary.customer_count, 0),
        2
    ) AS average_paid_amount_per_customer,
    round(
        summary.total_paid_amount / nullIf(targets.monthly_revenue_target, 0),
        4
    ) AS target_attainment_ratio
FROM (
    SELECT
        country,
        count() AS customer_count,
        sum(paid_order_count) AS paid_order_count,
        sum(total_paid_amount) AS total_paid_amount
    FROM customer_order_summary
    GROUP BY country
) AS summary
INNER JOIN country_targets AS targets
    ON summary.country = targets.country
INNER JOIN customer_regions AS regions
    ON summary.country = regions.country
