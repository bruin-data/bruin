/* @bruin

name: gold.activity_coverage_by_product
type: sf.sql
description: |
  Product-family activity coverage rollup for the Credit Union Salesforce demo.
connection: snowflake-default
tags:
  - finance
  - credit_union
  - crm
  - lending
  - gold
  - dashboard
domains:
  - crm
  - lending
meta:
  asset_grain: One row per derived product family.
  load_pattern: View over current silver opportunity mart.
  refresh_cadence: Daily batch pipeline.

materialization:
  type: view

depends:
  - silver.salesforce_opportunity_pipeline

columns:
  - name: product_family
    type: VARCHAR
    description: Derived credit union product family.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: unique
  - name: opportunity_count
    type: INTEGER
    description: Number of opportunities in the product family.
    checks:
      - name: non_negative
  - name: amount_usd
    type: DOUBLE
    description: Total opportunity amount in USD.
    checks:
      - name: non_negative
  - name: activity_coverage_pct
    type: DOUBLE
    description: Share of opportunities with at least one linked task.
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: completed_activity_per_opportunity
    type: DOUBLE
    description: Average completed task count per opportunity.
    checks:
      - name: non_negative

@bruin */

SELECT
    product_family,
    COUNT(*) AS opportunity_count,
    SUM(amount_usd) AS amount_usd,
    COUNT_IF(activity_count > 0) / NULLIF(COUNT(*), 0) AS activity_coverage_pct,
    AVG(completed_activity_count) AS completed_activity_per_opportunity
FROM silver.salesforce_opportunity_pipeline
GROUP BY 1
ORDER BY amount_usd DESC, product_family
