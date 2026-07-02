/* @bruin

name: gold.pipeline_kpis
type: sf.sql
description: |
  Executive KPI rollup for the Credit Union Salesforce demo dashboard.
connection: snowflake-default
tags:
  - finance
  - credit_union
  - crm
  - gold
  - dashboard
domains:
  - crm
  - lending
meta:
  asset_grain: One KPI row per metric key.
  load_pattern: View over current silver marts.
  refresh_cadence: Daily batch pipeline.

materialization:
  type: view

depends:
  - silver.salesforce_account_health
  - silver.salesforce_opportunity_pipeline

columns:
  - name: metric_key
    type: VARCHAR
    description: Stable metric identifier.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: unique
  - name: metric_value
    type: DOUBLE
    description: Numeric metric value.
    checks:
      - name: non_negative
  - name: metric_label
    type: VARCHAR
    description: Display label for the KPI.

@bruin */

WITH account_health AS (
    SELECT *
    FROM silver.salesforce_account_health
),

pipeline AS (
    SELECT *
    FROM silver.salesforce_opportunity_pipeline
)

SELECT 'accounts' AS metric_key, COUNT(*) AS metric_value, 'CRM accounts' AS metric_label
FROM account_health

UNION ALL

SELECT 'open_pipeline_amount_usd', SUM(open_pipeline_amount_usd), 'Open pipeline'
FROM account_health

UNION ALL

SELECT 'weighted_open_pipeline_amount_usd', SUM(weighted_open_pipeline_amount_usd), 'Weighted open pipeline'
FROM account_health

UNION ALL

SELECT 'activity_coverage_pct', COUNT_IF(activity_count > 0) / NULLIF(COUNT(*), 0), 'Average approved loan APR'
FROM pipeline
