/* @bruin

name: gold.pipeline_by_stage
type: sf.sql
description: |
  Stage-level pipeline rollup for the Credit Union Salesforce demo dashboard.
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
  asset_grain: One row per derived pipeline stage group.
  load_pattern: View over current silver opportunity mart.
  refresh_cadence: Daily batch pipeline.

materialization:
  type: view

depends:
  - silver.salesforce_opportunity_pipeline

columns:
  - name: stage_group
    type: VARCHAR
    description: Derived funnel stage group.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: unique
  - name: opportunity_count
    type: INTEGER
    description: Number of opportunities in the stage group.
    checks:
      - name: non_negative
  - name: amount_usd
    type: DOUBLE
    description: Total opportunity amount in USD.
    checks:
      - name: non_negative
  - name: weighted_amount_usd
    type: DOUBLE
    description: Total probability-weighted opportunity amount in USD.
    checks:
      - name: non_negative
  - name: avg_probability_pct
    type: DOUBLE
    description: Average Salesforce probability percentage.
    checks:
      - name: min
        value: 0
      - name: max
        value: 100
  - name: completed_activity_count
    type: INTEGER
    description: Completed task count for opportunities in the stage group.
    checks:
      - name: non_negative

@bruin */

SELECT
    stage_group,
    COUNT(*) AS opportunity_count,
    SUM(amount_usd) AS amount_usd,
    SUM(weighted_amount_usd) AS weighted_amount_usd,
    AVG(probability_pct) AS avg_probability_pct,
    SUM(completed_activity_count) AS completed_activity_count
FROM silver.salesforce_opportunity_pipeline
GROUP BY 1
ORDER BY
    CASE stage_group
        WHEN 'Early stage' THEN 1
        WHEN 'Qualified' THEN 2
        WHEN 'Late stage' THEN 3
        WHEN 'Closed won' THEN 4
        WHEN 'Closed lost' THEN 5
        ELSE 6
    END
