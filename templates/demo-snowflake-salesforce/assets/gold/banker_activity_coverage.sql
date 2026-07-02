/* @bruin

name: gold.banker_activity_coverage
type: sf.sql
description: Banker activity coverage rollup across Salesforce tasks, events, and product pipeline.
connection: snowflake-default
tags:
  - finance
  - credit_union
  - crm
  - activity
  - gold
  - dashboard
domains:
  - crm
  - member_relationships
meta:
  asset_grain: One row per Salesforce activity owner.
  load_pattern: View over silver activity timeline and product pipeline.
  refresh_cadence: Daily batch pipeline.

materialization:
  type: view

depends:
  - silver.salesforce_activity_timeline
  - silver.salesforce_product_pipeline

columns:
  - name: owner_id
    type: VARCHAR
    description: Salesforce User identifier for the banker or activity owner.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: owner_name
    type: VARCHAR
    description: Banker or owner name.
  - name: activity_count
    type: INTEGER
    description: Number of tasks and events.
    checks:
      - name: non_negative
  - name: completed_activity_count
    type: INTEGER
    description: Completed task and event count.
    checks:
      - name: non_negative
  - name: completed_activity_rate
    type: DOUBLE
    description: Share of activity completed.
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: owned_product_pipeline_usd
    type: DOUBLE
    description: Open product pipeline owned by the banker.
    checks:
      - name: non_negative
  - name: owned_product_count
    type: INTEGER
    description: Number of distinct product records in owned pipeline.
    checks:
      - name: non_negative

@bruin */

WITH product_pipeline AS (
    SELECT
        owner_id,
        SUM(IFF(NOT is_closed, line_amount_usd, 0)) AS owned_product_pipeline_usd,
        COUNT(DISTINCT product_id) AS owned_product_count
    FROM silver.salesforce_product_pipeline
    GROUP BY 1
)

SELECT
    COALESCE(a.owner_id, pp.owner_id) AS owner_id,
    COALESCE(MAX(a.owner_name), 'Unknown owner') AS owner_name,
    COUNT(a.activity_id) AS activity_count,
    COUNT_IF(a.completed_flag) AS completed_activity_count,
    COALESCE(COUNT_IF(a.completed_flag) / NULLIF(COUNT(a.activity_id), 0), 0) AS completed_activity_rate,
    COALESCE(MAX(pp.owned_product_pipeline_usd), 0) AS owned_product_pipeline_usd,
    COALESCE(MAX(pp.owned_product_count), 0) AS owned_product_count
FROM silver.salesforce_activity_timeline AS a
FULL OUTER JOIN product_pipeline AS pp
    ON pp.owner_id = a.owner_id
WHERE COALESCE(a.owner_id, pp.owner_id) IS NOT NULL
GROUP BY 1
