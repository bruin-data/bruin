/* @bruin

name: gold.sku_decision_drivers
type: sf.sql
description: Metric-level explanation rows for agent drilldown into limited-edition SKU recommendations.
connection: snowflake-default
tags:
  - energy_drink
  - gold
  - agent_context
domains:
  - sales
meta:
  asset_grain: One row per SKU and decision driver metric.
  pipeline_role: Citation table for agent explanations.

materialization:
  type: view

depends:
  - gold.limited_edition_decision_board

columns:
  - name: sku_id
    type: VARCHAR
    description: Limited-edition SKU identifier.
  - name: driver_name
    type: VARCHAR
    description: Metric name used in the lifecycle decision.
  - name: driver_value
    type: DOUBLE
    description: Numeric metric value.
  - name: threshold_value
    type: DOUBLE
    description: Decision threshold for the metric.
  - name: driver_status
    type: VARCHAR
    description: Whether the metric supports or weakens the recommendation.

@bruin */

SELECT
    sku_id,
    sku_name,
    recommended_action,
    'velocity_index' AS driver_name,
    avg_velocity_index AS driver_value,
    1.10 AS threshold_value,
    IFF(avg_velocity_index >= 1.10, 'supports', 'weakens') AS driver_status,
    'Average channel velocity versus permanent SKU benchmark.' AS driver_description
FROM gold.limited_edition_decision_board

UNION ALL

SELECT
    sku_id,
    sku_name,
    recommended_action,
    'gross_margin_pct' AS driver_name,
    gross_margin_pct AS driver_value,
    0.40 AS threshold_value,
    IFF(gross_margin_pct >= 0.40, 'supports', 'weakens') AS driver_status,
    'Gross margin rate versus target.' AS driver_description
FROM gold.limited_edition_decision_board

UNION ALL

SELECT
    sku_id,
    sku_name,
    recommended_action,
    'distribution_rate' AS driver_name,
    avg_distribution_rate AS driver_value,
    0.70 AS threshold_value,
    IFF(avg_distribution_rate >= 0.70, 'supports', 'weakens') AS driver_status,
    'Average distribution coverage across Off Premise channels.' AS driver_description
FROM gold.limited_edition_decision_board

UNION ALL

SELECT
    sku_id,
    sku_name,
    recommended_action,
    'stockout_rate' AS driver_name,
    avg_stockout_rate AS driver_value,
    0.16 AS threshold_value,
    IFF(avg_stockout_rate < 0.16, 'supports', 'weakens') AS driver_status,
    'Inventory availability risk during the selling window.' AS driver_description
FROM gold.limited_edition_decision_board
