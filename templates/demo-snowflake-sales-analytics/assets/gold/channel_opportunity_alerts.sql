/* @bruin

name: gold.channel_opportunity_alerts
type: sf.sql
description: Channel-level exceptions and opportunities for limited-edition SKUs.
connection: snowflake-default
tags:
  - energy_drink
  - gold
  - alerts
domains:
  - sales
meta:
  asset_grain: One row per SKU and channel alert.
  pipeline_role: Scheduled agent alert input.

materialization:
  type: view

depends:
  - silver.limited_edition_performance

columns:
  - name: sku_id
    type: VARCHAR
    description: Limited-edition SKU identifier.
  - name: channel
    type: VARCHAR
    description: Off Premise channel.
  - name: alert_type
    type: VARCHAR
    description: Type of opportunity or risk.
  - name: alert_message
    type: VARCHAR
    description: Human-readable alert context.

@bruin */

SELECT
    sku_id,
    sku_name,
    channel,
    'distribution_gap' AS alert_type,
    velocity_index,
    distribution_rate,
    stockout_rate,
    'Velocity is above benchmark but distribution is below target; ask account owner for authorization and display notes.' AS alert_message
FROM silver.limited_edition_performance
WHERE velocity_index >= 1.10
    AND distribution_rate < 0.70

UNION ALL

SELECT
    sku_id,
    sku_name,
    channel,
    'stockout_risk' AS alert_type,
    velocity_index,
    distribution_rate,
    stockout_rate,
    'Velocity is strong but stockouts are high; review replenishment before making a lifecycle decision.' AS alert_message
FROM silver.limited_edition_performance
WHERE velocity_index >= 1.00
    AND stockout_rate >= 0.16

UNION ALL

SELECT
    sku_id,
    sku_name,
    channel,
    'underperforming_channel' AS alert_type,
    velocity_index,
    distribution_rate,
    stockout_rate,
    'Channel is below benchmark; isolate retailer execution before scaling this SKU.' AS alert_message
FROM silver.limited_edition_performance
WHERE velocity_index < 0.85
