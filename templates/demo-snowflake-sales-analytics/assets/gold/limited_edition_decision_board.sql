/* @bruin

name: gold.limited_edition_decision_board
type: sf.sql
description: |
  Executive decision board for limited-edition SKUs with a recommended action:
  discontinue, extend, or make permanent.
connection: snowflake-default
tags:
  - energy_drink
  - gold
  - dashboard
  - limited_edition
domains:
  - sales
meta:
  asset_grain: One row per limited-edition SKU.
  pipeline_role: Primary dashboard and agent answer table.

materialization:
  type: view

depends:
  - silver.limited_edition_performance

columns:
  - name: sku_id
    type: VARCHAR
    description: Limited-edition SKU identifier.
  - name: recommended_action
    type: VARCHAR
    description: Overall SKU lifecycle recommendation.
  - name: recommendation_reason
    type: VARCHAR
    description: Short explanation for the recommendation.

@bruin */

WITH rollup AS (
    SELECT
        sku_id,
        sku_name,
        MAX(lifecycle_type) AS lifecycle_type,
        MAX(lifecycle_status) AS lifecycle_status,
        MAX(decision_cycle) AS decision_cycle,
        MAX(season_year) AS season_year,
        MAX(source_confidence) AS source_confidence,
        MIN(launch_date) AS launch_date,
        MAX(planned_end_date) AS planned_end_date,
        SUM(active_store_days) AS active_store_days,
        SUM(active_stores) AS active_stores,
        SUM(units_sold) AS units_sold,
        SUM(net_sales_usd) AS net_sales_usd,
        SUM(gross_margin_usd) AS gross_margin_usd,
        AVG(velocity_index) AS avg_velocity_index,
        MIN(velocity_index) AS min_velocity_index,
        MAX(velocity_index) AS max_velocity_index,
        DIV0(SUM(gross_margin_usd), SUM(net_sales_usd)) AS gross_margin_pct,
        AVG(distribution_rate) AS avg_distribution_rate,
        AVG(display_compliance_rate) AS avg_display_compliance_rate,
        AVG(stockout_rate) AS avg_stockout_rate,
        COUNT_IF(channel_recommendation = 'make_permanent') AS channels_make_permanent,
        COUNT_IF(channel_recommendation = 'extend') AS channels_extend,
        COUNT_IF(channel_recommendation = 'discontinue') AS channels_discontinue,
        COUNT(*) AS channel_count
    FROM silver.limited_edition_performance
    GROUP BY 1, 2
)
SELECT
    sku_id,
    sku_name,
    lifecycle_type,
    lifecycle_status,
    decision_cycle,
    season_year,
    source_confidence,
    launch_date,
    planned_end_date,
    active_store_days,
    active_stores,
    units_sold,
    net_sales_usd,
    gross_margin_usd,
    avg_velocity_index,
    min_velocity_index,
    max_velocity_index,
    gross_margin_pct,
    avg_distribution_rate,
    avg_display_compliance_rate,
    avg_stockout_rate,
    channels_make_permanent,
    channels_extend,
    channels_discontinue,
    CASE
        WHEN avg_velocity_index >= 1.10
            AND gross_margin_pct >= 0.40
            AND avg_distribution_rate >= 0.70
            AND avg_stockout_rate < 0.16
            AND channels_make_permanent >= 2
            THEN 'make_permanent'
        WHEN avg_velocity_index >= 0.90
            OR channels_make_permanent + channels_extend >= 3
            THEN 'extend'
        ELSE 'discontinue'
    END AS recommended_action,
    CASE
        WHEN avg_velocity_index >= 1.10
            AND gross_margin_pct >= 0.40
            AND avg_distribution_rate >= 0.70
            AND avg_stockout_rate < 0.16
            AND channels_make_permanent >= 2
            THEN 'Velocity and margin clear the permanent SKU benchmark across enough channels.'
        WHEN avg_velocity_index >= 0.90
            OR channels_make_permanent + channels_extend >= 3
            THEN 'Performance is mixed but strong enough to justify another selling window or channel-specific push.'
        ELSE 'Velocity, distribution, or margin is below the threshold for more shelf commitment.'
    END AS recommendation_reason
FROM rollup
