/* @bruin

name: silver.limited_edition_performance
type: sf.sql
description: |
  Limited-edition SKU performance versus permanent SKU benchmarks by channel.
connection: snowflake-default
tags:
  - energy_drink
  - silver
  - limited_edition
domains:
  - sales
meta:
  asset_grain: One row per limited-edition SKU and channel.
  pipeline_role: Decision metric layer for SKU lifecycle recommendations.

materialization:
  type: table
  strategy: create+replace

depends:
  - silver.sku_daily_sales

columns:
  - name: sku_id
    type: VARCHAR
    description: Limited-edition SKU identifier.
  - name: channel
    type: VARCHAR
    description: Off Premise channel.
  - name: velocity_index
    type: DOUBLE
    description: SKU units per active store-day divided by permanent benchmark velocity.
  - name: gross_margin_pct
    type: DOUBLE
    description: Gross margin percentage.
  - name: distribution_rate
    type: DOUBLE
    description: Share of active store-days in distribution.
  - name: stockout_rate
    type: DOUBLE
    description: Share of active store-days out of stock.
  - name: channel_recommendation
    type: VARCHAR
    description: Channel-level lifecycle recommendation.

@bruin */

WITH sku_metrics AS (
    SELECT
        sku_id,
        sku_name,
        lifecycle_type,
        lifecycle_status,
        decision_cycle,
        season_year,
        source_confidence,
        benchmark_group,
        channel,
        MIN(market_launch_date) AS launch_date,
        MAX(market_end_date) AS planned_end_date,
        MAX(target_margin_pct) AS target_margin_pct,
        COUNT(*) AS active_store_days,
        COUNT(DISTINCT store_id) AS active_stores,
        SUM(units_sold) AS units_sold,
        SUM(net_sales_usd) AS net_sales_usd,
        SUM(gross_margin_usd) AS gross_margin_usd,
        DIV0(SUM(units_sold), COUNT(*)) AS units_per_store_day,
        DIV0(SUM(gross_margin_usd), SUM(net_sales_usd)) AS gross_margin_pct,
        AVG(IFF(in_distribution, 1, 0)) AS distribution_rate,
        AVG(IFF(display_compliant, 1, 0)) AS display_compliance_rate,
        AVG(IFF(out_of_stock, 1, 0)) AS stockout_rate,
        AVG(IFF(promotion_id IS NOT NULL, 1, 0)) AS promo_coverage_rate
    FROM silver.sku_daily_sales
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
benchmarks AS (
    SELECT
        benchmark_group,
        channel,
        AVG(units_per_store_day) AS benchmark_units_per_store_day,
        AVG(gross_margin_pct) AS benchmark_gross_margin_pct,
        AVG(distribution_rate) AS benchmark_distribution_rate
    FROM sku_metrics
    WHERE lifecycle_type = 'permanent'
    GROUP BY 1, 2
)
SELECT
    sku_metrics.sku_id,
    sku_metrics.sku_name,
    sku_metrics.channel,
    sku_metrics.lifecycle_type,
    sku_metrics.lifecycle_status,
    sku_metrics.decision_cycle,
    sku_metrics.season_year,
    sku_metrics.source_confidence,
    sku_metrics.launch_date,
    sku_metrics.planned_end_date,
    sku_metrics.benchmark_group,
    sku_metrics.active_store_days,
    sku_metrics.active_stores,
    sku_metrics.units_sold,
    sku_metrics.net_sales_usd,
    sku_metrics.gross_margin_usd,
    sku_metrics.units_per_store_day,
    benchmarks.benchmark_units_per_store_day,
    DIV0(sku_metrics.units_per_store_day, benchmarks.benchmark_units_per_store_day) AS velocity_index,
    sku_metrics.gross_margin_pct,
    benchmarks.benchmark_gross_margin_pct,
    sku_metrics.target_margin_pct,
    sku_metrics.distribution_rate,
    benchmarks.benchmark_distribution_rate,
    sku_metrics.display_compliance_rate,
    sku_metrics.stockout_rate,
    sku_metrics.promo_coverage_rate,
    CASE
        WHEN DIV0(sku_metrics.units_per_store_day, benchmarks.benchmark_units_per_store_day) >= 1.10
            AND sku_metrics.gross_margin_pct >= sku_metrics.target_margin_pct
            AND sku_metrics.distribution_rate >= 0.70
            AND sku_metrics.stockout_rate < 0.16
            THEN 'make_permanent'
        WHEN DIV0(sku_metrics.units_per_store_day, benchmarks.benchmark_units_per_store_day) >= 0.90
            OR sku_metrics.gross_margin_pct >= sku_metrics.target_margin_pct
            THEN 'extend'
        ELSE 'discontinue'
    END AS channel_recommendation
FROM sku_metrics
LEFT JOIN benchmarks
    ON sku_metrics.benchmark_group = benchmarks.benchmark_group
    AND sku_metrics.channel = benchmarks.channel
WHERE sku_metrics.lifecycle_type IN ('limited_edition', 'permanent_candidate')
