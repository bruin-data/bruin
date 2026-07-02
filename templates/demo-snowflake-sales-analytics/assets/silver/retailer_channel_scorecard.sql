/* @bruin

name: silver.retailer_channel_scorecard
type: sf.sql
description: Retailer and channel scorecard for SKU sales, margin, distribution, promo, and inventory health.
connection: snowflake-default
tags:
  - energy_drink
  - silver
  - retailer_scorecard
domains:
  - sales
meta:
  asset_grain: One row per country, retailer, channel, and SKU.
  pipeline_role: Account-level drilldown layer for agents and dashboards.

materialization:
  type: table
  strategy: create+replace

depends:
  - silver.sku_daily_sales

columns:
  - name: retailer_id
    type: VARCHAR
    description: Retailer account identifier.
  - name: channel
    type: VARCHAR
    description: Off Premise channel.
  - name: country
    type: VARCHAR
    description: Country market.
  - name: sku_id
    type: VARCHAR
    description: SKU identifier.
  - name: velocity_units_per_store_day
    type: DOUBLE
    description: Units sold per active store-day.
  - name: distribution_rate
    type: DOUBLE
    description: Share of store-days in distribution.
  - name: stockout_rate
    type: DOUBLE
    description: Share of store-days out of stock.

@bruin */

SELECT
    country,
    region,
    market,
    retailer_id,
    retailer_name,
    channel,
    sku_id,
    sku_name,
    lifecycle_type,
    lifecycle_status,
    COUNT(*) AS active_store_days,
    COUNT(DISTINCT store_id) AS active_stores,
    SUM(units_sold) AS units_sold,
    SUM(net_sales_usd) AS net_sales_usd,
    SUM(gross_margin_usd) AS gross_margin_usd,
    DIV0(SUM(units_sold), COUNT(*)) AS velocity_units_per_store_day,
    DIV0(SUM(gross_margin_usd), SUM(net_sales_usd)) AS gross_margin_pct,
    AVG(IFF(in_distribution, 1, 0)) AS distribution_rate,
    AVG(IFF(display_compliant, 1, 0)) AS display_compliance_rate,
    AVG(IFF(out_of_stock, 1, 0)) AS stockout_rate,
    AVG(IFF(promotion_id IS NOT NULL, 1, 0)) AS promo_coverage_rate
FROM silver.sku_daily_sales
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
