/* @bruin

name: gold.weekly_sku_council_summary
type: sf.sql
description: Weekly limited-edition SKU summary for scheduled agents and SKU council prep.
connection: snowflake-default
tags:
  - energy_drink
  - gold
  - weekly_report
  - scheduled_agent
domains:
  - sales
meta:
  asset_grain: One row per week and limited-edition SKU.
  pipeline_role: Scheduled report table for SKU council.

materialization:
  type: view

depends:
  - silver.sku_daily_sales
  - gold.limited_edition_decision_board

columns:
  - name: week_start_date
    type: DATE
    description: First day of the reporting week.
  - name: sku_id
    type: VARCHAR
    description: Limited-edition SKU identifier.
  - name: weekly_units_sold
    type: INTEGER
    description: Weekly units sold.
  - name: recommended_action
    type: VARCHAR
    description: Current lifecycle recommendation.

@bruin */

SELECT
    DATE_TRUNC('week', sales.sales_date)::DATE AS week_start_date,
    sales.sku_id,
    sales.sku_name,
    decisions.lifecycle_status,
    decisions.decision_cycle,
    decisions.recommended_action,
    SUM(sales.units_sold) AS weekly_units_sold,
    SUM(sales.net_sales_usd) AS weekly_net_sales_usd,
    SUM(sales.gross_margin_usd) AS weekly_gross_margin_usd,
    DIV0(SUM(sales.gross_margin_usd), SUM(sales.net_sales_usd)) AS weekly_gross_margin_pct,
    AVG(IFF(sales.in_distribution, 1, 0)) AS weekly_distribution_rate,
    AVG(IFF(sales.out_of_stock, 1, 0)) AS weekly_stockout_rate,
    COUNT(DISTINCT sales.store_id) AS active_stores
FROM silver.sku_daily_sales AS sales
INNER JOIN gold.limited_edition_decision_board AS decisions
    ON sales.sku_id = decisions.sku_id
WHERE sales.lifecycle_type IN ('limited_edition', 'permanent_candidate')
GROUP BY 1, 2, 3, 4, 5, 6
