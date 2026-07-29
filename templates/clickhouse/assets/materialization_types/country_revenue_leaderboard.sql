/* @bruin

name: country_revenue_leaderboard
type: clickhouse.sql
description: Demonstrates a ClickHouse view materialization. Bruin creates a stored query definition rather than a table, so consumers always query the current upstream mart while the explicit dependency preserves lineage and execution ordering.
tags:
  - layer:serving
  - domain:commerce
  - materialization:view
domains:
  - commerce
meta:
  consumer: analytics
  data_classification: internal

materialization:
  type: view

depends:
  - country_revenue
owner: commerce-analytics@example.com

columns:
  - name: country
    type: String
    description: Country represented in the leaderboard.
  - name: sales_region
    type: LowCardinality(String)
    description: Commercial sales region.
  - name: total_paid_amount
    type: Float64
    description: Paid revenue in USD.
  - name: target_attainment_ratio
    type: Float64
    description: Paid revenue divided by monthly revenue target.

@bruin */

SELECT
    country,
    sales_region,
    total_paid_amount,
    target_attainment_ratio
FROM country_revenue
ORDER BY total_paid_amount DESC
