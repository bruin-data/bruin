/* @bruin
name: weekly_category_revenue
type: duckdb.sql
description: >-
  Weekly revenue by product category. One row per category per ISO week.
  Revenue is net_price times quantity, excluding cancelled orders.
owner: data-team@example.com
tags:
  - layer:mart
  - domain:commerce
meta:
  business_owner: "Commercial"
  metric_definition: "SUM(quantity * net_price) over completed and shipped order lines"
  currency: "Source currency, not converted. See known_limitation."
  expected_update: "Daily by 08:00 UTC"
  known_limitation: "Revenue is not currency-converted. Comparing countries mixes currencies."
depends:
  - fct_order_lines
  - dates
materialization:
  type: table
  strategy: create+replace
columns:
  - name: iso_week
    type: date
    primary_key: true
    checks:
      - name: not_null
  - name: category_name
    type: varchar
    primary_key: true
    checks:
      - name: not_null
  - name: revenue
    type: decimal
    checks:
      - name: non_negative
  - name: order_line_count
    type: integer
unit_tests:
  - name: aggregates_lines_by_week_and_category
    inputs:
      - asset: fct_order_lines
        rows:
          - {ordered_at: "2024-01-02 10:00:00", category_name: Electronics, order_status: completed, quantity: 2, net_price: 10, line_revenue: 20}
          - {ordered_at: "2024-01-03 10:00:00", category_name: Electronics, order_status: shipped, quantity: 1, net_price: 5, line_revenue: 5}
    expected:
      rows:
        - {iso_week: "2024-01-01", category_name: Electronics, revenue: 25, order_line_count: 2}
      match: exact
  - name: excludes_cancelled_lines
    inputs:
      - asset: fct_order_lines
        rows:
          - {ordered_at: "2024-02-05 10:00:00", category_name: Books, order_status: cancelled, quantity: 4, net_price: 20, line_revenue: 80}
    expected:
      count: 0
@bruin */

SELECT
    date_trunc('week', CAST(ordered_at AS DATE))::DATE AS iso_week,
    category_name,
    CAST(SUM(line_revenue) AS DECIMAL(14, 2)) AS revenue,
    COUNT(*)::INTEGER AS order_line_count
FROM fct_order_lines
WHERE order_status <> 'cancelled'
GROUP BY 1, 2
ORDER BY 1, 2;
