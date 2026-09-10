-- Run with: bruin query --description "reconcile weekly revenue" --query "$(cat queries/reconciliation/source-vs-mart.sql)"
WITH source_total AS (
    SELECT SUM(line_revenue) AS revenue
    FROM fct_order_lines
    WHERE order_status <> 'cancelled'
), mart_total AS (
    SELECT SUM(revenue) AS revenue
    FROM weekly_category_revenue
)
SELECT source_total.revenue AS source_revenue,
       mart_total.revenue AS mart_revenue,
       source_total.revenue - mart_total.revenue AS difference
FROM source_total CROSS JOIN mart_total;
