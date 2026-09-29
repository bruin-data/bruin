-- Run with: bruin query --description "compare monthly source totals" --query "$(cat queries/reconciliation/monthly-totals.sql)"
SELECT date_trunc('month', ordered_at)::DATE AS month_start,
       SUM(line_revenue) AS revenue,
       COUNT(*) AS line_count
FROM fct_order_lines
WHERE order_status <> 'cancelled'
GROUP BY 1
ORDER BY 1;
