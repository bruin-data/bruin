-- Run with: bruin query --connection duckdb-default --description "measure the sargable date range plan" --limit 100 --query "$(cat queries/ops/sargability-range.sql)"
EXPLAIN ANALYZE
SELECT order_id, ordered_at, line_revenue
FROM fct_order_lines
WHERE ordered_at >= TIMESTAMP '2024-01-01'
  AND ordered_at < TIMESTAMP '2025-01-01';
