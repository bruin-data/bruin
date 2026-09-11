-- Run with: bruin query --connection duckdb-default --description "measure the functional date predicate plan" --limit 100 --query "$(cat queries/ops/sargability-functional.sql)"
EXPLAIN ANALYZE
SELECT order_id, ordered_at, line_revenue
FROM fct_order_lines
WHERE YEAR(ordered_at) = 2024;
