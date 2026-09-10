-- Run with: bruin query --description "compare sargable date predicates" --query "$(cat queries/ops/sargability-demo.sql)"
EXPLAIN ANALYZE
SELECT order_id, ordered_at, line_revenue
FROM fct_order_lines
WHERE ordered_at >= TIMESTAMP '2024-01-01'
  AND ordered_at < TIMESTAMP '2025-01-01';

-- The functional form is intentionally non-sargable for the lesson.
EXPLAIN ANALYZE
SELECT order_id, ordered_at, line_revenue
FROM fct_order_lines
WHERE YEAR(ordered_at) = 2024;
