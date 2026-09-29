-- Run with: bruin query --connection duckdb-default --description "inspect the narrow projection result" --limit 10 --query "$(cat queries/ops/select-star-columns.sql)"
SELECT order_id, line_number, category_name, line_revenue
FROM fct_order_lines
LIMIT 10;
