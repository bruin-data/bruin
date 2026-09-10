-- Run with: bruin query --description "compare selected columns" --query "$(cat queries/ops/select-star-demo.sql)"
SELECT * FROM fct_order_lines LIMIT 10;
SELECT order_id, line_number, category_name, line_revenue
FROM fct_order_lines
LIMIT 10;
