-- Run with: bruin query --connection duckdb-default --description "inspect the select-star result" --limit 10 --query "$(cat queries/ops/select-star-all.sql)"
SELECT * FROM fct_order_lines LIMIT 10;
