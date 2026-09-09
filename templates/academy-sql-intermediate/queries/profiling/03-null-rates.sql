-- NULL count and percentage, one line per column.
--
-- Change the table name below, then add or remove a line per column you
-- want to check. Each line is independent, so to add a column, copy one
-- line and swap the column name in both places on it.
--
-- Worked example: orders.

WITH totals AS (
    SELECT COUNT(*) AS row_count FROM orders
)
SELECT 'order_id' AS column_name, COUNT(*) FILTER (WHERE order_id IS NULL) AS null_count, ROUND(100.0 * COUNT(*) FILTER (WHERE order_id IS NULL) / (SELECT row_count FROM totals), 2) AS null_pct FROM orders
UNION ALL
SELECT 'customer_id', COUNT(*) FILTER (WHERE customer_id IS NULL), ROUND(100.0 * COUNT(*) FILTER (WHERE customer_id IS NULL) / (SELECT row_count FROM totals), 2) FROM orders
UNION ALL
SELECT 'store_id', COUNT(*) FILTER (WHERE store_id IS NULL), ROUND(100.0 * COUNT(*) FILTER (WHERE store_id IS NULL) / (SELECT row_count FROM totals), 2) FROM orders
UNION ALL
SELECT 'ordered_at', COUNT(*) FILTER (WHERE ordered_at IS NULL), ROUND(100.0 * COUNT(*) FILTER (WHERE ordered_at IS NULL) / (SELECT row_count FROM totals), 2) FROM orders
UNION ALL
SELECT '_loaded_at', COUNT(*) FILTER (WHERE _loaded_at IS NULL), ROUND(100.0 * COUNT(*) FILTER (WHERE _loaded_at IS NULL) / (SELECT row_count FROM totals), 2) FROM orders
UNION ALL
SELECT 'promised_delivery_date', COUNT(*) FILTER (WHERE promised_delivery_date IS NULL), ROUND(100.0 * COUNT(*) FILTER (WHERE promised_delivery_date IS NULL) / (SELECT row_count FROM totals), 2) FROM orders
UNION ALL
SELECT 'currency_code', COUNT(*) FILTER (WHERE currency_code IS NULL), ROUND(100.0 * COUNT(*) FILTER (WHERE currency_code IS NULL) / (SELECT row_count FROM totals), 2) FROM orders
UNION ALL
SELECT 'order_status', COUNT(*) FILTER (WHERE order_status IS NULL), ROUND(100.0 * COUNT(*) FILTER (WHERE order_status IS NULL) / (SELECT row_count FROM totals), 2) FROM orders
UNION ALL
SELECT 'order_total', COUNT(*) FILTER (WHERE order_total IS NULL), ROUND(100.0 * COUNT(*) FILTER (WHERE order_total IS NULL) / (SELECT row_count FROM totals), 2) FROM orders
ORDER BY null_pct DESC;

-- A column at zero has no NULLs in this table today. Anything above zero is
-- a column you need a plan for before you aggregate or join on it, because
-- NULLs disappear from GROUP BY buckets and fail equality joins silently.
