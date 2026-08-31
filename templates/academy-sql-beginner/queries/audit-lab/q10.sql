-- Question: On average, how many lines does an order have?
-- Run this, then decide whether the answer is correct.

SELECT ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT order_id), 2) AS avg_lines_per_order
FROM order_items;
