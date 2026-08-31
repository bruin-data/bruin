-- Question: What was total product revenue in 2024?
-- Run this, then decide whether the answer is correct.

SELECT SUM(oi.quantity * oi.unit_price) AS revenue_2024
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.ordered_at >= '2024-01-01' AND o.ordered_at < '2025-01-01';
