-- Question: What was total product revenue by category, all time?
-- Run this, then decide whether the answer is correct.

SELECT
    p.category_name,
    SUM(oi.quantity * oi.net_price) AS revenue
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.category_name
ORDER BY revenue DESC;
