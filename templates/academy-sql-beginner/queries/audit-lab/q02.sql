-- Question: What was total revenue by store, all time?
-- Run this, then decide whether the answer is correct.

SELECT
    s.city,
    SUM(o.order_total) AS revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN stores s       ON o.store_id = s.store_id
GROUP BY s.city
ORDER BY revenue DESC;
