-- Question: How many orders has each store taken?
-- Run this, then decide whether the answer is correct.

SELECT
    s.city,
    COUNT(*) AS orders
FROM orders o
JOIN stores s ON o.store_id = s.store_id
GROUP BY s.city
ORDER BY orders DESC;
