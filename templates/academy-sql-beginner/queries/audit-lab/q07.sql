-- Question: How many orders were placed in 2024?
-- Run this, then decide whether the answer is correct.

SELECT COUNT(*) AS orders_2024
FROM orders
WHERE ordered_at BETWEEN '2024-01-01' AND '2024-12-31';
