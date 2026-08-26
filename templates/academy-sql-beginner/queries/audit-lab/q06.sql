-- Question: What is the average order total across all orders?
-- Run this, then decide whether the answer is correct.

SELECT ROUND(AVG(order_total), 2) AS avg_order_total
FROM orders;
