-- Question: Among consumer-segment customers who have placed at least one
--           order, what is the average order revenue per customer?
-- Run this, then decide whether the answer is correct.

SELECT ROUND(
           SUM(o.order_total) / COUNT(DISTINCT o.customer_id)
       , 2) AS avg_revenue_per_customer
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE c.segment = 'consumer';
