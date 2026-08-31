-- Question: How many orders were not cancelled?
-- Run this, then decide whether the answer is correct.

SELECT COUNT(*) AS not_cancelled
FROM orders
WHERE order_status != 'cancelled';
