-- Step 5 - Counting and totalling
--
-- Aggregate functions collapse many rows into one number: COUNT, SUM, AVG, MIN,
-- MAX. GROUP BY runs the aggregate once per group. One row of this result is one
-- order_status.

SELECT
    order_status,
    COUNT(*)              AS orders,
    ROUND(SUM(order_total), 2) AS total_value,
    ROUND(AVG(order_total), 2) AS avg_value
FROM orders
GROUP BY order_status
ORDER BY orders DESC;

-- Notice the row where order_status is NULL. NULL is its own group here - the
-- orders with no status did not vanish, they just grouped together. A filter
-- like WHERE order_status != 'cancelled' would have dropped them instead.
--
-- Try this:
--
-- 1. COUNT(*) counts every row. COUNT(a_column) counts only rows where that
--    column is not NULL. Run both against order_items and compare:
--      SELECT COUNT(*), COUNT(unit_cost) FROM order_items;
--    The gap is the number of lines with a missing unit_cost. AVG(unit_cost)
--    divides by the smaller of the two - the non-null count.
--
-- 2. HAVING filters groups after aggregating (WHERE filters rows before).
--    Find the statuses with more than 100 orders:
--      ... GROUP BY order_status HAVING COUNT(*) > 100
--
-- 3. CASE turns a condition into a value, and it works inside an aggregate. This
--    counts two things in one pass over the table, one row of result:
--      SELECT
--          COUNT(*)                                            AS orders,
--          SUM(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
--          SUM(CASE WHEN order_status IS NULL       THEN 1 ELSE 0 END) AS no_status
--      FROM orders;
--    Read the CASE aloud: "when the status is cancelled, count 1, otherwise 0."
--
-- 4. Which customer segments spend the most? Group customers... but
--    careful, spend lives in orders. That needs a join - Step 6.
