-- Step 6 - Joining tables, and the most important lesson in the course
--
-- A JOIN lines up rows from two tables on a matching column. orders has
-- one row per order; order_items has one row per line, about 2.4 lines
-- per order. Joining them multiplies each order row by its number of lines.
--
-- This query shows the fan-out directly: how many lines each order gained.

SELECT
    o.order_id,
    o.order_total,
    COUNT(oi.line_number) AS lines_on_order
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id, o.order_total
ORDER BY lines_on_order DESC, o.order_id
LIMIT 10;

-- The trap: order_total is an order-level value. Summing it after the join
-- counts it once per line, not once per order.
--
-- 1. Run these two and compare. The first is inflated by roughly 2.4x:
--      SELECT SUM(o.order_total)
--      FROM orders o
--      JOIN order_items oi ON o.order_id = oi.order_id;   -- WRONG
--
--      SELECT SUM(order_total) FROM orders;               -- right
--
--    When you want a line-level revenue instead, sum a line-level column:
--      SELECT SUM(oi.quantity * oi.net_price) FROM order_items oi;
--    (use net_price, the price actually charged, not unit_price, which is the
--     product's catalogue price before any discount.)
--
-- 2. INNER JOIN vs LEFT JOIN. A few order lines point at a product_id that is
--    not in products. Compare the row counts:
--      SELECT COUNT(*) FROM order_items oi
--      JOIN products p ON oi.product_id = p.product_id;        -- inner
--
--      SELECT COUNT(*) FROM order_items oi
--      LEFT JOIN products p ON oi.product_id = p.product_id;   -- left
--    The inner join silently dropped the unmatched lines - and their revenue.
--
-- 3. LEFT JOIN also keeps rows that have nothing on the other side. Forty of the
--    500 customers have never ordered. Find them:
--      SELECT COUNT(*) FROM customers c
--      LEFT JOIN orders o ON c.customer_id = o.customer_id
--      WHERE o.order_id IS NULL;
--
-- 4. Where you put a condition on the right-hand table decides whether a LEFT
--    JOIN stays a LEFT JOIN. Run both and compare the counts:
--      SELECT COUNT(*) FROM customers c
--      LEFT JOIN orders o
--             ON c.customer_id = o.customer_id
--            AND o.order_status = 'completed';    -- condition in ON: still a LEFT JOIN
--
--      SELECT COUNT(*) FROM customers c
--      LEFT JOIN orders o ON c.customer_id = o.customer_id
--      WHERE o.order_status = 'completed';        -- condition in WHERE: now an INNER JOIN
--    The WHERE version throws away the unmatched customers, because their
--    order_status is NULL and NULL = 'completed' is never true. This is the
--    quietest way to turn a LEFT JOIN back into an INNER one.
--
-- 5. A warning about DISTINCT. Adding it to the broken query in step 1 makes the
--    number look plausible again without fixing the grain. If you find yourself
--    reaching for DISTINCT to make a total look right, the join is wrong. Treat
--    every DISTINCT in a query - yours or an agent's - as something to justify.
