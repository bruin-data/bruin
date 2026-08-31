-- Step 7 - Common Table Expressions (CTEs)
--
-- A CTE is a named, temporary result you define with WITH and then use like a
-- table. It lets you do one step at a time instead of nesting everything.
--
-- Here the first step rolls order_items up to one row per order (line revenue,
-- summed at the right grain). The second step joins that back to orders, one row
-- per order.
--
-- Do not expect line_revenue and order_total to agree. They are two different
-- numbers: 851,617.69 of line revenue against 604,065.00 of header totals across
-- the whole table, and on any single order they can be far apart. order_total is
-- supplied by the source rather than derived from the lines. See docs/schema.md.
-- The point of the join here is the grain, not the reconciliation.

WITH order_revenue AS (
    SELECT
        oi.order_id,
        SUM(oi.quantity * oi.net_price) AS line_revenue
    FROM order_items oi
    GROUP BY oi.order_id
)
SELECT
    o.order_id,
    o.order_total,
    r.line_revenue
FROM order_revenue r
JOIN orders o ON r.order_id = o.order_id
ORDER BY r.line_revenue DESC
LIMIT 10;

-- Because order_revenue already has one row per order, joining it to orders
-- does NOT fan out. Aggregating to the right grain first is the usual fix for
-- the double-counting trap from Step 6.
--
-- The order you write a query in is not the order the database runs it in. This
-- is the logical order of execution, and it explains most beginner surprises:
--
--      FROM      pick the tables and join them
--      WHERE     drop rows          <- runs before grouping, so no aggregates here
--      GROUP BY  form the groups
--      HAVING    drop groups        <- runs after grouping, so aggregates allowed
--      SELECT    compute the output columns and their aliases
--      DISTINCT  collapse duplicate output rows
--      ORDER BY  sort              <- can use a SELECT alias, because it runs later
--      LIMIT     cut the result short
--
-- One thing falls straight out of that list: WHERE runs before GROUP BY, so it
-- cannot see an aggregate. Run both of these and compare:
--
--      SELECT order_status, COUNT(*) FROM orders
--      WHERE COUNT(*) > 100 GROUP BY order_status;      -- errors
--
--      SELECT order_status, COUNT(*) FROM orders
--      GROUP BY order_status HAVING COUNT(*) > 100;     -- works
--
-- The first fails with "WHERE clause cannot contain aggregates". At the moment
-- WHERE runs, the groups do not exist yet and there is nothing to count. HAVING
-- runs after GROUP BY, which is why it is the one that can filter on a count.
-- That is the whole WHERE-versus-HAVING rule, and it is just the order above.
--
-- Try this:
--
-- 1. Add a second CTE that keeps only 2024 orders, then join to order_revenue,
--    to get 2024 line revenue without any fan-out.
--
-- 2. Rank customers by total line revenue. You will need to join order_revenue
--    to orders (for customer_id), group by customer_id, and ORDER BY the
--    total DESC. This is the "top customers" question.
--
-- 3. Read the query at the top of this file aloud, one CTE at a time, saying what
--    one row of each step represents. Reading a query aloud is a cheap and
--    genuinely effective way to catch a wrong grain before you trust a number.
