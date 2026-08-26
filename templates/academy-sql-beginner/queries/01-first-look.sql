-- Step 4 - A first look at the data
--
-- This is a plain SQL file, not a Bruin asset. Run it with:
--   bruin query --connection duckdb-default --query "$(cat queries/01-first-look.sql)"
-- or just copy a query into your editor and run it there.
--
-- A SELECT reads columns FROM a table. WHERE keeps only some rows, ORDER BY
-- sorts them, and LIMIT stops after the first few. One row of this result is
-- one order.

SELECT
    order_id,
    ordered_at,
    order_status,
    order_total
FROM orders
ORDER BY ordered_at
LIMIT 20;

-- Try this, one change at a time:
--
-- 1. Keep only completed orders:
--      WHERE order_status = 'completed'
--
-- 2. Keep only 2024 orders. ordered_at is a timestamp, so compare with a range
--    rather than BETWEEN two dates:
--      WHERE ordered_at >= '2024-01-01' AND ordered_at < '2025-01-01'
--
-- 3. Name a column something readable with AS, and combine conditions with
--    AND, OR and NOT. Parentheses matter once you mix AND and OR:
--      SELECT order_id AS id, order_total AS value
--      FROM orders
--      WHERE (order_status = 'completed' OR order_status = 'shipped')
--        AND order_total > 500
--
-- 4. Three more ways to filter, all readable out loud:
--      WHERE order_status IN ('completed', 'shipped')   -- one of a list
--      WHERE order_total BETWEEN 100 AND 200            -- inclusive at both ends
--      WHERE currency_code LIKE 'A%'                    -- text starts with A
--
-- 5. DISTINCT collapses repeated values. How many currencies are there?
--      SELECT DISTINCT currency_code FROM orders ORDER BY currency_code
--    Careful: DISTINCT is not a fix for a wrong query. If a number looks too big
--    and DISTINCT makes it look right, the grain is still wrong underneath.
--
-- 6. Now count the orders whose status is NOT 'completed':
--      WHERE order_status != 'completed'
--    Then count the ones where order_status IS NULL. Add the two counts up.
--    Do they equal the number of not-completed orders you expected? A != filter
--    silently skips NULLs, and some orders here have a NULL status. IS NULL and
--    IS NOT NULL are the only comparisons that work on a NULL.
