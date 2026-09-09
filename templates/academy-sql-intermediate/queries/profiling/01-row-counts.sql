-- Row counts and grain check.
--
-- Change the table name and the claimed key below and run this again on any
-- table you are about to work with. It is the first thing to run on data you
-- do not know yet.
--
-- Worked example: orders, claimed key is order_id.

SELECT
    COUNT(*)                    AS total_rows,
    COUNT(DISTINCT order_id)    AS distinct_order_id
FROM orders;

-- Read the two numbers together.
--
-- If total_rows equals distinct_order_id, one row per order_id is the grain
-- of this table, which is what "order_id is the key" should mean.
--
-- If total_rows is larger than distinct_order_id, either order_id is not
-- actually the key, or the same key shows up on more than one row. Either
-- way, treat the assumption as unproven until the two numbers match.
