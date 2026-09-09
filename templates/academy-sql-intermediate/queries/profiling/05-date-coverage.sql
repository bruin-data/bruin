-- Date coverage against the calendar spine.
--
-- Change the fact table, its date or timestamp column, and the range below
-- and run this again on any date column you are about to trend or bucket.
--
-- Worked example: orders.ordered_at against the dates spine.

WITH order_days AS (
    SELECT DISTINCT CAST(ordered_at AS DATE) AS order_day
    FROM orders
)
SELECT
    MIN(order_day)  AS first_order_day,
    MAX(order_day)  AS last_order_day,
    COUNT(*)        AS distinct_order_days
FROM order_days;

-- distinct_order_days tells you how many calendar days between
-- first_order_day and last_order_day have at least one row. Compare it
-- with the number of days in that span to see whether every day is covered.

-- Find the specific days inside that span with no row in the fact table.

WITH order_days AS (
    SELECT DISTINCT CAST(ordered_at AS DATE) AS order_day
    FROM orders
),
bounds AS (
    SELECT
        MIN(order_day) AS first_day,
        MAX(order_day) AS last_day
    FROM order_days
)
SELECT d.date_day
FROM dates d
JOIN bounds b
  ON d.date_day BETWEEN b.first_day AND b.last_day
LEFT JOIN order_days od
  ON od.order_day = d.date_day
WHERE od.order_day IS NULL
ORDER BY d.date_day;

-- Every row here is a day inside your own data's range with no fact rows.
-- Decide whether that means nothing happened that day or the load missed
-- it, before you treat a gap in a chart as fact.
