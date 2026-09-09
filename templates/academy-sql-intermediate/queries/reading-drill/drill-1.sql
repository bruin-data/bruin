-- Drill 1. Target: annotated in 30 seconds.
--
-- Question: how many orders arrived in each quarter of 2024, and what did they
-- total at the header level?
--
-- Annotate each step with what one row of it represents.

SELECT
    d.year,
    d.quarter,
    COUNT(*)            AS order_rows,
    SUM(o.order_total)  AS header_total
FROM orders o
JOIN dates d
  ON d.date_day = CAST(o.ordered_at AS DATE)
WHERE d.year = 2024
GROUP BY d.year, d.quarter
ORDER BY d.quarter;
