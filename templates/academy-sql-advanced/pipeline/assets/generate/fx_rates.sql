/* @bruin
name: fx_rates
type: duckdb.sql

description: "Daily exchange rates."

materialization:
  type: table
  strategy: create+replace

columns:
  - name: rate_date
    type: date
  - name: from_currency
    type: varchar
  - name: to_currency
    type: varchar
  - name: rate
    type: decimal
@bruin */

-- Deterministic generation. Every rate is a pure function of the day offset.
-- Do not introduce random(), now(), or current_date - see docs/_data-design.md.
--
-- Five currency pairs on every one of the 1,096 days, including the USD
-- identity row. Each rate is a base level plus two movements, both integer
-- arithmetic on the day offset:
--   trend - a straight line across the three years, so 2023 and 2025 differ
--   cycle - a triangle wave with a 365-day period, so the line is not a ramp
-- Both are computed in millionths and divided once at the end, so the result is
-- exact integer arithmetic rather than accumulated floating point.

WITH days AS (
    -- range(0, 1096) yields 0..1095: one number per day across three years.
    SELECT CAST(i AS INTEGER) AS d FROM range(0, 1096) AS t(i)
),
pairs AS (
    SELECT * FROM (
        VALUES
            ('USD', 1000000,   0,   0),
            ('EUR', 1080000,  40, 150),
            ('GBP', 1260000,  50, 180),
            ('CAD',  740000, -30, 120),
            ('AUD',  660000, -45, 140)
    ) AS c(from_currency, base_micro, trend_slope, cycle_slope)
),
shaped AS (
    SELECT
        d,
        d - 548                              AS trend,   -- -548 .. 547
        abs(((d * 2) % 730) - 365) - 182     AS cycle    -- -182 .. 183, 365-day period
    FROM days
)
SELECT
    (DATE '2023-01-01' + s.d)                AS rate_date,
    p.from_currency,
    'USD'                                    AS to_currency,
    CAST(
        (p.base_micro + s.trend * p.trend_slope + s.cycle * p.cycle_slope) / 1000000.0
        AS DECIMAL(12, 6)
    )                                        AS rate
FROM shaped s
CROSS JOIN pairs p
ORDER BY rate_date, from_currency;
