/* @bruin
name: orders
type: duckdb.sql

description: "Customer orders."

materialization:
  type: table
  strategy: create+replace

columns:
  - name: order_id
    type: integer
  - name: customer_id
    type: integer
  - name: store_id
    type: integer
  - name: ordered_at
    type: timestamp
  - name: ordered_at_utc
    type: timestamptz
  - name: _loaded_at
    type: timestamp
  - name: promised_delivery_date
    type: date
  - name: currency_code
    type: varchar
  - name: order_status
    type: varchar
  - name: order_total
    type: decimal
@bruin */

-- Deterministic generation. Every value is a pure function of the row number i.
-- Do not introduce random(), now(), or current_date - see docs/_data-design.md.
--
-- Order volume ramps up from 2023 to a peak in 2024 (with a deliberate dip in
-- Q3 2024) and eases off in 2025. That shape is what makes the year-over-year
-- and window-function exercises non-trivial. The ramp is built purely by
-- mapping row numbers to dates non-uniformly.

WITH seq AS (
    -- range(1, 1201) yields 1..1200: one number per order.
    -- range() produces BIGINT; cast to INTEGER so DATE + offset type-checks.
    SELECT CAST(i AS INTEGER) AS i FROM range(1, 1201) AS t(i)
),
scrambled AS (
    -- Every column below is derived from one of these scrambled counters rather
    -- than from i directly. Taking (i * prime) % prime first, and only then
    -- folding into the range you want, is what keeps the columns independent of
    -- each other. A plain (i * m) % 6 repeats every six rows, so it locks step
    -- with anything else keyed on 2 or 3 - see rule 5 in docs/_data-design.md.
    SELECT
        i,
        (i * 137) % 379  AS k_day,      -- which day in the period
        (i * 47)  % 101  AS k_store,    -- which store
        (i * 313) % 503  AS k_customer, -- which customer, general pool
        (i * 53)  % 97   AS k_regular,  -- which customer, regulars pool
        (i * 29)  % 73   AS k_hour,     -- hour of day
        (i * 61)  % 131  AS k_promise,  -- whether a delivery date was promised
        (i * 83)  % 127  AS k_spelling, -- how the completed status is written
        -- Two scrambles added together, so order_total is not a straight line in
        -- i. Without this, sorting orders by date walks order_total up or down by
        -- a constant step and the whole dataset reads as machine-made.
        (((i * 617) % 1013) + ((i * 89) % 251)) % 950 AS k_total
    FROM seq
),
placed AS (
    SELECT
        i,
        k_total,
        k_promise,
        k_spelling,
        1 + k_store % 6                                             AS home_store_id,
        -- Every fourth order goes to one of fifty regulars, so revenue
        -- concentrates on a handful of customers and "top customers" is a
        -- meaningful question. The rest spread across all 500.
        -- The general pool stops at 460 on purpose, so customers 461..500 never
        -- place an order. A LEFT JOIN from customers to orders therefore has
        -- something to show, which an INNER JOIN would hide.
        -- Every 240th order carries a customer_id that was never issued.
        CASE
            WHEN i % 240 = 0 THEN 9001
            WHEN i % 4   = 0 THEN 1 + k_regular % 50
            ELSE                  1 + k_customer % 460
        END                                                         AS customer_id,
        -- Map i to a day offset from 2023-01-01, non-uniformly, to shape volume.
        (CASE
            WHEN i BETWEEN 791 AND 794 THEN DATE '2024-03-30' + (i - 791)
            WHEN i BETWEEN 795 AND 798 THEN DATE '2024-10-26' + (i - 795)
            ELSE DATE '2023-01-01' +
                CASE
                    WHEN i <= 360 THEN k_day % 365                      -- 2023: 360 orders
                    WHEN i <= 840 THEN 365 +                            -- 2024: 480 orders (peak)
                        CASE
                            WHEN i - 360 <= 140 THEN       k_day % 91      -- Q1
                            WHEN i - 360 <= 280 THEN  91 + k_day % 91      -- Q2
                            WHEN i - 360 <= 340 THEN 182 + k_day % 92      -- Q3 (the dip: 60 orders)
                            ELSE                     274 + k_day % 92      -- Q4
                        END
                    ELSE 731 + k_day % 365                              -- 2025: 360 orders
                END
        END) + (CASE WHEN i BETWEEN 791 AND 798 THEN
                         CASE WHEN (i - 791) % 3 = 0 THEN INTERVAL '23' HOUR
                              ELSE ((i - 791) % 2) * INTERVAL '1' HOUR END
                     ELSE (k_hour % 24) * INTERVAL '1' HOUR END)        AS ordered_at,
        CASE 1 + k_store % 6
            WHEN 1 THEN 'America/New_York'
            WHEN 2 THEN 'Europe/London'
            WHEN 3 THEN 'Europe/Berlin'
            WHEN 4 THEN 'America/Toronto'
            WHEN 5 THEN 'Australia/Sydney'
            ELSE 'America/Los_Angeles'
        END                                                         AS store_timezone
    FROM scrambled
),
rendered AS (
    SELECT
        i                                                              AS order_id,
        customer_id,
        -- Every 97th order was rung up on a till that never made it into the
        -- store list.
        CASE WHEN i % 97 = 0 THEN 7 ELSE home_store_id END             AS store_id,
        ordered_at,
        -- About 80% of rows carry a UTC conversion. The rest are deliberately
        -- missing so the reconstruction exercise is real.
        CASE WHEN i % 5 = 0 THEN NULL
             ELSE ordered_at AT TIME ZONE store_timezone
        END                                                            AS ordered_at_utc,
        -- Thirty-five 2024 rows arrive ten days late and five arrive 120 days
        -- late. The latter proves that a seven-day lookback is only a mitigation.
        CASE
            WHEN i BETWEEN 801 AND 831 THEN ordered_at + INTERVAL '10' DAY
            WHEN i BETWEEN 832 AND 836 THEN ordered_at + INTERVAL '120' DAY
            WHEN i % 31 = 0 THEN ordered_at + INTERVAL '3' DAY
            ELSE ordered_at + (((i * 5) % 72) * INTERVAL '1' HOUR)
        END                                                            AS _loaded_at,
        -- Missing on a small number of orders. Keyed on k_promise so it does not
        -- land only on regulars' orders.
        CASE WHEN k_promise < 3
             THEN NULL
             ELSE CAST(ordered_at AS DATE) + (2 + (i * 3) % 7)
        END                                                            AS promised_delivery_date,
        -- Currency follows the store's country. The phantom till keeps the
        -- currency of the store whose numbering slot it took.
        CASE home_store_id
            WHEN 1 THEN 'USD'
            WHEN 2 THEN 'GBP'
            WHEN 3 THEN 'EUR'
            WHEN 4 THEN 'CAD'
            WHEN 5 THEN 'AUD'
            ELSE        'USD'
        END                                                            AS currency_code,
        -- order_status is empty on every 50th order. The finished state is
        -- written four different ways: three of them differ only in case, and
        -- the fourth is a different word.
        CASE
            WHEN i BETWEEN 1180 AND 1200 THEN NULL
            WHEN i % 50 = 0 THEN NULL
            WHEN i % 11 = 0 THEN 'cancelled'
            WHEN i % 17 = 0 THEN 'returned'
            WHEN i % 7  = 0 THEN 'processing'
            WHEN i % 3  = 0 THEN 'shipped'
            ELSE CASE k_spelling % 4
                     WHEN 0 THEN 'completed'
                     WHEN 1 THEN 'Completed'
                     WHEN 2 THEN 'COMPLETED'
                     ELSE        'complete'
                 END
        END                                                            AS order_status,
        -- Header-level total in the order's currency. Spread across 50.00 .. 999.99.
        -- Deliberately NOT the sum of the order's lines - it is an order-level figure.
        -- Cast to DECIMAL, not left as the DOUBLE that dividing would produce.
        CAST(((50 + k_total) * 100 + (i * 41) % 100) / 100.0 AS DECIMAL(10, 2)) AS order_total
    FROM placed
),
-- Twelve orders were sent a second time by the source system, two days later,
-- with the status changed. Both rows survive because they are not identical.
resent AS (
    SELECT
        order_id,
        customer_id,
        store_id,
        ordered_at,
        ordered_at_utc,
        _loaded_at + INTERVAL '48' HOUR                                AS _loaded_at,
        promised_delivery_date,
        currency_code,
        CASE WHEN order_status = 'cancelled' THEN 'returned' ELSE 'cancelled' END AS order_status,
        order_total
    FROM rendered
    WHERE order_id % 100 = 37
)
SELECT * FROM rendered
UNION ALL
SELECT * FROM resent
ORDER BY order_id, _loaded_at;
