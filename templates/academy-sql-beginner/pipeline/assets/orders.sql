/* @bruin
name: orders
type: duckdb.sql

description: >-
  One row per customer order. Generated sample data for the Agentic Data Analysis
  course. 1,200 orders across 2023-01-01 to 2025-12-31. Deterministic: the same
  rows are produced on every run.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: order_id
    type: integer
    description: "Unique identifier for the order. One row per order_id."
    primary_key: true
    checks:
      - name: unique
      - name: not_null
  - name: customer_id
    type: integer
    description: "Joins to customers.customer_id."
  - name: store_id
    type: integer
    description: "Joins to stores.store_id."
  - name: ordered_at
    type: timestamp
    description: "When the order was placed, in store-local time."
  - name: _loaded_at
    type: timestamp
    description: "When this row was recorded by the source system. Not the same as ordered_at."
  - name: promised_delivery_date
    type: date
    description: "Delivery date promised at order time. Null on a small number of orders."
  - name: currency_code
    type: varchar
    description: >-
      Which currency the order was priced in: USD, EUR, GBP, CAD or AUD. This is a
      label only. The amounts are NOT converted - the same product costs 699.00 in
      every one of them - so summing revenue across currencies needs no exchange
      rate. See docs/schema.md.
  - name: order_status
    type: varchar
    description: >-
      Order lifecycle status. Null on 2 percent of rows, so a not-equals filter
      on this column will silently exclude them.
  - name: order_total
    type: decimal
    description: >-
      Order-level total. Two warnings. It is a header value, so summing it after
      joining to order_items will multiply it by the number of lines on each
      order. And it does NOT equal the sum of the order's lines - it is supplied
      independently, so 604,065.00 here against 851,617.69 of line revenue is
      expected, not a bug.
@bruin */

-- Deterministic generation. Every value is a pure function of the row number i.
-- Do not introduce random(), now(), or current_date - see docs/data-design.md.
--
-- Order volume ramps up from 2023 to a peak in 2024 (with a deliberate dip in
-- Q3 2024) and eases off in 2025. That shape is what makes the year-over-year
-- and window-function exercises in the later courses non-trivial. The ramp is
-- built purely by mapping row numbers to dates non-uniformly.

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
    -- with anything else keyed on 2 or 3 - see rule 5 in docs/data-design.md.
    SELECT
        i,
        (i * 137) % 379  AS k_day,      -- which day in the period
        (i * 47)  % 101  AS k_store,    -- which store
        (i * 313) % 503  AS k_customer, -- which customer, general pool
        (i * 53)  % 97   AS k_regular,  -- which customer, regulars pool
        (i * 29)  % 73   AS k_hour,     -- hour of day
        (i * 61)  % 131  AS k_promise,  -- whether a delivery date was promised
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
        1 + k_store % 6                                             AS store_id,
        -- Every fourth order goes to one of fifty regulars, so revenue
        -- concentrates on a handful of customers and "top customers" is a
        -- meaningful question. The rest spread across all 500.
        -- The general pool stops at 460 on purpose, so customers 461..500 never
        -- place an order. A LEFT JOIN from customers to orders therefore has
        -- something to show, which an INNER JOIN would hide.
        CASE WHEN i % 4 = 0
             THEN 1 + k_regular % 50
             ELSE 1 + k_customer % 460
        END                                                         AS customer_id,
        -- Map i to a day offset from 2023-01-01, non-uniformly, to shape volume.
        (DATE '2023-01-01' +
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
        ) + ((k_hour % 24) * INTERVAL '1' HOUR)                     AS ordered_at
    FROM scrambled
)
SELECT
    i                                                              AS order_id,
    customer_id,
    store_id,
    ordered_at,
    -- Recorded a little after the order was placed, so it differs from ordered_at.
    ordered_at + (((i * 5) % 72) * INTERVAL '1' HOUR)              AS _loaded_at,
    -- Missing on a small number of orders, a plausible gap rather than a taught
    -- defect. Keyed on k_promise so it does not land only on regulars' orders.
    CASE WHEN k_promise < 3
         THEN NULL
         ELSE CAST(ordered_at AS DATE) + (2 + (i * 3) % 7)
    END                                                            AS promised_delivery_date,
    -- Currency follows the store's country.
    CASE store_id
        WHEN 1 THEN 'USD'
        WHEN 2 THEN 'GBP'
        WHEN 3 THEN 'EUR'
        WHEN 4 THEN 'CAD'
        WHEN 5 THEN 'AUD'
        ELSE        'EUR'
    END                                                            AS currency_code,
    -- Defect 1: order_status is null on every 50th order (24 orders). A filter
    -- written as order_status != 'cancelled' silently drops these rows.
    CASE
        WHEN i % 50 = 0 THEN NULL
        WHEN i % 11 = 0 THEN 'cancelled'
        WHEN i % 17 = 0 THEN 'returned'
        WHEN i % 7  = 0 THEN 'processing'
        WHEN i % 3  = 0 THEN 'shipped'
        ELSE                'completed'
    END                                                            AS order_status,
    -- Header-level total in the order's currency. Spread across 50.00 .. 999.99.
    -- Deliberately NOT the sum of the order's lines - it is an order-level figure.
    -- Cast to DECIMAL, not left as the DOUBLE that dividing would produce.
    -- Money in floating point prints 6635.2699999999995 at a student who is
    -- being taught to trust numbers.
    CAST(((50 + k_total) * 100 + (i * 41) % 100) / 100.0 AS DECIMAL(10, 2)) AS order_total
FROM placed
ORDER BY order_id;
