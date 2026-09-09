/* @bruin
name: order_items
type: duckdb.sql

description: "Lines on an order."

depends:
  - orders
  - products

materialization:
  type: table
  strategy: create+replace

columns:
  - name: order_id
    type: integer
  - name: line_number
    type: integer
  - name: product_id
    type: integer
  - name: quantity
    type: integer
  - name: unit_price
    type: decimal
  - name: net_price
    type: decimal
  - name: unit_cost
    type: decimal
@bruin */

-- Deterministic generation. Every value is a pure function of a per-line number.
-- Lines are generated from orders, so the fan-out is real, and prices are read
-- from products, so unit_price and unit_cost mean what their names say.
-- Do not introduce random(), now(), or current_date - see docs/_data-design.md.

WITH order_keys AS (
    -- DISTINCT, because orders carries a second copy of some order_ids and lines
    -- must not be generated twice for them.
    SELECT DISTINCT order_id FROM orders
),
order_line_counts AS (
    -- How many lines each order gets. Keyed on a scrambled order_id rather than
    -- on order_id % 10, so the line count does not move in lockstep with the
    -- every-50th-order status gap - see rule 5 in docs/_data-design.md.
    -- The cuts give 19% one-line orders, 32% two, 41% three and 9% four, which
    -- averages to exactly 2.4 lines across the 1,200 orders.
    SELECT
        order_id,
        CASE
            WHEN (order_id * 31) % 113 <  21 THEN 1
            WHEN (order_id * 31) % 113 <  57 THEN 2
            WHEN (order_id * 31) % 113 < 103 THEN 3
            ELSE                                  4
        END AS n_lines
    FROM order_keys
),
exploded AS (
    -- Turn one order into n_lines rows, numbered 1..n_lines.
    SELECT o.order_id, ln.line_number
    FROM order_line_counts o
    JOIN range(1, 5) AS ln(line_number) ON ln.line_number <= o.n_lines
),
numbered AS (
    -- s is a stable 1..N line counter used to drive every value below.
    SELECT
        order_id,
        line_number,
        ROW_NUMBER() OVER (ORDER BY order_id, line_number) AS s
    FROM exploded
),
chosen AS (
    SELECT
        order_id,
        line_number,
        s,
        -- Scrambled counters again, one per column, so which product a line is
        -- for is independent of how many units it is for.
        (s * 71) % 103 AS k_product,
        (s * 23) % 59  AS k_quantity,
        (s * 37) % 89  AS k_discount,
        CASE
            -- Every 190th line points at 9999, which is not a real product.
            WHEN s % 190 = 0 THEN 9999
            -- Electronics sells in low volume at a high price, Apparel is the
            -- volume driver, and everything else fills in behind them. The
            -- thinner categories are what leave some category-weeks empty.
            -- Every one of the 60 products still sells at least once.
            WHEN s % 11 = 0 THEN 1  + ((s * 71) % 103) % 12  -- Electronics (ids 1..12)
            WHEN s % 3  = 0 THEN 22 + ((s * 71) % 103) % 10  -- Apparel (ids 22..31)
            ELSE                13 + ((s * 71) % 103) % 48   -- everything but Electronics (ids 13..60)
        END AS product_id
    FROM numbered
),
priced AS (
    -- LEFT JOIN, not INNER: the lines pointing at 9999 have no product row, and
    -- dropping them here would delete something the course depends on. They fall
    -- back to a fixed catalogue price and cost.
    SELECT
        c.order_id,
        c.line_number,
        c.s,
        c.product_id,
        1 + c.k_quantity % 4                    AS quantity,
        COALESCE(p.list_price, 249.00)          AS unit_price,
        COALESCE(p.unit_cost,  112.00)          AS base_cost,
        c.k_discount % 25                       AS discount_pct
    FROM chosen c
    LEFT JOIN products p ON c.product_id = p.product_id
),
rendered AS (
    SELECT
        s,
        order_id,
        CAST(line_number AS INTEGER) AS line_number,
        CAST(product_id AS INTEGER)  AS product_id,
        CAST(quantity AS INTEGER)    AS quantity,
        unit_price,
        -- Discount of 0..24 percent off the catalogue price. net_price is the real one.
        CAST(ROUND(unit_price * (100 - discount_pct) / 100.0, 2) AS DECIMAL(10, 2)) AS net_price,
        -- unit_cost is empty on every 50th line.
        CASE WHEN s % 50 = 0 THEN NULL ELSE base_cost END   AS unit_cost
    FROM priced
),
-- Every 192nd line was loaded twice, byte for byte. Nothing on the row
-- distinguishes the copy from the original.
replayed AS (
    SELECT * FROM rendered WHERE s % 192 = 0
)
SELECT order_id, line_number, product_id, quantity, unit_price, net_price, unit_cost
FROM (
    SELECT * FROM rendered
    UNION ALL
    SELECT * FROM replayed
) AS all_lines
ORDER BY order_id, line_number, product_id;
