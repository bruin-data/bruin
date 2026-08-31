/* @bruin
name: order_items
type: duckdb.sql

description: >-
  One row per line on an order, 2.4 lines per order on average. Generated from
  orders and priced from products, so the fan-out ratio and the
  prices are both real. Generated sample data for the Agentic Data Analysis
  course. Deterministic: the same rows are produced on every run.

depends:
  - orders
  - products

materialization:
  type: table
  strategy: create+replace

columns:
  - name: order_id
    type: integer
    description: "Joins to orders.order_id. Several lines can share one order_id."
    checks:
      - name: not_null
  - name: line_number
    type: integer
    description: "Line position within the order, starting at 1."
    checks:
      - name: not_null
  - name: product_id
    type: integer
    description: >-
      Joins to products.product_id. A small number of lines point at a
      product_id that is not in products, so an inner join drops them.
  - name: quantity
    type: integer
    description: "Units of the product on this line."
  - name: unit_price
    type: decimal
    description: >-
      The product's catalogue price at the time of sale, copied from
      products.list_price. This is the price before discount, so it is
      usually higher than net_price and is NOT what revenue is calculated from.
  - name: net_price
    type: decimal
    description: >-
      Actual price charged per unit after discount. This, not unit_price, is the
      one to multiply by quantity for revenue.
  - name: unit_cost
    type: decimal
    description: >-
      What this unit cost the retailer, copied from products.unit_cost.
      Null on roughly one line in fifty, which changes an AVG denominator and
      makes COUNT(unit_cost) differ from COUNT(*).
@bruin */

-- Deterministic generation. Every value is a pure function of a per-line number.
-- Lines are generated from orders, so the 2.4x fan-out is real, and
-- prices are read from products, so unit_price and unit_cost mean what
-- their names say.
-- Do not introduce random(), now(), or current_date - see docs/data-design.md.

WITH order_line_counts AS (
    -- How many lines each order gets. Keyed on a scrambled order_id rather than
    -- on order_id % 10, so the line count does not move in lockstep with the
    -- every-50th-order status defect - see rule 5 in docs/data-design.md.
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
    FROM orders
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
            -- Defect 3: orphan product_id. 15 lines point at 9999, which is not
            -- a real product, so an INNER JOIN to products drops their
            -- revenue.
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
    -- LEFT JOIN, not INNER: the orphan lines have no product row, and dropping
    -- them here would delete the defect that Step 6 and the capstone rely on.
    -- They fall back to a fixed catalogue price and cost.
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
)
SELECT
    order_id,
    CAST(line_number AS INTEGER) AS line_number,
    CAST(product_id AS INTEGER)  AS product_id,
    CAST(quantity AS INTEGER)    AS quantity,
    unit_price,
    -- Discount of 0..24 percent off the catalogue price. net_price is the real one.
    CAST(ROUND(unit_price * (100 - discount_pct) / 100.0, 2) AS DECIMAL(10, 2)) AS net_price,
    -- Defect 2: unit_cost is null on every 50th line (57 lines).
    CASE WHEN s % 50 = 0 THEN NULL ELSE base_cost END   AS unit_cost
FROM priced
ORDER BY order_id, line_number;
