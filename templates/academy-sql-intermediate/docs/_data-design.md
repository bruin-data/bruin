# Data design (contributor record, not shipped)

**This file is deliberately excluded from the template.** Go's `//go:embed *` skips underscore-prefixed
files, so `bruin init academy-sql-intermediate` never writes it to a student's disk, and
`templates/templates.go` records why. Finding the defects listed here is the coursework for lessons
3, 6 and 8. Do not rename this file, and do not add it to the embed list.

`cmd/init_template_test.go` asserts both that it does not ship and that the contents below still
match the generated data.

## Determinism rules

Identical to the beginner template. In short:

1. No `random()`, `hash()`, `md5()`, `now()`, `current_date`, `current_timestamp` or `today()`.
   Only `range()` and integer arithmetic on the row number.
2. Every date is an absolute literal or an offset from one.
3. No dependence on `--start-date` or `--end-date`.
4. Explicit `ORDER BY` wherever row order could affect a result.
5. Scramble through a large prime before folding into a small range, and give every column its own
   `(p, q)` pair. `(i * 7) % 6` is just `i % 6`.
6. Hardcode the small dimensions so the rows read like real ones.
7. One test generates into two fresh databases and compares checksums.

## Base data

The generators are the beginner template's, unchanged in their arithmetic, so the underlying figures
match: 851,617.69 of line revenue and 604,065.00 of `order_total` across 1,200 orders and 2,880
lines. What changed is the defect set layered on top and the header metadata stripped off.

`fx_rates` is new. Rates are `(base_micro + trend * trend_slope + cycle * cycle_slope) / 1000000.0`,
where `trend = d - 548` is linear across the three years and
`cycle = abs(((d * 2) % 730) - 365) - 182` is a 365-day triangle wave. Both terms are integers, so
the division happens once and the result is exact. `USD` has both slopes at zero and is therefore a
constant 1.000000 identity row.

## The nine defects and where they are injected

| # | Defect | Injected in | Rule | Verified |
|---|---|---|---|---|
| 1 | NULL `order_status` | `orders.sql` | `i % 50 = 0` | 24 rows |
| 2 | NULL `unit_cost` | `order_items.sql` | `s % 50 = 0` | 57 rows |
| 3 | Orphan `product_id` | `order_items.sql` | `s % 190 = 0` sets 9999 | 15 lines |
| 4 | Orphan `customer_id` | `orders.sql` | `i % 240 = 0` sets 9001 | 5 rows |
| 5 | Duplicated `customer_id` | `customers.sql` | ids 1..10 unioned back in | 10 ids, 510 rows |
| 6 | Byte-identical duplicate lines | `order_items.sql` | `s % 192 = 0` unioned back in | 15 rows |
| 7 | Resent orders, later `_loaded_at`, changed status | `orders.sql` | `order_id % 100 = 37` unioned back in, `+48h`, status flipped | 12 ids, 1,212 rows |
| 8 | Casing and whitespace | `customers.sql`, `products.sql`, `orders.sql` | see below | see below |
| 9 | Sales on a store with no dimension row | `orders.sql` | `i % 97 = 0` sets `store_id` 7 | 12 rows |

The modulus sets are pairwise disjoint by construction:

- 50, 240 and 97 share no multiple below 1,201 with `i % 100 = 37`, whose members all end in 37.
- `lcm(190, 192) = 18,240` and `lcm(50, 192) = 4,800`, both above 2,880, so defects 2, 3 and 6 never
  land on the same line.

Check this again if you ever change a modulus.

### Defects 6 and 7 are the point of this template

Defect 6 is byte-identical rows: `SELECT DISTINCT` removes them and the answer is then right.
Defect 7 is the same `order_id` twice with a different `order_status` and a `_loaded_at` two days
later: `DISTINCT` keeps both, because they differ. The fix is
`QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1`.

Not deduplicating the resent orders double-counts 18,518.92 of line revenue. Not deduplicating the
replayed lines double-counts 7,278.04. Keep both defects, keep them distinguishable, and do not let
a future cleanup collapse them into one.

### Defect 8 in detail

| Column | Rule | Verified |
|---|---|---|
| `customers.country` | slot 1 of the location list is the home market, taking a quarter of the book; its country is written five ways by `k_spelling % 5` | `USA` 34, `usa` 29, `U.S.A.` 22, `USA ` 22, ` USA` 20, so 127 rows over 5 spellings |
| `customers.city` | `k_case % 40 = 0` uppercases, `= 1` lowercases | 31 rows, 20 distinct values for 12 cities |
| `products.brand` | `product_id` 29 gets a leading space, 58 a trailing one | 2 rows, 11 distinct values for 9 brands |
| `orders.order_status` | the completed branch splits on `k_spelling % 4` | `Completed` 147, `COMPLETED` 146, `complete` 143, `completed` 140 |

`complete` against `completed` is the one that matters. It is not a casing problem, so `LOWER()`
alone still misses 143 orders across the table and 43 within 2023.

`UPPER(TRIM(country))` folds 16 distinct values to 13, not to 12, because `U.S.A.` survives it. That
is deliberate: normalisation is a decision about which values are the same, not a function you can
apply and stop thinking.

## Intended shape

Do not flatten any of this:

- Order volume ramps 360 in 2023, 480 in 2024, 360 in 2025, with a deliberate dip in Q3 2024.
- Revenue concentrates: one order in four goes to a pool of fifty regulars.
- Customers 461 to 500 never order, so a `LEFT JOIN` from `customers` has something to show.
- Two categories dominate, so 128 of the 424 category-weeks in 2023 are empty and the date-spine
  lesson has something to fix.
- FX rates drift, so converting revenue changes an answer across years rather than scaling it.

## Every number the course quotes

The full harvested list lives with the pull request that added this template. The figures the lesson
rubrics depend on are:

| Figure | Value |
|---|---|
| Line revenue 2023 / 2024 / 2025 | 264,926.21 / 338,209.56 / 248,481.92 |
| Line revenue all years | 851,617.69 |
| `SUM(order_total)`, deduped | 604,065.00 |
| Completed orders in 2023, all four spellings | 169 |
| Distinct 2023 customers with a dimension row | 268 |
| Top country by 2023 revenue | USA, 53,828.71 |
| Highest month of 2023 | December, 29,547.65 |
| 2023 revenue in USD | 258,645.94 |
| Same, joined on date alone | 1,324,631.05, exactly 5x |
| Largest weekly drop in 2023 | Electronics, week of 2023-11-20, -6,193.24 |
| Empty category-weeks in 2023 | 128 of 424, over 53 week starts |
| Orphan-product line revenue | 3,637.89 over 15 lines |
| `drill-3.sql` as written / grain fixed | 373,614.70 / 348,521.23 |
