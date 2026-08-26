# Data design

This file is for whoever maintains the generator assets, not for students. It
records how the sample data is built and, more importantly, **why it must not be
changed casually.**

## Read this before you touch a generator

The beginner, intermediate, and advanced courses hard-code roughly fifty expected
numbers - row counts, revenue totals, defect counts, audit-lab answers. Those
numbers are published on the course pages and baked into the acceptance tests. A
single changed row can break a published page.

**If you "improve" a generator, you will almost certainly break those numbers.**
Before changing anything in `pipeline/assets/`, re-run the acceptance
checks (see the beginner template spec, and `queries/audit-lab/answer-key.md`) and
update every number that moves, in lockstep, across the repo and the course. If
you are not prepared to do that, do not change the generators.

## House rules: determinism

The same data must be generated on every machine, every run, forever. To make that
true, every generator obeys these rules:

1. **No `random()`, `hash()`, or `md5()`.** Only `range()` and integer arithmetic
   on the row number. Hash functions are not guaranteed stable across DuckDB major
   versions; arithmetic is.
2. **No `now()`, `current_date`, `current_timestamp`, or `today()`.** Every date is
   an absolute literal or an offset from one. The data must not change tomorrow.
3. **No dependence on `--start-date` / `--end-date`.** The generated data is fixed;
   only downstream incremental assets (in the later courses) care about run dates.
4. **Explicit `ORDER BY`** wherever row order could affect a result.
5. **Scramble through a large prime before folding into a small range.** This is
   the rule that is easiest to get wrong, and getting it wrong is what makes
   generated data look generated. See the section below.
6. **`range()` returns BIGINT.** DuckDB has no `DATE + BIGINT`, so `i` is cast to
   INTEGER in every generator that adds it to a date.
7. **Hardcode the small dimensions.** `stores` (6) and `products` (60) are literal
   `VALUES` lists so the names read like real ones. `customers` (500) indexes short
   hardcoded name and city lists by arithmetic, which keeps the file small.
8. **Prices come from the dimension, not from thin air.** `order_items.unit_price`
   and `order_items.unit_cost` are read from `products`, so the column names
   mean what they say and a student who joins the two tables sees consistent
   numbers.
9. **Money is DECIMAL, counters are INTEGER.** Dividing in DuckDB gives you a
   DOUBLE, and a DOUBLE prints `6635.2699999999995` at a student who is being
   taught to trust numbers. Cast every money column to `DECIMAL(n, 2)` and every
   small counter to `INTEGER` on the way out, and check the result with
   `information_schema.columns` rather than assuming.
10. **No schema prefix.** Assets sit directly in `pipeline/assets/` with
    single-segment names, so the tables land in DuckDB's default schema. That is
    what makes `SHOW TABLES` work for a beginner, and it is why every query in the
    course reads `FROM orders` rather than `FROM retail.orders`.

There is a determinism acceptance test: generate into two fresh databases and
compare a checksum of every table. It exists to catch anyone reintroducing
nondeterminism. Keep it green.

## Rule 5 in detail: why `(i * m) % 6` is a trap

The obvious way to spread a row number across six stores is `1 + (i * 7) % 6`. It
looks fine. It is not, and coprimality is not enough to save it:

- **Any multiplier coprime with 6 is congruent to 1 or 5 mod 6.** `7 % 6 = 1`, so
  `(i * 7) % 6` is just `i % 6`. The "scramble" does nothing.
- **A period of 6 shares factors with every other small period.** Line counts keyed
  on `i % 10` share the factor 2, so stores split into two groups with different
  average line counts. Order status keyed on `i % 3` shares the factor 3, so two of
  the six stores had *no completed orders at all* and four had no shipped orders.
  A defect injected on `i % 50` landed on only three of the six stores.
- **Straight-line values give themselves away.** `(i * m) % M` is linear in `i`, so
  if you sort by another linear column the values march up or down by a constant
  step. In an earlier version, `SELECT ... FROM orders ORDER BY ordered_at`
  - the very first query in the course - returned order totals falling by exactly
  35.48 on every row.

**The rule.** Compute a scrambled counter `(i * p) % q` where `q` is a prime
comfortably larger than the range you want, then fold that into the range with a
second modulo. `q` being prime means it shares no factors with the small periods
used elsewhere, and the double fold breaks the arithmetic progression. Give each
column its own `(p, q)` pair so columns stay independent of one another:

```sql
-- Wrong: period 6, locked to anything keyed on 2 or 3.
1 + (i * 7) % 6                AS store_id

-- Right: period 101, independent of the % 3 and % 10 keys used elsewhere.
1 + ((i * 47) % 101) % 6       AS store_id
```

For a value that must not look like a ramp when sorted by something else, add two
scrambles with different moduli. The sum is not linear in `i` under either
modulus, so no sort order produces a constant step:

```sql
(((i * 617) % 1013) + ((i * 89) % 251)) % 950   AS k_total
```

**How to check a change.** After editing a generator, run these four:

- Every store has every order status, and roughly the same lines per order.
- Every product sells at least once, and every customer list value appears.
- `SELECT ... ORDER BY ordered_at LIMIT 20` shows no constant step in any column.
- The defect counts are still 24 / 57 / 15 / 10.

## The intended shape

Uniform modulo alone gives a flat, boring dataset. The shape below is deliberate.
Each property earns its place by serving a lesson; do not flatten it.

### Order volume ramps, then dips

`orders` maps row numbers to dates non-uniformly:

- **2023: 360 orders. 2024: 480 (the peak). 2025: 360.** Clear year-over-year
  movement up then down, so the intermediate course's `LAG` and YoY exercises have
  a real signal.
- **Within 2024, Q3 dips to 60 orders** (Q1/Q2/Q4 are 140 each). A visible slump
  for the window-function lessons to find.

The ramp is built by choosing how many row numbers fall in each period, and then
scattering within the period with `k_day`. No randomness.

### Revenue concentrates on a few customers

Every fourth order is assigned to one of 50 "regular" customers (`1 + k_regular %
50`); the rest spread across customers 1-460. So a handful of customers carry a lot
of the revenue and "who are our top customers?" has a real answer. The duplicated
customers (ids 1-10) sit inside that regular pool, which makes the capstone's
dimension fan-out visible.

### 40 customers have never ordered

The general pool stops at 460, so customers 461-500 have no orders at all. Step 10
asks what happens to a number when a customer has no orders; without these rows the
question has nothing to point at, and a LEFT JOIN would look identical to an INNER
JOIN.

### One category dominates, and there is a long tail

Electronics carries about 46% of revenue off only 9% of the lines, because its
catalogue prices are about ten times the median. Apparel is the volume driver at
24% of revenue, and the remaining six categories run from 15% down to 1%. That is
what a real retail mix looks like, and it is what leaves 104 of the 416
category-weeks in 2024 empty - the problem the date-spine lesson in the
intermediate course exists to fix.

## Which lesson depends on which property

| Property | Depended on by |
|---|---|
| Two grains: orders vs order_items (2.4x) | Beginner Step 6, capstone, audit q02 |
| `unit_price` differs from `net_price` | Beginner Step 5, audit q05 |
| `order_total` is a header measure | Beginner Step 6, audit q02 |
| 8 categories, 20 subcategories, 12 countries | Beginner Step 5 (GROUP BY) |
| 40 customers with no orders | Beginner Step 6 and 10 (LEFT vs INNER JOIN) |
| Volume ramp + Q3 2024 dip | Intermediate (YoY, LAG) |
| Thin categories -> empty category-weeks | Intermediate (date spine) |
| Revenue concentration | Beginner (top customers) |
| `ordered_at` vs `_loaded_at` | Advanced (business vs ingestion time) |
| 36 months of dated data | Advanced (incremental, backfill) |

## Scrambled counters, at a glance

Each is `(row_number * p) % q` with `q` prime, folded into its range with a second
modulo (see rule 5):

- **Orders**, keyed on `i`: `k_day = (i*137)%379`, `k_store = (i*47)%101`,
  `k_customer = (i*313)%503`, `k_regular = (i*53)%97`, `k_hour = (i*29)%73`,
  `k_promise = (i*61)%131`, and `k_total = ((i*617)%1013 + (i*89)%251) % 950`.
- **Customers**, keyed on `i`: `k_first = (i*29)%101`, `k_last = (i*47)%103`,
  `k_location = (i*61)%107`, `k_segment = (i*83)%109`, `k_signup = (i*313)%1499`.
- **Order items**, keyed on the per-line counter `s`: lines per order from
  `(order_id*31)%113`, `k_product = (s*71)%103`, `k_quantity = (s*23)%59`,
  `k_discount = (s*37)%89`.

The defect conditions stay keyed on the plain counter, because their counts have to
be exact: `i % 50 = 0` for NULL `order_status` (24 orders), `s % 50 = 0` for NULL
`unit_cost` (57 lines), `s % 190 = 0` for the orphan `product_id` (15 lines).

`s` is a stable per-line counter (`ROW_NUMBER() OVER (ORDER BY order_id,
line_number)`), which is what makes the defect counts exact rather than
approximate.
