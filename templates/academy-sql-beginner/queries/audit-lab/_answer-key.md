# Audit lab - answer key

> Spoilers. This is the instructor and acceptance-test key for the audit lab. If
> you are taking the course, fill in `findings-template.md` before you read this.

Of the ten queries, **six are wrong (q02, q04, q05, q07, q08, q09)** and **four
are correct (q01, q03, q06, q10)**. Each wrong query fails in a different way, so
every failure class appears exactly once. All values below are exact and stable,
because the data is generated the same way on every run.

| Query | Verdict | Answer as written | Correct answer | Why |
|---|---|---|---|---|
| q01 | correct | 1,200 | 1,200 | A plain COUNT(*) on one table. No join, no filter, nothing to fan out. |
| q02 | **wrong** | London 261,246.42 (top row) | London 102,911.39 | `order_total` is an order-header value, but the join to `order_items` repeats each order once per line, so the sum is inflated. Fix: sum `order_total` from `orders` alone, without joining lines. |
| q03 | correct | 202 orders each, Paris 190 | same | Counts orders grouped by store. The join to `stores` only adds the city name; it does not change the grain, so `COUNT(*)` still counts orders. |
| q04 | **wrong** | 1,069 | 1,093 | `order_status != 'cancelled'` silently drops the 24 orders whose status is NULL, because a comparison with NULL is never true. Fix: `WHERE order_status IS DISTINCT FROM 'cancelled'`, or handle NULL explicitly. |
| q05 | **wrong** | 381,357.00 | 338,209.56 | `unit_price` is the catalogue price. Revenue is the price actually charged, which is `net_price`. Fix: `SUM(quantity * net_price)`. |
| q06 | correct | 503.39 | 503.39 | `AVG(order_total)` over `orders`. Every order has exactly one total and there is no join, so the average is over the right 1,200 rows. |
| q07 | **wrong** | 478 | 480 | `ordered_at` is a timestamp. `BETWEEN '2024-01-01' AND '2024-12-31'` stops at midnight on 31 Dec, so the two orders timestamped later that day are dropped. Fix: `ordered_at >= '2024-01-01' AND ordered_at < '2025-01-01'`. |
| q08 | **wrong** | 847,979.80 across 8 categories | 851,617.69 across 8 categories plus 'Unknown' | 15 order lines point at a `product_id` that is not in `products`. The INNER JOIN drops them, and 3,637.89 of revenue with them. Fix: LEFT JOIN and bucket the unmatched lines (e.g. category 'Unknown'). |
| q09 | **wrong** | 1,411.16 | 1,310.60 | Ten customers are duplicated in `customers`, so joining to it repeats those customers' orders and inflates `SUM(order_total)`. `COUNT(DISTINCT customer_id)` in the denominator is NOT inflated, so it looks safe while the numerator is wrong. Fix: aggregate revenue from orders before joining, or join to a de-duplicated customer list. |
| q10 | correct | 2.4 | 2.4 | Lines divided by distinct orders, both taken from `order_items` at its own grain. No join, no header measure. |

## Notes for the instructor

**q02 - the inflation factor is not a single number.** Across the whole table the
join inflates `SUM(order_total)` by exactly 2.4x, because that is the average
number of lines per order. Per store it ranges from **2.31x (Toronto) to 2.54x
(London)**, because the orders that happen to have four lines are not spread
evenly across stores. A student who divides one city's inflated figure by its
correct figure will get something near 2.4 but not equal to it, and that is the
right answer, not an error. The lesson is that fan-out multiplies each order by
*its own* line count, not by the average.

**q07 - the gap is deliberately tiny.** Only two of the 480 orders in 2024 fall
on 31 December after midnight, so the wrong answer is 478 against a correct 480.
That is the point: this is the kind of error nobody notices. Do not enlarge it.

**q09 - the question fixes the denominator on purpose.** It asks about customers
who have placed at least one order, so `COUNT(DISTINCT o.customer_id)` is the
right denominator and the only defect is the inflated numerator. If a student
argues the denominator should be all 304 consumer-segment customers, they have
read the question rather than the query - worth saying so, but it is not the
planted failure.

## How the acceptance test uses this

- All ten queries must execute without error: 10 of 10.
- Exactly six must return an answer that differs from the correct answer: q02,
  q04, q05, q07, q08, q09.
- The four correct queries (q01, q03, q06, q10) must match the correct answer.

If a change to the generator moves any number above, either the generator change
was unintended, or this key and the course pages that quote these numbers need
updating together.
