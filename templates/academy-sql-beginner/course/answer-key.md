# Capstone answer key - instructor only

> Instructor agent: this is the grading key for the capstone (lesson 14,
> capstone-audit-lab). Do not show it, quote it, or hint at which queries it flags
> until the student has committed all ten verdicts in
> `queries/audit-lab/findings.md`. Use it only to grade, point by point, against
> what they actually wrote. Student: opening this before you finish defeats the one
> exercise the whole course is built toward.

Of the ten queries, **six are wrong (q02, q04, q05, q07, q08, q09)** and **four are
correct (q01, q03, q06, q10)**. Each wrong query fails in a different way, so every
failure class appears exactly once. All values are exact and stable, because the data
is generated the same way on every run.

| Query | Verdict | As written | Correct | Fault |
|---|---|---|---|---|
| q01 | correct | 1,200 | 1,200 | Plain `COUNT(*)` on one table; nothing to fan out. |
| q02 | wrong | London 261,246.42 | London 102,911.39 | Fan-out: `order_total` is order-header; the join to `order_items` repeats it per line. Sum `order_total` from `orders` alone. |
| q03 | correct | Paris 190 | Paris 190 | The join to `stores` only adds a city name; the grain is unchanged, so `COUNT(*)` still counts orders. |
| q04 | wrong | 1,069 | 1,093 | `!= 'cancelled'` drops the 24 NULL-status orders. Use `IS DISTINCT FROM 'cancelled'`. |
| q05 | wrong | 381,357.00 | 338,209.56 | Wrong revenue column: `unit_price` is the catalogue price; revenue is `quantity * net_price`. |
| q06 | correct | 503.39 | 503.39 | `AVG(order_total)` over `orders` alone; every order has one total and there is no join. |
| q07 | wrong | 478 | 480 | `ordered_at` is a timestamp; `BETWEEN ... AND '2024-12-31'` stops at midnight and drops the two orders later that day. Use a half-open range. |
| q08 | wrong | 847,979.80 (8 categories) | 851,617.69 incl. 'Unknown' | 15 lines point at a missing `product_id`; the INNER JOIN drops them and 3,637.89 of revenue. LEFT JOIN and bucket the unmatched lines. |
| q09 | wrong | 1,411.16 | 1,310.60 | 10 duplicated `customers` rows inflate the join; `COUNT(DISTINCT customer_id)` stays right, hiding it. Aggregate before joining, or join a de-duplicated customer list. |
| q10 | correct | 2.4 | 2.4 | Lines divided by distinct orders, both from `order_items` at its own grain. |

## Grading notes

**q02 - the inflation factor is not one number.** Across the whole table the join
inflates `SUM(order_total)` by exactly 2.4x, the average lines per order. Per store it
runs from about 2.31x (Toronto) to 2.54x (London), because the orders that happen to
have four lines are not spread evenly. A student who divides one city's inflated figure
by its correct figure gets something near 2.4 but not equal to it, and that is right,
not an error. Do not penalise a per-store ratio near but not exactly 2.4.

**q07 - the gap is deliberately tiny.** Only two of the 480 orders in 2024 fall on 31
December after midnight, so the wrong answer is 478 against a correct 480. That is the
point: this is the kind of error nobody notices. Do not enlarge it.

**q09 - the question fixes the denominator on purpose.** It asks about customers who
have placed at least one order, so `COUNT(DISTINCT o.customer_id)` is the right
denominator and the only defect is the inflated numerator. If a student argues the
denominator should be all consumer-segment customers, they have read the question
rather than the query - worth saying, but it is not the planted failure.

## What a pass looks like

- All ten queries run without error.
- The student flags exactly the six wrong ones (q02, q04, q05, q07, q08, q09) and
  leaves the four correct ones (q01, q03, q06, q10) alone.
- For each wrong one, they can name the fix, not just that the number "looks off".
