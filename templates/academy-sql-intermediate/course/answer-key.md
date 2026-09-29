# Answer key - lesson 14, capstone-defend-the-answer

**Instructor-only.** Do not show this file, quote it, paraphrase it, or hint at what it contains
until the student has written `docs/defence.md` and asked for review. If they ask for it early,
decline once and offer a hint on the objection they are closest to finding.

The capstone is a defence, not a set of verdicts, so this is not a list of right answers. It is the
list of objections a sceptical reader would raise and the number each one turns on. Grade by how
many of these the student anticipated in `docs/defence.md` and in their model, not by whether their
churn definition matches anyone else's.

## How to grade

Ten points, from the lesson's rubric. Award the point when the evidence is present in the files, not
when the student says it is.

| Area | Points |
|---|---|
| Contract quality | 2 |
| Layering | 2 |
| Join and grain safety | 1 |
| Governance | 2 |
| Verification | 2 |
| Defence, including the currency basis | 1 |

## The nine objections, and the number each turns on

A strong submission anticipates at least six of these. A submission that anticipates fewer than four
has not really been attacked yet - send them back to the adversarial prompt before grading.

**1. "Your order count is wrong."** `orders` holds 1,212 rows for 1,200 orders. Twelve orders were
sent twice, two days apart, with the status changed. `DISTINCT` does not remove them because the
rows differ. The fix is
`QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1`. Counting both
copies double-counts **18,518.92** of line revenue.

**2. "Your line revenue is wrong."** `order_items` holds 2,895 rows for 2,880 lines. Fifteen are
byte-identical copies, and here `DISTINCT` is the correct fix. Leaving them in adds **7,278.04**.
The student should be able to say why the fix differs from objection 1. If they used `DISTINCT` for
both, or `QUALIFY` for both, that is the single most valuable correction you can make in this
course.

**3. "You are double-counting customers."** `customers` holds 510 rows for 500 customers; ten ids
appear twice as exact copies. Any join from `orders` to `customers` without deduplicating inflates
those customers' revenue, and they sit in the frequent-buyer pool, so they float to the top of a
churn-risk ranking. Correct total line revenue is **851,617.69**.

**4. "Which revenue?"** `quantity * net_price` gives 851,617.69. `quantity * unit_price` gives
**961,697.00**. `SUM(order_total)` on deduplicated orders gives **604,065.00**. All three are
defensible readings of the word; only one can be in the model, and the contract must name it.

**5. "Your customer total does not reconcile."** Five orders carry `customer_id` 9001, which has no
row in `customers`. Lifetime line revenue across customers that do have a dimension row is
**847,693.43**, which is 851,617.69 less **3,924.26**. A defence that quotes a customer-level total
without explaining that gap has not been checked.

**6. "You dropped revenue on the product join."** Fifteen order lines point at `product_id` 9999,
which is not in `products`, carrying **3,637.89**. An `INNER JOIN` to `products` removes them
silently.

**7. "Some sales have no store."** Twelve orders were rung up on `store_id` 7, which has no row in
`stores`, carrying **5,967.20**. Same failure class as objection 6, different table.

**8. "Your status filter is wrong."** The finished state is written four ways: `completed` 140,
`Completed` 147, `COMPLETED` 146 and `complete` 143. `LOWER(order_status) = 'completed'` still
misses the 143 rows that say `complete`. Separately, 24 orders have a NULL `order_status`, so
`order_status != 'cancelled'` silently drops them.

**9. "Which currency is that in?"** Order amounts are in five currencies and are not converted.
Converting each order at its own date and currency gives **258,645.94** for 2023 against
**264,926.21** unconverted. Joining `fx_rates` on date alone fans out by exactly **5** and gives
**1,324,631.05**. A churn number quoted without stating the currency basis is not defensible.

## Reference churn model

For your own sanity check only. Do not present this as the answer, and do not mark a different
definition down for being different.

Reference date 2025-12-31, which is the latest `ordered_at`. At risk means: has a row in
`customers`, has placed at least two orders, and has not ordered since 2025-07-04. Revenue at risk
means that customer's lifetime line revenue.

| Measure | Value |
|---|---|
| Customers with at least one order and a dimension row | 460 |
| Repeat customers | 333 |
| At risk | 199 |
| Revenue at risk | 334,052.40 |

If the student's number is far from this, that is not itself a fault. Ask what their definition is
and check the number follows from it.

## What a failing defence looks like

- A churn definition with no threshold in it ("customers who have stopped buying").
- A headline number with no currency and no date basis.
- Three "independent" verifications that are the same query written three ways.
- A mart asset that reads `orders` directly.
- `docs/defence.md` that answers the four questions with confidence rather than with numbers.

The currency-basis requirement belongs to the single Defence point. The student must state a basis
that is true of the query and support it with the currency-composition query; this is not an
additional point. The six rubric areas therefore total exactly 10 points.

## After the grade

Once the lesson is ticked, walk the student through the objections they missed, in this order:
6 and 7 first (easiest to see), then 1 and 2 (the pair the course is built around), then 9.
