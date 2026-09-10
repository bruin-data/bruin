# Lesson 06: patterns-agents-get-wrong

## Objectives
- Choose `QUALIFY ROW_NUMBER()` or `DISTINCT` correctly for a given duplicate problem, and say why the other one fails.
- Build a date spine so a group with no rows still appears as zero.
- State the half-open date range rule and the aggregate-before-join rule from memory.

## Concepts to teach
Four patterns, each one a failure mode an agent produces confidently and without an error message.

**Deduplication** is the most important thing in this course, because this project ships two kinds
of duplicate and only one of them responds to `DISTINCT`. `order_items` has 15 rows that are
byte-identical copies of other rows - `SELECT DISTINCT *` removes exactly these and nothing else,
and they are worth 7,278.04 of double-counted line revenue if left in. `orders` has a different
problem: 12 `order_id` values appear twice, but the two copies differ - a different `order_status`,
and a `_loaded_at` two days later on the resend. `DISTINCT` does not fix this, because the rows are
not identical; it would keep both. The correct tool is `QUALIFY ROW_NUMBER() OVER (PARTITION BY
order_id ORDER BY _loaded_at DESC) = 1`, which keeps the latest version of each order and discards
the rest. These 12 orders are worth 18,518.92 of double-counted line revenue if both copies survive
a join. Knowing which of the two you are looking at - identical rows, or same key with different
payload - decides which fix is correct.

**Date spines** fix a quieter failure: a `GROUP BY` only returns rows that exist, so a
category-week with zero orders is missing from the result rather than present at zero. A chart built
on that result lies by omission - a gap reads as "no data" when it should read as "zero." In 2023
this project has 8 categories across 53 week starts, which is 424 category-week cells; only 296 of
them have any sales, so 128 cells are silently missing, spread across 51 of the 53 weeks. The fix is
to build a spine - every category crossed with every week, from `dates` - and `LEFT JOIN` the actual
sales onto it, so a missing cell becomes an explicit zero instead of a missing row.

Fifty-three, not fifty-two, and the reason is worth a minute. 2023-01-01 was a Sunday, so
`date_trunc('week', ...)` puts it in the week beginning 2022-12-26. Take the weeks of 2023 as
`SELECT DISTINCT CAST(date_trunc('week', date_day) AS DATE) FROM dates WHERE year = 2023` and you
get 53 week starts, from 2022-12-26 to 2023-12-25. A spine boundary is a decision, the same as a
metric definition, and a spine that silently drops the partial week at either end is one more way to
lose rows you never counted.

**Half-open date ranges** - `>= start AND < end`, always, never `BETWEEN` on a timestamp column.
`BETWEEN '2023-01-01' AND '2023-12-31'` on a `TIMESTAMP` column silently excludes every order placed
after midnight on December 31st, because `BETWEEN` treats the upper bound as `2023-12-31 00:00:00`,
not the whole day. The half-open form has no such trap: the boundary is unambiguous no matter what
time component the column carries.

**Aggregate before you join** is the general fix behind the FX case in this lesson. `fx_rates` has
5 currency pairs for every date. There are two different mistakes to keep apart:

```sql
-- Incorrect: date-only join; every order matches all five pairs for that date.
ON CAST(o.ordered_at AS DATE) = f.rate_date
```

That casted date-only join returns 6,060 rows and multiplies every order line by 5 - 2023 line
revenue goes from the correct **264,926.21** unconverted to **1,324,631.05**, exactly 5 times too
high, because the join fanned out before the sum ran. A literal raw timestamp-to-date join,
`ON o.ordered_at = f.rate_date`, is a separate type-mismatch problem; in this data it returns only
335 rows. It is not the explanation for the 5x fan-out.

```sql
-- Correct: one matching rate for the order date, source currency, and USD target.
ON CAST(o.ordered_at AS DATE) = f.rate_date
AND o.currency_code = f.from_currency
AND f.to_currency = 'USD'
```

Converting properly with all three predicates gives **258,645.94** in USD for 2023. The rule that
generalises past FX: if a join is one-to-many and you need to sum something from the "one" side,
aggregate first, then join the already-aggregated result - never sum after a fan-out join and hope
the multiplication cancels out.

## Quiz
1. Q: `order_items` and `orders` each have duplicate rows, but they need different fixes. What is the fix for each, and why does the other one not work for the other table?
   A: `order_items`'s 15 duplicates are byte-identical, so `SELECT DISTINCT *` removes them correctly. `orders`'s 12 duplicates share an `order_id` but differ in `order_status` and `_loaded_at`, so `DISTINCT` would keep both non-identical rows; only `QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1` picks the one to keep.
2. Q: A weekly revenue chart has a gap in the middle. Is the gap more likely a missing-data problem or a missing-row problem, and what fixes it?
   A: Almost always a missing-row problem - the week had genuinely zero orders for that category, so it never appeared in the `GROUP BY` output. Building a spine from `dates` and `LEFT JOIN`ing the sales onto it turns the gap into an explicit zero.
3. Q: Why does the casted date-only join to `fx_rates` multiply revenue by exactly 5 rather than giving a wrong-but-plausible number, and what is a separate timestamp/date mistake?
   A: Because `fx_rates` carries 5 currency pairs for every date, so each order line joins to 5 rows instead of 1, and summing after that join sums each line 5 times over. Comparing the raw `ordered_at` timestamp directly to `rate_date` is a separate type-mismatch problem; it returns only 335 rows here rather than the 6,060-row casted date-only join.

## Task
Fix `queries/weekly-category.sql` from lesson 5 so that a category-week with no sales appears as
zero rather than being missing from the result - build a spine of every category crossed with every
week of 2023 from `dates`, and `LEFT JOIN` the revenue onto it. Then, in the conversation, state
which of this project's two duplicate problems `DISTINCT` correctly fixes and which one it does not,
and why.

## Rubric (for `review my work`)
- [ ] The spine is built as `SELECT DISTINCT CAST(date_trunc('week', date_day) AS DATE) FROM dates WHERE year = 2023`, giving 53 week starts from 2022-12-26 to 2023-12-25.
- [ ] The fixed query returns 424 rows for 2023 - 8 categories times 53 weeks - with no category-week missing, of which 128 carry zero.
- [ ] The student names 7,278.04 (or the 15 duplicate rows behind it) as the `order_items` problem `DISTINCT` fixes.
- [ ] The student names 18,518.92 (or the 12 duplicate `order_id` values behind it) as the `orders` problem `DISTINCT` does not fix.
- [ ] The student states the `QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1` clause correctly.

## Done signal
Confirm the query returns exactly 424 rows for 2023 and the student can say, without looking it up,
which duplicate problem is a `DISTINCT` fix and which is a `QUALIFY` fix. Carry forward: from here on,
every query you write or review should default to a half-open date range and an aggregate-before-join
shape unless there is a stated reason not to.
