# Lesson 06: count-sum-group

## Objectives
- Collapse rows into totals with COUNT, SUM, AVG, GROUP BY, and HAVING.
- Tell apart the three counts: `COUNT(*)`, `COUNT(column)`, and `COUNT(DISTINCT column)`.
- See how NULLs quietly change what an aggregate counts.

## Concepts to teach
`queries/02-aggregates.sql` is the worksheet. Aggregate functions collapse many rows
into one number; `GROUP BY` runs the aggregate once per group; `HAVING` filters groups
after aggregating, where `WHERE` filters rows before. One row of the sample query is one
`order_status`.

The catch is what an aggregate does with NULL. `COUNT(*)` counts rows. `COUNT(a_column)`
counts only rows where that column is not NULL. Run both against `order_items`:
`COUNT(*)` is 2,880, but `COUNT(unit_cost)` is 57 lower, because 57 lines have a NULL
`unit_cost`. `AVG` is the dangerous one - it divides by the count of non-NULL values,
not by the number of rows, so the denominator is not the one you assumed.

A third count answers a different question: `COUNT(DISTINCT x)` counts how many
*different* values a column has, not how many rows. `SELECT COUNT(*),
COUNT(DISTINCT order_id) FROM order_items` returns 2,880 and 1,200 - the 2,880 lines
belong to only 1,200 distinct orders. This is the count the capstone turns on: when a
join duplicates rows, `COUNT(DISTINCT customer_id)` still returns the right number of
customers even while a `SUM` over the same join is inflated, so the denominator can
look trustworthy while the numerator is wrong.

Grouping does not hide NULLs either: a NULL `order_status` forms its own group rather
than vanishing, which is easy to miss when you scan the output.

## Quiz
1. Q: Why is `COUNT(unit_cost)` smaller than `COUNT(*)` on `order_items`, and by how much?
   A: `COUNT(column)` skips NULLs; 57 lines have a NULL `unit_cost`, so it is smaller by exactly 57.
2. Q: What is the difference between WHERE and HAVING?
   A: WHERE filters rows before grouping; HAVING filters groups after aggregating, so only HAVING can filter on a COUNT or SUM.
3. Q: On `order_items`, what do `COUNT(*)`, `COUNT(unit_cost)`, and `COUNT(DISTINCT order_id)` each count?
   A: `COUNT(*)` counts all rows (2,880); `COUNT(unit_cost)` counts rows where `unit_cost` is not NULL (2,823); `COUNT(DISTINCT order_id)` counts distinct order ids (1,200).

## Task
In `queries/02-aggregates.sql`, run
`SELECT COUNT(*), COUNT(unit_cost), COUNT(DISTINCT order_id) FROM order_items` and report
all three numbers. Explain in one sentence what each one counts.

## Rubric (for `review my work`)
- [ ] Reports `COUNT(*)` = 2,880, `COUNT(unit_cost)` = 2,823, and `COUNT(DISTINCT order_id)` = 1,200.
- [ ] Can state what each of the three counts returns: all rows, non-NULL values in a column, and distinct values.
- [ ] Explains the 57-row gap between `COUNT(*)` and `COUNT(unit_cost)` as the NULL `unit_cost` lines that `COUNT(column)` skips - and notes `AVG(unit_cost)` divides by the smaller count.

## Done signal
Confirm the student ties the 57-row gap to `COUNT` skipping NULLs and sees the risk to `AVG`. Carry forward: an aggregate's denominator is a choice the query made for you - check it.
