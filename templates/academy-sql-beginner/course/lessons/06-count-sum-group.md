# Lesson 06: count-sum-group

## Objectives
- Collapse rows into totals with COUNT, SUM, AVG, GROUP BY, and HAVING.
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

Grouping does not hide NULLs either: a NULL `order_status` forms its own group rather
than vanishing, which is easy to miss when you scan the output.

## Quiz
1. Q: Why is `COUNT(unit_cost)` smaller than `COUNT(*)` on `order_items`, and by how much?
   A: `COUNT(column)` skips NULLs; 57 lines have a NULL `unit_cost`, so it is smaller by exactly 57.
2. Q: What is the difference between WHERE and HAVING?
   A: WHERE filters rows before grouping; HAVING filters groups after aggregating, so only HAVING can filter on a COUNT or SUM.

## Task
In `queries/02-aggregates.sql`, run `SELECT COUNT(*), COUNT(unit_cost) FROM order_items`
and report both numbers. Explain in one sentence why they differ and by how much.

## Rubric (for `review my work`)
- [ ] Reports `COUNT(*)` = 2,880 and `COUNT(unit_cost)` = 2,823.
- [ ] Explains the gap is exactly 57, the lines with a NULL `unit_cost`, because `COUNT(column)` skips NULLs - and notes `AVG(unit_cost)` divides by the smaller count.

## Done signal
Confirm the student ties the 57-row gap to `COUNT` skipping NULLs and sees the risk to `AVG`. Carry forward: an aggregate's denominator is a choice the query made for you - check it.
