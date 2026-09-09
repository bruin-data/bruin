# Lesson 08: name-your-steps

## Objectives
- Break a query into named steps with a CTE (WITH).
- State the order the database actually runs a query in.

## Concepts to teach
`queries/04-cte.sql` is the worksheet. A **CTE** is a named, temporary result you define
with `WITH` and then use like a table. It lets you do one step at a time instead of
nesting everything. The example rolls `order_items` up to one row per order first, then
joins that to `orders` - because the first step already has one row per order, the join
does not fan out. Aggregating to the right grain first is the usual fix for the
double-counting trap from the join lesson (join-without-breaking).

The order you write a query is not the order it runs. The logical order is: `FROM`
(pick and join tables), `WHERE` (drop rows), `GROUP BY` (form groups), `HAVING` (drop
groups), `SELECT` (compute output columns), `ORDER BY` (sort), `LIMIT` (cut short). One
thing falls straight out of that: `WHERE` runs before `GROUP BY`, so it cannot see an
aggregate - that is why filtering on a `COUNT` needs `HAVING`, not `WHERE`.

## Quiz
1. Q: Why does joining the `order_revenue` CTE to `orders` not fan out?
   A: The CTE already has one row per order, so the join is one-to-one on `order_id`; the grain does not change.
2. Q: `WHERE COUNT(*) > 100` errors but `HAVING COUNT(*) > 100` works. Why?
   A: WHERE runs before GROUP BY, when no groups or counts exist yet; HAVING runs after grouping, so it can filter on an aggregate.

## Task
In `queries/04-cte.sql`, refactor the fan-out from the previous lesson into two steps: a
CTE that aggregates `order_items` to one row per order, then a join to `orders`. Confirm
it no longer inflates. Then write, in one line, the logical run order of a query.

## Rubric (for `review my work`)
- [ ] The query uses a correct `WITH` structure that aggregates to one row per order before joining, and does not fan out.
- [ ] The student states the run order (FROM, WHERE, GROUP BY, HAVING, SELECT, ORDER BY, LIMIT) and can explain why WHERE cannot filter on an aggregate.

## Done signal
Confirm the CTE removes the inflation and the student can recite the run order. Carry forward: naming each step makes a wrong grain visible before it reaches a total.
