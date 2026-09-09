# Lesson 04: stage-the-work

## Objectives
- Restructure a query into one named CTE per logical step.
- Write a grain comment above each CTE.
- Prove a restructured query returns the same rows as the original.

## Concepts to teach
Query structure is a reviewability decision, not a style preference. A CTE is not free-standing
syntax sugar - it is the unit a reviewer, or an agent reading the file six months from now, checks
one at a time. Two rules make that checking possible. First, one CTE per logical step: filter, then
join, then aggregate, then rank, each as its own named block rather than folded together. Second,
name each CTE after what it produces, not after its position in the query. `orders_2024` says what
rows are in it. `t1` says nothing. A CTE that changes the grain - that turns "one row per order line"
into "one row per customer" - has to say so in its name: `orders_per_customer`, not `joined`. A name
that hides a grain change is the single most common reason a reviewer misses a fan-out.

The file the student will restructure is `queries/reading-drill/drill-2.sql`. It answers a real
question - monthly line revenue per category in 2024, compared with the month before - correctly.
Its three CTEs are named `t1`, `t2`, and `final`. The logic does not need to change; the names and
the comments do. Filters belong early: `t1` in that file already filters to 2024 before joining
anything, which is the right instinct and worth pointing out even while renaming it.

Contrast this with a nested subquery three levels deep, doing the same filter, join, and aggregate
as inline subqueries instead of named CTEs. The logic can be identical. The difference is that a CTE
chain reads top to bottom, one grain at a time, while a nested subquery makes a reviewer hold three
levels of parentheses in their head before they can say what a single row means. Restructuring is
not decoration on top of correct SQL - it is what makes correct SQL checkable by someone who did not
write it, including the agent that will read this file again next lesson.

## Quiz
1. Q: Why is `orders_per_customer` a better CTE name than `joined` for a step that aggregates order lines up to one row per customer?
   A: Because it states the grain change directly - a reviewer knows before reading the CTE's body that it produces one row per customer, rather than having to infer it from the `GROUP BY`.
2. Q: `drill-2.sql` filters to 2024 in its first CTE, before any join. Why does that ordering matter?
   A: Because filtering early shrinks every step that follows and makes the boundary condition visible in one place, instead of buried in a `WHERE` clause after several joins where it is easy to miss or duplicate.
3. Q: You restructure a query's CTE names and add grain comments but change nothing else. What must you still verify before calling the job done?
   A: That the restructured query returns exactly the same rows as the original - renaming and commenting should never change the result, and the only way to be sure is to run both and compare.

## Task
Restructure `queries/reading-drill/drill-2.sql` into named steps: rename `t1`, `t2`, and `final` to
names that describe what each produces, and add a one-line comment above each CTE stating what one
row of it represents. Do not change the logic. Save the result as
`queries/reading-drill/drill-2-restructured.sql`, then run both files and confirm they return
identical rows.

## Rubric (for `review my work`)
- [ ] `queries/reading-drill/drill-2-restructured.sql` exists.
- [ ] No CTE in the restructured file is named `t1`, `t2`, or `final`.
- [ ] Every CTE in the restructured file has a one-line grain comment directly above it.
- [ ] Running `drill-2.sql` and `drill-2-restructured.sql` returns identical rows.

## Done signal
Confirm the restructured file exists, no CTE carries a placeholder name, and the student ran both
versions and reported that the rows matched. Carry forward: a query you cannot check in under two
minutes is a query you cannot trust an agent's edits to either.
