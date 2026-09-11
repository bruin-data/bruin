# Lesson 09: sargability-and-cost

## Objectives
- Compare a sargable range predicate with a functional predicate.
- Separate local DuckDB evidence from warehouse cost claims.
- Set a query limit and budget for agent exploration.

## Concepts to teach
`ordered_at >= '2024-01-01' AND ordered_at < '2025-01-01'` exposes a range; `YEAR(ordered_at) = 2024` applies a function to every row and can defeat pruning. In this built project the sargable scan reads 1,140 rows while the functional form reads 2,762. The same issue appears with `LOWER(email)`. Local DuckDB can show plans and rows scanned, but this small project is not a warehouse-cost benchmark.

On the optional MotherDuck path, partition and cluster declarations can be compared. Bytes scanned are a BigQuery billing unit, and Bruin's documented incremental benchmark drops from 1.92 GB to 10.5 MB, a 99.45% reduction. Label those as warehouse examples, not local measurements. Agents also create N+1 loops and unbounded exploration, so use `--limit` and a three-query budget.

## Quiz
1. Q: Why is a half-open date range a good predicate?
   A: It expresses an exact boundary without applying a function to the indexed or partitioned column.
2. Q: Does DuckDB's scan count equal a BigQuery bill?
   A: No. It demonstrates predicate mechanics locally, not warehouse bytes or price.
3. Q: What is an N+1 query pattern?
   A: One query fetches keys and then one query per key runs instead of one set-based query.

## Task
Run `queries/ops/sargability-demo.sql`, capture the exact rows scanned for both forms in `docs/sargability.md`, and explain why the numbers are local evidence only. Keep the exploration under three queries and include the `--description` command used.

## Rubric (for `review my work`)
- [ ] Shows both predicates and the exact scan counts: 1,140 for the sargable range and 2,762 for the functional predicate.
- [ ] States which form scans fewer rows in this run and explains the difference without claiming warehouse billing equivalence.
- [ ] Records a query budget of 3 or fewer and uses `--limit` for exploration.

## Done signal
You can discuss query cost without pretending a small local table is a warehouse. Carry forward: the next lesson makes agent writes safe with an environment boundary.
