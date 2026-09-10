# Lesson 08: late-data-and-backfills

## Objectives
- Quantify late rows and identify moved business-time partitions.
- Explain why a naive ordered-time filter misses them.
- Propose a bounded backfill and a reconciliation check.

## Concepts to teach
The source has 1,212 order rows. Five rows arrive more than 30 days after their business date, and all five are more than 90 days late; the concentrated 2024 batch exposes the difference between when an order happened and when it arrived. A seven-day lookback catches some late data but cannot catch the five extreme rows.

Filter on `_loaded_at` to discover new rows, then let Bruin's incremental materialization replace the corresponding `ordered_at` partitions. This is a description of the strategy, not permission to issue a raw `DELETE`; the project guardrail forbids destructive statements. A bounded repair would use `bruin run --start-date 2024-10-01 --end-date 2024-12-31 pipeline`, while `bruin backfill` can orchestrate partitioned chunks. `interval_modifiers` can add a single-unit lookback, but `1h30m` is not a valid single unit and requires `--apply-interval-modifiers`.

## Quiz
1. Q: Why does filtering only on `ordered_at >= last_run` miss a late row?
   A: The business date is before the watermark even though ingestion happened after it.
2. Q: How many source order rows are more than 90 days late?
   A: Exactly 5.
3. Q: Is a seven-day lookback a complete fix?
   A: No. It is a mitigation; the five rows over 90 days prove that a bounded lookback can still miss data.

## Task
Use `queries/ops/late-arrival-audit.sql` and a monthly comparison to identify every moved 2024 month, its revenue delta, the five rows over 90 days, and the backfill command. Record the findings in `docs/late-data-findings.md` without running the proposed backfill.

## Rubric (for `review my work`)
- [ ] Reports exactly 5 order rows with delay greater than 90 days.
- [ ] Reports exactly 5 rows over 30 days and lists the moved months and deltas: 2024-10 = 10,681.87, 2024-11 = 12,493.10, and 2024-12 = 6,030.35.
- [ ] Explains `_loaded_at` discovery, `ordered_at` partition replacement, and gives both date flags for the proposed backfill.

## Done signal
You can separate discovery time from business time and repair a bounded history. Carry forward: the next lesson asks whether the filter itself lets the engine prune work.
