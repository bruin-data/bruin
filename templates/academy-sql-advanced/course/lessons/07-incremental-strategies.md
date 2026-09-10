# Lesson 07: incremental-strategies

## Objectives
- Compare Bruin's table strategies and their failure modes.
- Keep business time and ingestion time distinct.
- Convert the weekly mart and prove idempotence.

## Concepts to teach
`create+replace` rebuilds everything, `append` duplicates on rerun, `delete+insert` replaces rows for an incremental key, `truncate+insert` preserves table structure while rewriting all rows, and `merge` needs a truly unique primary key. `time_interval` bounds a time-keyed replacement. SCD2 strategies preserve changing dimension history and need a full-refresh first run.

For this data, `ordered_at` is business time and `_loaded_at` is ingestion time. Replace business-time partitions while filtering changed source rows by ingestion time. Confusing them either misses late data or rewrites the wrong period.

## Quiz
1. Q: What is the central two-timestamp rule?
   A: Use `_loaded_at` to find newly learned rows and `ordered_at` to define the business partitions they replace.
2. Q: What does append do when the same range runs twice?
   A: It adds the rows again unless a separate deduplication step prevents it.
3. Q: What does an SCD2 table preserve?
   A: Historical versions of a dimension row with validity metadata.

## Task
Convert `weekly_category_revenue` from `create+replace` to a suitable incremental strategy. Explain the strategy and both timestamps in `docs/incremental-decision.md`. Run the same date range twice and record row count and revenue total after each run.

## Rubric (for `review my work`)
- [ ] The decision names `ordered_at` as business time and `_loaded_at` as ingestion time.
- [ ] The same range run twice has identical row count and revenue total on both runs.
- [ ] The chosen strategy declares its required incremental metadata and remains valid for a date range.

## Done signal
You can make reruns safe by matching the strategy to the grain and timestamps. Carry forward: late ingestion tests whether that choice is really correct.
