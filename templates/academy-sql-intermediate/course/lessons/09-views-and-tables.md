# Lesson 09: views-and-tables

## Objectives
- State the two conditions that make a view the right choice, and the two that make a table right.
- Write both forms of the `materialization` block from memory.
- Convert two staging assets to views without changing a single row count.

## Concepts to teach
`materialization: type: table` with `strategy: create+replace`, the block every asset in
`pipeline/assets/staging/` already carries, drops the table and rebuilds it from scratch on every
run. `materialization: type: view` skips the storage step entirely - a view is a saved query, run
fresh every time something reads it. Both are one decision, made per asset, and the two questions
that decide it are cost and freshness, not preference.

A view is right when the underlying query is cheap, the result needs to always reflect the latest
data, and the transformation is thin - a rename, a cast, a filter. Every asset in this project's
staging layer fits that description: none of the source tables holds more than 5,480 rows, so
recomputing on every read costs nothing worth naming. A table is right when the query is expensive
to recompute, when several downstream assets reuse the same result, or when something
latency-sensitive - a dashboard, an interactive query - reads it repeatedly and cannot wait for the
transformation to run each time.

Getting it wrong costs something in both directions. A table where a view belonged holds
yesterday's numbers until the next scheduled run replaces them, with no error and nothing telling a
reader the data is stale - it is wrong quietly. A view where a table belonged recomputes its full
query on every single read, so a query that would cost nothing once now costs it every time
something asks, and on a bigger table than this project's that cost compounds fast.

```yaml
materialization:
  type: view
```

```yaml
materialization:
  type: table
  strategy: create+replace
```

Beyond `create+replace`, Bruin supports incremental strategies - `delete+insert`, `merge`,
`append`, among others - that update only the rows that changed instead of rebuilding the whole
table. Name them here and stop: the advanced course covers when and how to use them.

## Quiz
1. Q: Which two questions decide whether an asset should be a view or a table?
   A: Whether the query is cheap or expensive to recompute, and whether the result needs to always be fresh or can tolerate the age of the last run - a view answers cheap-and-fresh, a table answers expensive-or-reused.
2. Q: A staging asset is a view. Nobody has run the pipeline in three days. What does querying that asset return?
   A: The latest data available in the source tables it reads, computed at query time - a view has no age of its own, since it recomputes on every read instead of storing a stale copy.
3. Q: Name two materialization strategies besides `create+replace` that update a table incrementally.
   A: Any two of `delete+insert`, `truncate+insert`, `append`, or `merge` - the advanced course covers when each applies.

## Task
Pick two of the staging assets built in lesson 8 and change their `materialization` block from
`type: table` with `strategy: create+replace` to `type: view`. Run `bruin validate`, then run the
pipeline again and confirm each asset's row count is unchanged from lesson 8's after-staging count.
For each of the two, write one sentence saying why a view is the right call there. Then write one
sentence naming a different asset in this project you would keep as a table, and why.

## Rubric (for `review my work`)
- [ ] Exactly two staging assets carry `materialization: type: view`, with no `strategy` key.
- [ ] `bruin validate` passes after the change.
- [ ] Each converted asset's row count still matches its lesson 8 after-staging number exactly (`dates` 1,096; `stores` 6; `products` 60; `customers` 500; `orders` 1,200; `order_items` 2,880; `fx_rates` 5,480 - whichever two were chosen).
- [ ] Both reasons name cost or freshness, not a stated preference.
- [ ] One further asset in this project is named as one to keep as a table, with a reason about reuse or latency.

## Done signal
Confirm the two assets validate as views, the row counts match lesson 8 exactly, and both reasons
given are about cost or freshness rather than taste. Carry forward: the materialization block is a
decision you make per asset, not a default you leave alone.
