# Lesson 02: ddl-dml-and-approval

## Objectives
- Distinguish DDL from DML and read the risk of each operation.
- Classify operations by reversibility and approval level.
- Explain `full_refresh_restricted` and `strategy: ddl`.

## Concepts to teach
DDL changes structure (`CREATE`, `ALTER`, `DROP`, `TRUNCATE`); DML changes rows (`INSERT`, `UPDATE`, `DELETE`, `MERGE`). A wrong `SELECT` wastes time, while an unbounded `UPDATE` or `DROP` changes or removes data. Create and insert in a dev schema are cheap to redo; in-place changes need a proposal and human approval.

Bruin's `strategy: ddl` describes a table from its columns without a query body. `full_refresh_restricted` protects a table from a destructive rebuild. These are controls around operations, not a substitute for a database permission boundary.

## Quiz
1. Q: Why is `MERGE` higher risk than a `SELECT`?
   A: It changes existing rows and can insert or update the wrong keys if its match condition is wrong.
2. Q: What does `full_refresh_restricted` protect against?
   A: It blocks a full refresh of a protected asset.
3. Q: Which changes may be autonomous in a dev schema?
   A: A bounded `CREATE` or `INSERT` that can be safely discarded and rebuilt.

## Task
Write `docs/operation-policy.md` with a table covering `SELECT`, `CREATE`, `INSERT`, `ALTER`, `MERGE`, `UPDATE`, `DELETE`, `TRUNCATE`, and `DROP`. Include what happens if each is wrong, whether it is reversible, and whether an agent may run it. Do not run the statements.

## Rubric (for `review my work`)
- [ ] Covers all nine named operations exactly once.
- [ ] Marks `DROP`, `DELETE`, and `TRUNCATE` as data-loss risks requiring no autonomous execution.
- [ ] Marks dev `CREATE`/`INSERT` as bounded and redoable, and in-place `ALTER`/`MERGE`/`UPDATE` as proposal-only.

## Done signal
You can classify a proposed command by its blast radius before running it. Carry forward: the next lesson applies the same discipline to the pipeline graph.
