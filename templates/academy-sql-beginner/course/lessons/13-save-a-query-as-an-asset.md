# Lesson 13: save-a-query-as-an-asset

## Objectives
- Turn an audited query into a Bruin asset with a header, deps, and a description.
- Validate and run it.

## Concepts to teach
Everything in `queries/` is a scratch file: you run it, read the answer, nothing is
kept. An **asset** is the query the project keeps, names, and rebuilds on command, so
the answer becomes a table other queries can use. Read `docs/writing-an-asset.md` for
the format - it is a `.sql` file in `pipeline/assets/` with a `@bruin` comment block on
top: `name`, `type: duckdb.sql`, a `description` of what one row means, `depends` on the
assets it reads, and a `materialization`.

Two things earn their place. `depends` lists the assets that must be built first, by
asset name, not file path - that is what lets Bruin build in the right order. The
`description` is where you write down what you learned auditing the query, for the next
person and the next agent. Validate before you run, every time: `validate` catches a
typo in the metadata in a tenth of a second.

## Quiz
1. Q: What is the difference between a file in `queries/` and an asset in `pipeline/assets/`?
   A: A `queries/` file is a scratch query you run and discard; an asset is kept, named, and rebuilt, producing a table other assets can depend on.
2. Q: What does `depends` control, and how do you write its entries?
   A: The build order - which assets must exist first - written as asset names, not file paths.

## Task
Promote an audited query (from the agent lessons, corrected) into a new asset in
`pipeline/assets/`. Give it a `name`, a `description` of what one row means, and the
right `depends`. Then run:

```bash
bruin validate academy-sql-beginner/pipeline
bruin run academy-sql-beginner/pipeline/assets/<your-asset>.sql
```

## Rubric (for `review my work`)
- [ ] A valid asset exists in `pipeline/assets/`: it passes `bruin validate` and runs successfully.
- [ ] It has a real `description` (what one row means) and correct `depends` on the assets it reads.

## Done signal
Confirm the asset validates, runs, and has a meaningful description and dependencies. Carry forward: an audited query worth keeping belongs in an asset, where its meaning is written down.
