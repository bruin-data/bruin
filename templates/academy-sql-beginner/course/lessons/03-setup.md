# Lesson 03: setup

## Objectives
- Have a working project with the sample data built.
- Confirm the two headline row counts by hand.

## Concepts to teach
The project builds its own data: six SQL assets in `pipeline/assets/` generate the
tables when you run the pipeline, and they produce the exact same rows on every
machine. Nothing to download. If setup already ran the pipeline for you, this lesson is
about proving it worked rather than trusting that it did.

Build (or rebuild) the data from the folder where you ran `bruin init`:

```bash
bruin run academy-sql-beginner/pipeline
```

Then count the two tables the rest of the course leans on. `bruin query` runs a query
and prints the result; `--description` says why you ran it, which is the convention
here:

```bash
bruin query --connection duckdb-default --description "count orders" \
  --query "SELECT COUNT(*) FROM orders"

bruin query --connection duckdb-default --description "count order lines" \
  --query "SELECT COUNT(*) FROM order_items"
```

## Quiz
1. Q: Why can this course quote exact numbers you can reproduce?
   A: The data is generated deterministically by plain arithmetic - the same rows on every run and every machine.
2. Q: What does `--description` do, and why bother?
   A: It records why the query was run. It is the project convention, and it is a habit worth keeping so a reader (including an agent) knows the intent behind a query.

## Task
Run the two counts above and read the numbers. Confirm `orders` returns 1,200 and
`order_items` returns 2,880.

## Rubric (for `review my work`)
- [ ] The student ran the counts and reports `orders` = 1,200 and `order_items` = 2,880.
- [ ] If either number differs, the pipeline did not build - re-run it before moving on.

## Done signal
Confirm both counts match (1,200 and 2,880). Carry forward: 1,200 orders and 2,880 lines are your first two anchors - numbers you have checked and can measure later answers against.
