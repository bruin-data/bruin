# Lesson 04: meet-the-warehouse

## Objectives
- Read `docs/schema.md` and grasp what "grain" means.
- State the grain of `orders` versus `order_items`, and predict what a join between them does.

## Concepts to teach
The **grain** of a table is what one row of it stands for. Say it out loud before you
trust any query - getting the grain wrong is the most common way to produce a number
that looks right and is not. Read `docs/schema.md`: it lists all six tables, their
columns, and how they relate.

Two grains matter and they are easy to confuse. `orders` is one row per order (1,200
rows). `order_items` is one row per line on an order (2,880 rows), averaging 2.4 lines
per order. When you join them, each order row is repeated once for every line it has -
so a value that belongs to the order, like `order_total`, gets counted 2.4 times over
on average. That is the fan-out you will meet properly in the join lesson.

There is a third trap sitting in the schema: `customers` has 510 rows for 500
customers, because ten ids are duplicated. Note it now; you do not need it yet.

## Quiz
1. Q: What is the grain of `orders`, and of `order_items`?
   A: `orders` is one row per order; `order_items` is one row per order line.
2. Q: If you join `orders` to `order_items` and sum `order_total`, what happens and why?
   A: `order_total` is an order-level value, but the join repeats each order once per line (about 2.4x), so the sum is inflated by roughly 2.4 times.

## Task
From `docs/schema.md` alone (no queries yet), write one sentence stating the grain of
`orders`, one for `order_items`, and one sentence predicting what happens to
`SUM(order_total)` if you join the two tables.

## Rubric (for `review my work`)
- [ ] Names both grains correctly: one order per row, one order line per row.
- [ ] Predicts the fan-out risk: summing an order-header column across the join inflates it by roughly 2.4x.

## Done signal
Confirm the student can state both grains and anticipate fan-out before running anything. Carry forward: "what does one row represent?" is the first question to ask of every result.
