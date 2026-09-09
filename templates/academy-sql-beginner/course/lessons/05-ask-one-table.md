# Lesson 05: ask-one-table

## Objectives
- Read one table with SELECT, WHERE, ORDER BY, and LIMIT.
- Find the silent failure a `!=` filter causes on NULLs.

## Concepts to teach
`queries/01-first-look.sql` is your worksheet. A `SELECT` reads columns `FROM` a table;
`WHERE` keeps only some rows; `ORDER BY` sorts; `LIMIT` stops after the first few. One
row of that result is one order. Run it, then work the numbered variations in the file
one change at a time - predict each result before you run it.

The trap is at the end. NULL means "no value here" - not zero, not empty, just absent.
You cannot compare with NULL: `order_status != 'cancelled'` is neither true nor false
for a NULL status, it is "unknown", and `WHERE` keeps only rows where the test is
*true*. So `!= 'cancelled'` silently drops every order whose status is NULL. In this
data, 24 orders have a NULL `order_status`. `IS NULL` and `IS NOT NULL` are the only
tests that work on a NULL.

## Quiz
1. Q: Why does `WHERE order_status != 'cancelled'` drop the 24 NULL-status orders?
   A: A comparison with NULL is "unknown", never true, and WHERE keeps only rows where the condition is true - so NULL rows fall out silently.
2. Q: `ordered_at` is a timestamp. How should you keep only 2024 orders?
   A: With a half-open range: `ordered_at >= '2024-01-01' AND ordered_at < '2025-01-01'`.

## Task
In `queries/01-first-look.sql`, count the orders whose status is not `'completed'`
using `!=`, then separately count the orders where `order_status IS NULL`. Show that the
`!=` count leaves the NULL orders out, and state how many orders it silently dropped.

## Rubric (for `review my work`)
- [ ] The student ran both counts and saw that `!= 'completed'` excludes the NULL-status rows.
- [ ] They identify that a `!=` (or `NOT IN`) filter silently drops the 24 NULL `order_status` orders, and know `IS NULL` / `IS NOT NULL` are the only tests that work on NULL.

## Done signal
Confirm the student can name the exact silent failure: `!= 'cancelled'` drops 24 NULL-status orders. Carry forward: every filter throws rows away - always ask which ones.
