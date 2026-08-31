# How queries fail

Read this first. It is the idea the whole course is built on, and it takes two
minutes.

## Loud failures and silent failures

A **loud failure** is when the query does not run. You typed `SELCT`, or named a
column that does not exist, and the database refuses and tells you why. Loud
failures are annoying and completely harmless: you cannot act on an answer you
never got.

A **silent failure** is when the query runs perfectly, returns a tidy number, and
the number is wrong. Nothing is red. Nothing warns you. You put it in a report.

A syntax error costs you a minute. A silent failure can cost you a quarter, because
by the time anyone notices, decisions have been made on it.

Every trap in this course is a silent failure. That is why the course spends its
time on reading and checking queries rather than on writing them faster.

## The one word you need before Step 4: NULL

**NULL means "no value here."** Not zero, not an empty string, not "unknown to
you" - the database is recording that it does not have this fact. An order whose
`order_status` is NULL is not an order with a status of `"NULL"`; it is an order
where nobody wrote a status down.

Three things follow, and they cause most of the silent failures in this dataset:

**1. You cannot compare with NULL.** `order_status = 'cancelled'` is true or false.
`order_status != 'cancelled'` looks like it must be one or the other too. But if the
status is NULL, both are neither - the answer is "unknown", and `WHERE` keeps only
rows where the answer is *true*. So:

```sql
WHERE order_status != 'cancelled'   -- silently drops every NULL-status order
```

To ask the question you meant, say so explicitly:

```sql
WHERE order_status IS DISTINCT FROM 'cancelled'
```

`IS NULL` and `IS NOT NULL` are the only tests that work on a NULL. `= NULL` never
matches anything, not even another NULL.

**2. `IN` and `NOT IN` eat NULLs the same way.** `IN ('completed', 'shipped')` is
just a stack of `=` comparisons, so NULL-status rows fall out of it silently too.
This one catches people who already know about `!=`.

**3. Aggregates skip NULLs.** `COUNT(*)` counts rows. `COUNT(some_column)` counts
rows where that column is not NULL, so the two differ whenever the column has gaps.
`AVG` is the dangerous one: it divides by the number of non-NULL values, not by the
number of rows, so the denominator is not the one you assumed.

## What to do about it

You cannot avoid silent failures by being careful. You avoid them by checking. The
course gives you two tools for that, and both are yours to keep:

- [`../queries/audit-template.md`](../queries/audit-template.md) - seven questions to
  ask of any query, in the order worth asking them.
- [`../queries/anchors.md`](../queries/anchors.md) - a short list of numbers you have
  verified by hand, to measure every later answer against.

## The three questions to carry out of this course

Ask these of every query, including your own, including the ones an agent hands you:

1. **What does one row of this result represent?** If you cannot say it in a
   sentence, you do not know what the number means.
2. **What did this query throw away?** Every filter and every join drops rows. Which
   ones, and did you agree to that?
3. **How would I get this number a different way?** And if the second way agrees,
   does it agree because it is right, or because it repeats the same assumption?
