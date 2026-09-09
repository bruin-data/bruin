# Lesson 03: profile-before-you-model

## Objectives
- Run six profiling questions against any table before building on it.
- Report exact counts for duplicates, orphan keys and value variants in this project.
- Say what each finding would do to a revenue number.

## Concepts to teach
Profiling is a routine, not an instinct. Six questions, each with a query, asked of every table
before you trust it: **how many rows, and what does one row represent** - **is the claimed key
actually unique** (`GROUP BY key HAVING COUNT(*) > 1`) - **which columns have missing values, and at
what rate** - **are there orphan foreign keys** (an anti-join) - **what is the date range, and are
there gaps** - **are the categorical values consistent** (`GROUP BY` a text column and read the
list). `queries/profiling/` ships one runnable file per question, each written against one table
with a note saying to change the table name and run it again. Those six files are the real takeaway
of this lesson; the findings are second.

The findings in this project are not decoration. `docs/schema.md` says `orders` holds 1,212 rows,
and the phrase it uses is "as delivered by the source system" rather than "one row per order". Ask
the student to notice the difference before they run anything. Two of the tables carry duplicate
rows and the fix is different for each, which is lesson 6. Two of the joins have orphan keys on the
fact side, which is why an `INNER JOIN` here quietly deletes revenue. One text column is written
five different ways, which means a `GROUP BY` on it ranks the wrong value first.

Third: every finding has to be quantified in money or rows, or it is not a finding. "There are
duplicates in `customers`" is a note. "Ten `customer_id` values appear twice, and because they sit
in the frequent-buyer pool, joining to them inflates the top of any customer ranking" is a finding.
Do not fix anything in this lesson. Profile, quantify, write it down. Cleaning is lesson 8, and
doing it now means cleaning things you have not yet decided are wrong.

## Quiz
1. Q: You run `SELECT COUNT(*), COUNT(DISTINCT order_id) FROM orders` and the two numbers differ. What have you not yet learned?
   A: Whether the duplicate rows are identical or differ in their payload. `SELECT DISTINCT *` answers it: if the row count drops to the distinct-key count they are exact copies, and if it does not, the source disagrees with itself about those records and choosing one is a decision.
2. Q: Why does an anti-join belong in a profiling routine when the foreign key has no constraint on it?
   A: Because with no constraint nothing stops an orphan being written, and an `INNER JOIN` to the dimension will drop those fact rows silently - no error, just a smaller number.
3. Q: `GROUP BY country ORDER BY COUNT(*) DESC` returns France at the top. Why might that be the wrong answer?
   A: Because one country's rows are split across several spellings, so its true total is divided between rows of the result. Folding case and whitespace changes the ranking.

## Task
Work through all six files in `queries/profiling/`, adapting each to the tables it does not already
cover, and report these six numbers in the conversation:

1. Rows in `orders`, and distinct `order_id`.
2. How many `customer_id` values appear more than once in `customers`.
3. How many rows in `order_items` have a missing `unit_cost`.
4. How many rows of `order_items` have a `product_id` with no row in `products`.
5. How many calendar days between 2023-01-01 and 2025-12-31 have no order at all.
6. How many distinct values `customers.country` holds, and how many of them are the same country.

For each one, say in a sentence what it would do to a revenue number.

## Rubric (for `review my work`)
- [ ] `orders` has 1,212 rows and 1,200 distinct `order_id`.
- [ ] 10 `customer_id` values appear more than once (510 rows for 500 customers).
- [ ] 57 rows of `order_items` have a missing `unit_cost`.
- [ ] 15 rows of `order_items` point at a `product_id` with no row in `products`.
- [ ] 160 of the 1,096 calendar days have no order.
- [ ] `customers.country` holds 16 distinct values, 5 of which are the same country written differently.
- [ ] At least three findings are quantified as an effect on a number, not just reported.

## Done signal
Confirm all six numbers match exactly and that nothing has been fixed yet. Carry forward: profile
first, decide what a defensible number is second, clean third - in that order, every time.
