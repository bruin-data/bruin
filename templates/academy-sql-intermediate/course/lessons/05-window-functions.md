# Lesson 05: window-functions

## Objectives
- Pick the right window function for a stated question rather than recalling syntax.
- Write a window with an explicit `PARTITION BY`, `ORDER BY`, and frame where one is needed.
- Explain what a missing `PARTITION BY` and a tie inside `ROW_NUMBER` each silently break.

## Concepts to teach
Learn window functions by the question they answer, not by memorising a list. "What was each
category's revenue last week compared with the week before?" is `LAG`. "What is the four-week
moving average?" is an aggregate window with a frame - `AVG(...) OVER (PARTITION BY ... ORDER BY ...
ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)`. "Which rows are the newest per key?" or "top three
customers per country?" is `ROW_NUMBER` with `PARTITION BY`. "Where does each value sit in the
distribution?" is `NTILE` or `PERCENT_RANK`. Every one of these has the same three parts: `OVER`
opens the window, `PARTITION BY` says which rows are grouped together for the calculation, and
`ORDER BY` inside the window says what order the calculation runs in. A frame - `ROWS BETWEEN ...`
- narrows that further to a sliding range of rows around the current one, which is what turns a
running total into a moving average.

Two traps follow directly from those three parts, and both produce a query that runs without error
and returns the wrong answer. The first is a missing `PARTITION BY`. Leave it off a `LAG` and the
window looks backward across the entire result set instead of within each category, so week one of
Apparel silently compares against the last row of Toys & Games. Nothing errors; the number is wrong
and looks exactly as plausible as a right one. The second trap is ties. `ROW_NUMBER` assigns 1, 2,
3, ... and breaks a tie between equal values arbitrarily, so a re-run can pick a different row for
the top spot. `RANK` and `DENSE_RANK` give tied rows the same rank instead. Using `ROW_NUMBER` to
pick "the top customer" when two customers are exactly tied means the answer can change between
runs with no data change at all.

The file for this lesson's task is `queries/weekly-category.sql`, which does not exist yet - the
student writes it. The question is: for each product category and each week of 2023, what is line
revenue, the prior week's revenue, the week-over-week change, and a four-week moving average. The
week grain in this project is always `CAST(date_trunc('week', ordered_at) AS DATE)`, and the 2023
window is always `ordered_at >= TIMESTAMP '2023-01-01 00:00:00' AND ordered_at < TIMESTAMP
'2024-01-01 00:00:00'`. Every number this lesson's rubric checks depends on using exactly those two
expressions.

## Quiz
1. Q: Which window function answers "what was last week's revenue for this category," and what clause makes it look inside the category rather than across the whole table?
   A: `LAG`, and it needs `PARTITION BY category_name` in its `OVER` clause - without it, `LAG` looks at the previous row in the whole result set, not the previous week of the same category.
2. Q: Why is a four-week moving average an aggregate function, not a ranking function?
   A: Because it needs `AVG` (or `SUM`) computed over a frame of the current and preceding rows, not a rank or an offset - the frame clause (`ROWS BETWEEN 3 PRECEDING AND CURRENT ROW`) is what makes it a moving window rather than a single lookback.
3. Q: You use `ROW_NUMBER` to find the single highest-revenue category each week, and two categories tie for first. What happens, and would `RANK` behave differently?
   A: `ROW_NUMBER` picks one of the tied categories arbitrarily as rank 1, so the result can differ between runs with no data change. `RANK` would give both categories rank 1, which is deterministic and shows the tie instead of hiding it.

## Task
Write a query in `queries/weekly-category.sql` that returns, for each product category and each
week of 2023: line revenue, the prior week's revenue, the week-over-week change, and a four-week
moving average. Use `CAST(date_trunc('week', ordered_at) AS DATE)` for the week grain, and filter to
`ordered_at >= TIMESTAMP '2023-01-01 00:00:00' AND ordered_at < TIMESTAMP '2024-01-01 00:00:00'`.
You have not been taught deduplication yet, so deduplicate the two sources this way for now: read
order lines with `SELECT DISTINCT * FROM order_items`, and read orders with `QUALIFY ROW_NUMBER()
OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1`. Lesson 6 explains why each is the right
tool for its table. State, in the conversation, the `PARTITION BY` clause each window in your query
uses and why.

## Rubric (for `review my work`)
- [ ] The query lives in `queries/weekly-category.sql` and uses the week grain `CAST(date_trunc('week', ordered_at) AS DATE)`.
- [ ] The 2023 window is exactly `ordered_at >= TIMESTAMP '2023-01-01 00:00:00' AND ordered_at < TIMESTAMP '2024-01-01 00:00:00'`.
- [ ] The largest week-over-week drop found is Electronics in the week beginning 2023-11-20: 6,784.54 down to 591.30, a fall of 6,193.24.
- [ ] The first week of the series returns NULL for the prior-week and week-over-week columns, and the student can say this is correct because no prior week exists, not a bug.
- [ ] Every window function in the query states its `PARTITION BY category_name` explicitly.

## Done signal
Confirm the query returns the stated largest drop exactly, the student can explain the first row's
NULLs, and every window is partitioned by category. Carry forward: this query used `SELECT
DISTINCT` and a `QUALIFY` filter to get clean input without explanation - lesson 6 is where you
learn why those are the correct tools and not a shortcut.
