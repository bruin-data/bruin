# Reading drill

Three queries, getting longer. Read each one and write, beside every CTE and every join, what one
row represents at that point. That annotation is the whole technique: fan-out stops being something
you notice and becomes something you can see.

Target times, from the first line to the last annotation:

| File | Length | Target |
|---|---|---|
| `drill-1.sql` | about 20 lines, one join | 30 seconds |
| `drill-2.sql` | about 45 lines, three CTEs, one window function | 60 seconds |
| `drill-3.sql` | about 80 lines, five CTEs, four joins | 90 seconds |

Time yourself. Read for structure and grain first, filters and columns second.

Two of these three are correct. **One has a grain error: a join that changes what one row means,
followed by an aggregate written as though it had not.** Do not run them to find out which. Find it
by reading, then run it to confirm the size of the mistake.

`drill-2.sql` is a fair warning about what "correct" buys you. It returns the right numbers and it
is still hard to review, because its CTEs are called `t1`, `t2` and `final` and none of those names
say what a row is. Correct and reviewable are different properties. When you write your own, name
each step after what it produces.

The raw tables in this project have problems of their own - missing values, rows that arrived
twice, keys that point at nothing. Ignore all of that here. This drill is about grain only.
