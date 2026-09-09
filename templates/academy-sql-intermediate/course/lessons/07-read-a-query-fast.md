# Lesson 07: read-a-query-fast

## Objectives
- Annotate a query's grain step by step, so fan-out becomes visible instead of intuited.
- Read an 80-line query in 90 seconds and say where the grain changes.
- Locate the grain error in `drill-3.sql` and quantify it.

## Concepts to teach
This is a drill, not a concept lesson. The technique is **grain annotation**: read a query top to
bottom and write, beside each CTE and each join, what one row represents at that point. That is
all. Fan-out stops being something you have to notice and becomes something you can see, because
the moment a line says "one row per order" and the next says "one row per order line" while the
aggregate below still sums an order-level column, the error is on the page.

Two supporting habits. **Two passes**: the first for structure and grain only, ignoring filters and
column lists; the second for filters, boundaries and which columns are actually selected. And
**read it aloud** - not silently. Reading aloud forces you through the join conditions you would
otherwise skim, and skimmed join conditions are where fan-out lives.

`queries/reading-drill/` holds three queries getting longer, with target times of 30, 60 and 90
seconds. Two of the three are correct on the grain axis. One is not. Do not run them to find out
which - find it by reading, then run it to measure the damage. `drill-2.sql` is worth a comment of
its own: it is correct and it is still hard to review, because its steps are named `t1`, `t2` and
`final`. Correct and reviewable are different properties.

## Quiz
1. Q: What do you write beside a CTE when you annotate grain?
   A: A sentence saying what one row of that CTE represents, for example "one row per order" or "one row per order line per currency pair". Nothing else.
2. Q: A query joins `orders` to `order_items` and then sums `order_total`. Reading only the annotations, how do you know it is wrong?
   A: The annotation above the join says one row per order and the annotation below says one row per order line. `order_total` belongs to the grain above the join, so summing it below counts it once per line.
3. Q: Why does the first pass ignore filters?
   A: Because a filter cannot make a number too big, and grain can. Structure and grain decide whether the query is answering the right shape of question; filters decide which rows, and that is a smaller and later problem.

## Task
Annotate all three files in `queries/reading-drill/`, writing your annotations as comments in a copy
of each file saved as `queries/reading-drill/drill-N-annotated.sql`. Time yourself on each and
report the three times. Then name which of the three has the grain error, which step introduces it,
and run that query twice - once as written and once with only that one step corrected - to report
both totals.

## Rubric (for `review my work`)
- [ ] Three annotated files exist, and every CTE and every join in each carries a grain sentence.
- [ ] The student names `drill-3.sql` as the query with the grain error, and `lines_with_customer` as the step that introduces it.
- [ ] They explain it correctly: `customers` holds 510 rows for 500 customers, so joining to it repeats the lines of the 10 duplicated ids.
- [ ] Both totals are reported: 373,614.70 as written, 348,521.23 with only that join deduplicated, a difference of 25,093.47.
- [ ] The three times are reported. The 90-second target on `drill-3.sql` is a goal, not a pass condition - do not fail the lesson on it.

## Done signal
Confirm the annotations exist on the page rather than in the student's head, and that the two
`drill-3.sql` totals match. Carry forward: annotate the grain before you read anything else, and
the fan-out finds you.
