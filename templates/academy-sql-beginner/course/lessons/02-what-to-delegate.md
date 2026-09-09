# Lesson 02: what-to-delegate

## Objectives
- Tell a loud failure from a silent one.
- Decide which kind of failure needs a human to catch it.

## Concepts to teach
Read `docs/failure-modes.md` first - it is two minutes and it is the idea the course
is built on. A **loud failure** is a query that does not run: a typo, a missing column,
the database refuses and tells you why. Annoying, harmless - you cannot act on an
answer you never got. A **silent failure** is a query that runs perfectly, returns a
tidy number, and the number is wrong. Nothing is red. You put it in a report.

The machine already catches loud failures for you; that is the part safe to delegate.
Silent failures are the part that needs a human, because the only way to catch one is
to ask what the query means and check it. A syntax error costs a minute. A silent
failure can cost a quarter, because decisions get made on it before anyone notices.

Every trap in this course is a silent failure. That is why you are learning to read and
check queries rather than to write them faster.

## Quiz
1. Q: `SELECT order_totl FROM orders` returns an error. Loud or silent, and who catches it?
   A: Loud - the database catches it and refuses to run. No human judgement needed.
2. Q: A query sums `order_total` after joining orders to their lines and reports 1.4 million. It runs fine. Loud or silent?
   A: Silent - it runs and returns a number, but the join counts each order total once per line, so the number is wrong. Only a human asking "what does one row mean here?" catches it.

## Task
Here are three outcomes. Label each loud or silent, and for the silent one say why only
a person could catch it:
1. The database says `column "revenu" does not exist`.
2. A revenue total comes back about 2.4 times too big but runs cleanly.
3. A filter of `!= 'cancelled'` quietly returns fewer orders than expected.

## Rubric (for `review my work`)
- [ ] Item 1 labelled loud; items 2 and 3 labelled silent.
- [ ] The student labels a wrong-but-runs case (item 2 or 3) as silent and says it needs a human because nothing flags it.

## Done signal
Confirm the student can spot that "it ran and gave a number" is not evidence the number is right. Carry forward: trust comes from checking, not from the query running.
