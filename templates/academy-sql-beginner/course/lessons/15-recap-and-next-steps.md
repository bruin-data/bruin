# Lesson 15: recap-and-next-steps

## Objectives
- Consolidate the course into the habits worth keeping.
- Know where to go next.

## Concepts to teach
The course was never about writing SQL fast. It was about not being fooled by a number
that runs. Three habits carry the whole thing, and they are yours to keep:

- **Read before you run.** An agent's query looks authoritative and can still fan out,
  drop NULLs, or sum the wrong column. Read it and say what one row means first.
- **Get the number a second way.** A headline you can only reach one way is a headline
  you have not checked. A genuine second method comes from another grain or table.
- **Fix the context, not just the answer.** When you catch a mistake, persist the
  correction in `AGENTS.md` or a column `description` so it does not come back.

Where to next: the intermediate course adds window functions, data modelling, and
multi-currency revenue with a real `fx_rates` table; the advanced course covers
incremental strategies, quality checks, and pipelines. For scale, `docs/` points at
TPC-H via DuckDB and BigQuery public datasets. See getbruin.com/learn.

## Quiz
1. Q: Give the three habits this course leaves you with.
   A: Read before you run; get the number a second (independent) way; fix the context in a file, not just the chat.
2. Q: What does "read before you run" protect you from specifically?
   A: The silent failures - fan-out, NULL-dropping filters, the wrong column - that a query commits without any error.

## Task
Write down the three habits you intend to keep from this course, in your own words, and
one sentence on when each one would have saved you during the lessons.

## Rubric (for `review my work`)
- [ ] The student names read-before-run, the second-method check, and fix-the-context.
- [ ] Each is tied to a concrete moment (a fan-out, a NULL filter, a wrong column) where it applies.

## Done signal
Confirm all three habits are named and grounded in something they actually hit. That completes the course - point them at the intermediate course and getbruin.com/learn.
