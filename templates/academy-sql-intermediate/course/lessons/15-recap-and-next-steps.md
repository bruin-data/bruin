# Lesson 15: recap-and-next-steps

## Objectives
- Name the six habits this course built, and which one you will actually keep using.
- Say which number from the course changed how you think about trusting a query.
- Know what the advanced course adds and why none of it belongs here yet.

## Concepts to teach
No new material. Six habits carried the whole course, in the order you met them:

1. **Resolve ambiguity before writing SQL.** "Category performance" had four defensible readings
   before anyone wrote a WHERE clause. The fix is a question, not a query.
2. **Write a contract two analysts would implement identically.** Grain, keys, metric, filters, and
   what the model deliberately does not answer, written down before the SQL exists.
3. **Profile a table before you build on it.** Six questions, each with a query, asked of every
   table - row count and grain, key uniqueness, NULL rates, orphan keys, date coverage, and
   categorical consistency.
4. **Annotate grain to find fan-out mechanically.** Writing what one row represents beside every CTE
   and join turns a silent multiplication into something you can see on the page.
5. **Write descriptions for the failures you actually observed.** Not the column name restated - the
   caveat a query surfaced, like `order_total` not reconciling with line revenue.
6. **Measure whether your context helped, instead of assuming it did.** The same before-and-after
   evaluation that showed a specific line of `AGENTS.md` or the glossary either moved the score or
   did not.

None of these are tools. They survive a change of warehouse, a change of AI model, and a change of
job, because they are decisions about what to check and what to write down, not syntax. Keep the
one that matches how you actually work: if most of your mistakes come from vague requests, keep the
ask-back habit at the front; if most of them come from a query that ran clean and was still wrong,
keep profiling and grain annotation in front of every new table you touch.

The habit that is easiest to drop is the sixth, because it takes the most discipline to run twice.
Do not drop it. A description or a rule you never measured is a guess about what helped, and this
course exists because guesses about data are exactly what put you here.

Next: the advanced course covers pipelines, tests, incremental loads, and operating all of this with
an agent without letting it near production.

## Quiz
1. Q: Name the six habits, in the order the course taught them.
   A: Resolve ambiguity before writing SQL; write a contract two analysts would implement identically; profile a table before building on it; annotate grain to find fan-out mechanically; write descriptions for the failures you actually observed; measure whether your context helped instead of assuming it did.
2. Q: Why is habit six the one most likely to get skipped, and why does that matter?
   A: It requires running the same evaluation twice and comparing scores, which takes more discipline than writing a rule once; skipping it means every other habit's payoff is assumed rather than checked, which is the exact failure mode the course is about.
3. Q: What does the advanced course add that this one deliberately left out?
   A: Pipelines, tests, incremental loads, and operating all of it with an agent without letting it near production.

## Task
Say, in the conversation, which single habit from the six you will use on a specific piece of real
work in the next week - name the habit and the work, not the habit alone. Then say which number from
this course surprised you most, and why.

## Rubric (for `review my work`)
- [ ] One habit is named from the list of six, using language close enough to match its meaning.
- [ ] The habit is tied to a specific, named piece of real work, not a general intention.
- [ ] The surprising number is one of the actual numbers from this course (for example the 5x fan-out on `fx_rates`, the 169 completed orders in 2023 against 39 that a naive filter finds, or the 24 NULL `order_status` rows), not an invented or rounded figure.
- [ ] A reason is given for why that number was surprising, beyond restating the number.

## Done signal
Confirm the student names a real habit tied to real work and a real number tied to a real reason.
Carry forward: the course ends here, but the habits are only worth what you do with them next week -
point them at the advanced course and getbruin.com/learn.
