# Lesson 12: glossary-and-readme

## Objectives
- Assign the right content to `AGENTS.md`, the glossary, and `README.md` instead of mixing them.
- Write a `Revenue` entry that resolves the ambiguity this project has carried since lesson 1.
- Know the maintenance rule: these files decay, and a stale one is worse than a short one.

## Concepts to teach
Three files, three jobs, and most projects blur them into one long document nobody reads.
`AGENTS.md` holds rules and behaviour: how to access data, what is read-only, what needs approval,
how to handle ambiguity, which tables are canonical. It should read like instructions to a new hire
on their first day - short, imperative, specific. This project's own `AGENTS.md` is the example: it
tells the agent to ask before writing SQL for an ambiguous request, and to use
`bruin query --connection duckdb-default --description "..."` rather than describing data access in
the abstract.

The glossary holds domain terms and metric definitions: every acronym, every metric with its exact
computation, every term that means something specific in this project rather than in general. A term
belongs here, not in `AGENTS.md`, if the question it answers is "what does this word mean" rather
than "what should the agent do". `README.md` holds orientation: what this project is, where things
live, how to run it - the page a person reads once, on day one, and rarely again.

`docs/glossary.md` in this project ships as a stub: three terms defined, `Revenue` left as a TODO,
and a line listing six more terms nobody has written yet. Finishing it is this lesson's task, and
the `Revenue` entry is the one that matters most, because it is the same ambiguity lesson 1 opened
the course with. A finished entry does not say "revenue is money from sales" - it names one exact
expression and the column it comes from: `quantity * net_price` on `order_items`, not `order_total`
and not `unit_price`. Anyone who later needs a different revenue definition for a different question
should be adding a new, separately named term, not editing this one out from under the assets that
depend on it.

The maintenance rule is the part that gets skipped: these files decay. An `AGENTS.md` line stops
being true the day the schema changes under it, and a glossary term stops being true the day someone
redefines the metric without updating the entry. Update these files when the agent gets something
wrong because the file told it something false, and delete lines that are no longer true rather than
letting them accumulate. A stale `AGENTS.md` is worse than a short one, because a short file is
honest about what it does not cover and a stale one actively misleads.

## Quiz
1. Q: A new rule says the agent must never run `DELETE` against generated data without asking first. Which file does that belong in, and why not the glossary?
   A: `AGENTS.md`. It is a behaviour rule, not a definition of a term - the glossary answers "what does this word mean," not "what is the agent allowed to do."
2. Q: Why does the `Revenue` glossary entry need to name a column instead of describing a concept?
   A: Because this project has three candidate revenue columns - `net_price`, `unit_price`, `order_total` - and a prose definition without a named column and expression leaves the same ambiguity lesson 1 opened with unresolved.
3. Q: Why is a stale `AGENTS.md` worse than a short one?
   A: A short file is honest about its limits and an agent falls back to asking; a stale file states something false with the same confidence as something true, so the agent acts on it without knowing it is wrong.

## Task
Rewrite `docs/glossary.md`. Replace the `Revenue` TODO with a real entry naming the exact expression
and column. Add entries for every term listed in the stub's TODO line: AOV, active customer, churn,
category, net vs gross, and fiscal period. State the grain of `orders` and of `order_items` somewhere
in the file. For at least one entry, cite the asset file where that metric is actually computed.

## Rubric (for `review my work`)
- [ ] `docs/glossary.md` has a `Revenue` entry naming one exact expression (`quantity * net_price`) and the column it runs on (`order_items.net_price`).
- [ ] Entries exist for all six stub-TODO terms: AOV, active customer, churn, category, net vs gross, fiscal period.
- [ ] A grain statement appears for `orders` (one row per order) and for `order_items` (one row per line on an order).
- [ ] At least one entry cites the specific asset file where that metric is defined.
- [ ] The file no longer contains the word `TODO`.

## Done signal
Confirm the `Revenue` entry names a column and an expression, all six TODO terms have entries, both
grains are stated, and at least one citation points at a real file. Carry forward: a glossary that
names a column is a decision; a glossary that names a concept is the same ambiguity, filed away.
