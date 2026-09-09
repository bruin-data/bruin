# Lesson 02: write-the-model-contract

## Objectives
- Fill in a model contract for a real request, with no placeholder text left.
- Name the exact revenue column a metric depends on, and say what it excludes.
- Explain why pasting a contract into an agent's context removes ambiguity from every later request.

## Concepts to teach
A **model contract** is a short written file that fixes the answer before the SQL exists. It has
six fields: Question, Grain, Keys, Metric, Filters, and Not included. `docs/contracts/TEMPLATE.md`
ships in this project with those six fields blank and a note on how to use it. The student's job in
this lesson is to copy that file to `docs/contracts/weekly_category.md` and fill every field for
real, against the spine request: *"Give me a weekly view of category performance I can take to the
leadership meeting."*

Each field earns its place. Question is the stakeholder's request, verbatim, so nobody paraphrases
it into something else. Grain is a sentence about what one output row represents - "one row per
category per week," not "weekly data." Keys are the columns that make a row unique, which for this
model are the category and the week start date together. Metric names the exact column and its
exclusions: this project has three candidate revenue columns across `order_items` and `orders` -
`net_price`, `unit_price`, and `order_total` - and the contract has to name one of them, not the
word "revenue." Filters states what happens to cancelled orders and to orders with a missing
`order_status`, because both are live possibilities in this data and a contract that is silent on
them is not a contract. Not included says what this model deliberately will not answer, so nobody
extends it by assumption later.

The sentence that makes this lesson matter: once `docs/contracts/weekly_category.md` exists, paste
it into the agent's context and every later request about this model - "add a filter," "extend it
to 2024," "explain the drop in week 47" - inherits the same grain, the same metric, and the same
exclusions, without you restating them. The contract is not paperwork. It is the thing that lets two
different people, or two different agent sessions, produce the same numbers from the same request.

## Quiz
1. Q: Name the six fields of a model contract, in order.
   A: Question, Grain, Keys, Metric, Filters, Not included.
2. Q: Why does "Metric: revenue" fail as a contract field in this project?
   A: Because `order_items` carries both `net_price` and `unit_price`, and `orders` carries `order_total` on top of that, so "revenue" has at least three numeric answers. The field has to name one column and state its exclusions.
3. Q: You paste `docs/contracts/weekly_category.md` into an agent's context, then ask it to "add a filter for the top five countries." Why does the contract make that request safer?
   A: Because the agent already knows the grain, the metric column, and which orders are excluded, so it extends the existing query instead of silently re-deciding those three things while adding the filter.

## Task
Copy `docs/contracts/TEMPLATE.md` to `docs/contracts/weekly_category.md` and fill in all six fields
for the spine request: *"Give me a weekly view of category performance I can take to the leadership
meeting."* Write real sentences, not brackets. Pick one revenue column and say why. State plainly
what happens to cancelled orders and to orders with a NULL `order_status`.

## Rubric (for `review my work`)
- [ ] The file exists at `docs/contracts/weekly_category.md`.
- [ ] All six fields - Question, Grain, Keys, Metric, Filters, Not included - are filled, with no `<...>` placeholder text remaining.
- [ ] The Metric field names one of `net_price`, `unit_price`, or `order_total` explicitly.
- [ ] The Grain field is a sentence describing what one output row represents.
- [ ] The Filters field states, separately, what happens to cancelled orders and to orders with a missing `order_status`.

## Done signal
Confirm the file exists at the exact path, every field is filled with real text, and the Metric
field names a specific column rather than the word "revenue." Carry forward: every CTE and query
built in the rest of this course for this model should agree with what this contract says.
