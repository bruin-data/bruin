# Lesson 01: the-question-is-the-hard-part

## Objectives
- Name the five decisions a data question leaves open.
- Say why an agent handed an ambiguous request produces a confident wrong answer.
- Rewrite a vague request as a three-line specification.

## Concepts to teach
No code in this lesson. Start with the request the whole course is built on: *"Give me a weekly view
of category performance I can take to the leadership meeting, and tell me which customers are at
risk of churning."* Ask the student what "category performance" means. It has at least four
defensible readings in this project - line revenue, order-header revenue, units sold, or margin -
and every one of them is a different chart. An agent asked this does not stop to choose. It picks,
silently, and the number it returns looks exactly as confident as a right one.

The five decisions hidden in almost every data question: **grain** (what one row of the answer
represents), **metric definition** (which column, with which exclusions), **time period and
boundary handling** (which dates, and whether the end is inclusive), **inclusion rules** (which
rows count at all - cancelled orders? test accounts?), and **comparison basis** (against what -
last week, last year, plan?). Walk through the spine question and name each one. This project makes
the metric decision especially sharp: `order_items` carries both `unit_price` and `net_price`, and
`orders` carries `order_total` on top of that, so "revenue" has three candidate columns before
anyone has written a WHERE clause.

Then the habit: **the right first response to a vague request is a question, not a query.** A
well-directed agent behaves the same way, and one that does not is a liability, because it will
guess faster than you can check. What good looks like is short: *"Weekly line revenue, one row per
category per ISO week, `quantity * net_price`, excluding cancelled orders, 2023 only, compared with
the prior week."* Three lines, no ambiguity, two analysts get the same numbers.

## Quiz
1. Q: Name the five decisions a data question hides.
   A: Grain, metric definition, time period and boundary handling, inclusion rules, and comparison basis.
2. Q: For the spine question, which decision is most consequential if you get it wrong, and why?
   A: The metric definition. This project has three candidate revenue columns - `net_price`, `unit_price` and `order_total` - and each gives a different total, so the whole chart moves. Grain matters too, but a grain error usually shows up as an implausible number, while the wrong revenue column looks perfectly reasonable.
3. Q: Your agent answers a vague request immediately, with a clean number and no questions. What has it just done?
   A: It has made every one of the five decisions on your behalf and not told you which way. The number is not wrong because the SQL is wrong; it is wrong because it answers a question nobody asked.

## Task
No SQL. Take the request *"Give me a weekly view of category performance I can take to the
leadership meeting"* and write out, in the conversation, the five decisions it leaves open. For each
one, state the options you can see in this project's schema and which you would choose. Then say
which single decision you would take back to the person who asked, and why that one.

## Rubric (for `review my work`)
- [ ] All five decisions named: grain, metric definition, time period and boundary, inclusion rules, comparison basis.
- [ ] The metric decision names at least two of the three candidate columns: `net_price`, `unit_price`, `order_total`.
- [ ] A grain is stated as a sentence about one row, for example "one row per category per week".
- [ ] One decision is picked to escalate, with a reason that is about consequence rather than difficulty.

## Done signal
Confirm the student can name all five decisions without prompting and has picked a metric column
rather than saying "revenue". Carry forward: a question you have not specified is a question your
agent will specify for you.
