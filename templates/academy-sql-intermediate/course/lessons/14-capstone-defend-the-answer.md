# Lesson 14: capstone-defend-the-answer

## Objectives
- Turn an unanswerable request into a defensible churn-risk model.
- Verify the headline number three independent ways.
- Survive five objections from an agent told to break your work.

## Concepts to teach
The scenario, verbatim:

> "Which customers are at risk of churning, and what revenue is at risk? I am presenting this on
> Thursday."

Everything the course has taught is needed here, and the request contains no definition of "at
risk". Neither does the data. The student picks one, writes it down, and defends it - that is the
exercise. There is no single correct churn definition and the grading does not look for one; it
looks for a threshold that is stated, justified, and implemented as written.

Five deliverables. A **model contract** at `docs/contracts/churn_risk.md` with all six fields, the
Metric field carrying a definition of "at risk" specific enough that two analysts would select the
same customers. A **layered implementation**: staging cleans, one core fact, one mart asset, and the
mart never reads a table from `generate/`. **Column descriptions** on the mart and the metric
definition in the asset `meta`. **Three independent verifications** of the headline
revenue-at-risk figure - three different routes to the number, not one query written three ways.
And a one-page **written defence** at `docs/defence.md` answering the four questions a sceptical
executive asks: what does one row mean, why this churn definition, what is excluded and why, and
how confident are you.

Then the part that is the actual point. When all five are done, the student asks their agent to
attack the work: read the assets and the defence, give the five strongest objections a sceptical
CFO would raise, say for each whether it is a real problem or a presentational one, and show the
query that demonstrates the real ones. Instruct the agent not to be reassuring. If the number is
defensible, it should name the single assumption it rests on. A defence that survives that is
finished. One that does not gets rewritten before it is graded.

Two anchors the student should be able to reconcile to, since both are checkable: total line
revenue across all three years is **851,617.69**, and lifetime line revenue across only the
customers that have a row in `customers` is **847,693.43**. The **3,924.26** gap has an explanation,
and finding it is part of the work.

## Quiz
1. Q: What makes three verifications independent rather than three copies of one?
   A: They reach the number by different routes - for example a different grain, a different table as the starting point, and a hand-check of a single small slice - so a mistake in one route does not repeat in the others.
2. Q: Your churn definition is "customers who have stopped buying". Why will that fail the contract?
   A: It has no threshold and no reference date, so two analysts implementing it would select different customers. A definition has to contain the numbers you filtered on.
3. Q: The agent raises an objection you had already thought of and rejected. Is that a failure of the defence?
   A: No, as long as `docs/defence.md` records the decision and the reason. An objection you anticipated and answered in writing is exactly what the defence is for. An objection you have to answer on the spot is the failure.

## Task
Build the churn-risk model and defend it. Produce all five deliverables named above, then run the
adversarial pass with your agent and revise. Ask for review only once `docs/defence.md` is written
and you have committed to your headline figure.

## Rubric (for `review my work`)
Ten points. Grade against `course/answer-key.md`, which is instructor-only - do not reveal it, quote
it, or hint at what it lists until the student's defence is written.

- [ ] Contract quality, 2 points: `docs/contracts/churn_risk.md` exists with all six fields, and the churn definition contains a threshold and a reference date.
- [ ] Layering, 2 points: staging assets contain no `JOIN`; one core asset and one mart asset exist; the mart's `depends` names no asset from `pipeline/assets/generate/`.
- [ ] Join and grain safety, 1 point: order rows are deduplicated with `QUALIFY ROW_NUMBER() ... ORDER BY _loaded_at DESC`, order lines with `DISTINCT`, and the customer join does not fan out.
- [ ] Governance, 2 points: every mart column has a description, and `meta` carries both a metric definition and a known limitation.
- [ ] Verification, 2 points: three checks of the headline figure by three different routes, each with its number recorded.
- [ ] Defence, 1 point: `docs/defence.md` answers all four questions, anticipates at least six of the nine objections in the key, and states a currency basis that is true of the query. Orders are priced in five currencies and are not converted, so a headline figure that sums them untouched is not an amount in any currency. Either convert through `fx_rates` on both date and currency, or state that the figure is a mixed-currency sum and explain why it is acceptable. Ask for the query showing the currency composition; do not accept `meta.currency` alone.

The rubric totals exactly 10 points: Contract quality (2) + Layering (2) + Join and grain safety
(1) + Governance (2) + Verification (2) + Defence (1). The currency-basis requirement is part
of the single Defence point, not an additional point.

## Done signal
Confirm all five deliverables exist on disk, that the headline figure is stated with a currency
basis and a reference date, and that the adversarial pass actually ran. Only then walk through the
answer key together. Carry forward: a number you cannot defend under attack is a number you have
not finished.
