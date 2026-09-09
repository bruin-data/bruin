# Lesson 11: interrogate-the-logic

## Objectives
- Keep a short list of anchors: numbers you have verified.
- Interrogate an answer with four moves: why-this-not-that, what-if, show-me-a-second-way, and what-would-break-this.
- Reconcile a surprising answer against an anchor, or explain the gap.

## Concepts to teach
`queries/anchors.md` holds numbers you have checked and trust. Auditing is mostly this:
build a few numbers you are sure of, then measure everything else against them. Three
are filled in already - 1,200 orders, 2,880 lines, 2.4 lines per order - and they are
exact, because the data never drifts. When a later answer surprises you, an anchor tells
you whether the query is wrong or the surprise is real.

Interrogating an answer is four moves, and it helps to name them:
- **why-this-not-that** - why this column, table, or filter and not the obvious alternative (why `unit_price` and not `net_price`)?
- **what-if** - what changes if you relax an assumption (what if the NULL-status orders are included)?
- **show-me-a-second-way** - compute the headline a genuinely different way and see if it agrees.
- **what-would-break-this** - what input would make this answer wrong, and is that input present in the data?

This is also where you push back on the agent. If it reports a number that does not
reconcile with an anchor, do not accept "it ran". Either the query is wrong, or the gap
has an honest explanation - like `orders.order_total` summing to 604,065.00 while line
revenue sums to 851,617.69, two different measures that never agree and are not a bug.
Reconciling means either the numbers line up, or you can say exactly why they do not.

## Quiz
1. Q: What is an anchor, and what is it for?
   A: A number you verified by hand and trust; you measure surprising later answers against it to decide whether the query is wrong or the surprise is real.
2. Q: `SUM(order_total)` is 604,065.00 but line revenue is 851,617.69. Bug?
   A: No - they are two different measures. `order_total` is supplied by the source, not derived from the lines. The gap is expected; you just have to name it.

## Task
Add at least one new anchor to `queries/anchors.md` (for example, orders in 2024, or
distinct customers), with the query that produced it. Then take a number from an earlier
lesson or the agent and reconcile it against an anchor: state whether it agrees, and if
not, why.

## Rubric (for `review my work`)
- [ ] At least one new, correctly-computed anchor is added to `anchors.md`, with its query.
- [ ] The student reconciles a chosen answer to an anchor - it agrees, or the gap is explained (e.g. header total vs line revenue), rather than left unexplained.

## Done signal
Confirm the anchor is right and the reconciliation holds. Carry forward: a number you cannot tie to something you already trust is a number you have not checked.
