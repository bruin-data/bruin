# Lesson 10: metric-in-the-asset

## Objectives
- Build the full asset header on a mart asset: description, tags, and a `meta` block.
- State why `meta.metric_definition` belongs in the file rather than in a chat thread.
- Write `pipeline/assets/mart/weekly_category_revenue.sql` implementing the lesson 2 contract.

## Concepts to teach
The asset header is not decoration around the query - for a mart asset, it is the place the metric
definition actually lives. `description` takes the folded form, `>-`, so a two-sentence explanation
reads as prose instead of a wrapped line:

```yaml
description: >-
  One row per product category per ISO week in 2023, with revenue as quantity * net_price.
  Excludes cancelled orders.
```

`pipeline/pipeline.yml` already sets `owner` and `domains` inside a `default:` block that applies
to every asset in this project, so a mart asset names its own `owner` only when it genuinely
differs from that default - repeating it for every asset would duplicate a value that was already
set once. `tags` is a separate mechanism, used for selection and for marking which layer an
asset belongs to: `layer:mart`, `domain:commerce`.

The `meta` block is where this lesson's argument lands. It is free-form key-value pairs, and for a
mart asset that carries a metric, five of them earn their place: `business_owner` (who to ask about
this number), `metric_definition` (the exact expression, not the word "revenue"),
`currency` (the basis the number is stated in), `expected_update` (how often this should refresh),
and `known_limitation` (what this number does not account for, stated plainly rather than omitted).
This course simplifies revenue recognition throughout - it never models refunds, taxes, or
returns-after-payment - and that is exactly the kind of thing `known_limitation` exists to say out
loud.

Here is the argument itself: a metric definition that lives in a chat thread is a metric definition
nobody can find later, including an agent asked to change the query - it was never in its context,
and it never will be unless someone pastes it in again. A metric definition written into
`meta.metric_definition` travels with the code, shows up in every code review, and is read by any
agent that opens the file, without anyone having to remember it exists.

## Quiz
1. Q: Where does this course put a metric's exact definition, and why not in a chat thread?
   A: In the mart asset's `meta.metric_definition` field. A chat thread is invisible to anyone who was not in it, including an agent asked to modify the query later, while a definition in the asset header is read every time the file is opened.
2. Q: `pipeline/pipeline.yml` sets `owner` and `domains` under `default:` for every asset. When should `weekly_category_revenue.sql` set its own `owner`?
   A: Only if its real owner differs from the pipeline default - otherwise the asset would repeat a value the pipeline already states once for everything.
3. Q: Name the five `meta` fields this lesson teaches for a mart asset carrying a metric.
   A: `business_owner`, `metric_definition`, `currency`, `expected_update`, `known_limitation`.

## Task
Write `pipeline/assets/mart/weekly_category_revenue.sql`, implementing the contract from
`docs/contracts/weekly_category.md`. Give it the full header: a folded `description: >-`, `tags`
including `layer:mart` and `domain:commerce`, and a `meta` block with `business_owner`,
`metric_definition`, `currency`, `expected_update`, and `known_limitation` all filled in with real
values, not placeholders. Its `depends` should name a staging or core asset, never a table from
`pipeline/assets/generate/`.

## Rubric (for `review my work`)
- [ ] The file exists at `pipeline/assets/mart/weekly_category_revenue.sql`.
- [ ] `bruin validate` passes.
- [ ] `meta.metric_definition` names the exact expression the lesson 2 contract chose, for example `quantity * net_price`.
- [ ] `meta.currency` states a currency basis.
- [ ] `meta.known_limitation` is filled in, not empty.
- [ ] `depends` names a staging or core asset (for example `stg_order_items`), not a table from `pipeline/assets/generate/`.

## Done signal
Confirm the file exists at the exact path, `bruin validate` passes, and every `meta` field holds a
real value rather than a placeholder. Carry forward: the next time this metric's definition is in
question, the answer is in this file, not in whoever remembers the conversation where it was
decided.
