# Lesson 11: descriptions-and-tags

## Objectives
- Write a column description that records what a reader cannot infer, not what the column name already says.
- Use `tags`, `owner`, `domains`, and column-level `meta` for their actual jobs: selection, accountability, and machine-readable facts.
- Treat `bruin ai enhance` output as a draft that gets verified against the data, not published as written.

## Concepts to teach
Descriptions are usually taught as documentation hygiene, which is why they never get written -
hygiene is optional and everyone knows it. The better frame: a description is the input that decides
whether an agent's answer is right. An agent choosing between `net_price` and `unit_price` is reading
whatever text sits next to those two columns, not your mind. If nothing is there, it guesses, and the
guess looks exactly as confident as a correct answer.

A useless description restates the column name: `customer_id: the customer id`. A useful one records
what a reader cannot infer from the name or the type - the unit, the timezone, the population it
covers, a known caveat, or which of several similar columns is canonical. Five pairs from this
project's own data:

- `order_items.net_price` versus `unit_price` - a bad description calls both "the price". A good one
  says `net_price` is what revenue is computed from in this project, and `unit_price` is the
  pre-discount list price kept for reference only.
- `orders.order_total` - a bad description says "the order total". A good one says it is a
  header-level value from the source system that does not equal the sum of its lines: summed across
  deduped orders it is 604,065.00, against 851,617.69 in line revenue for the same period. Do not
  use it as a revenue source.
- `orders.order_status` - a bad description lists nothing. A good one says the finished state is
  written four ways (`Completed`, `COMPLETED`, `complete`, `completed`) and 24 rows are NULL, so
  any filter on this column needs `UPPER(TRIM(...))` and an explicit NULL decision.
- `customers.customer_id` - a bad description says "the customer id". A good one says it is not
  unique in this table: 10 ids appear twice, 510 rows for 500 customers, so a naive join fans out.
- `orders.currency_code` - a bad description says "the currency". A good one says it is a label
  only - amounts in this row are not converted to a common currency - and that `fx_rates` carries
  five pairs per date, so converting means joining on date and currency, not date alone.

Tags do two jobs in this project: selecting a slice of the pipeline to run (`bruin run --tag mart`),
and marking which layer an asset belongs to (`layer:staging`, `layer:core`, `layer:mart`).
`domains` is its own asset field, not a tag, and it groups an asset by the part of the business it
describes - `pipeline.yml` in this project already sets `domains: [commerce]` for every asset in its
`default:` block. `owner` names the person or team accountable for an asset, as an email or handle -
one line, not a paragraph. Column-level
`meta` holds small structured facts a description would bury in prose: a currency code, a rounding
rule, a source system name.

`bruin ai enhance` reads an asset and the underlying data and proposes descriptions and checks for
you - a legitimate starting point, not a finishing move. Published research on agent-context files
found that LLM-generated context can reduce task success while raising cost, compared with writing
nothing at all. The failure mode is specific: a plausible-sounding description that is wrong sends
an agent astray with more confidence than no description does. Generate with `bruin ai enhance`, then
verify every claim it makes with a query before you keep it.

## Quiz
1. Q: What makes `customer_id: the customer id` a useless description, precisely?
   A: It restates information the column name and type already give a reader; it adds nothing that could change how the column gets used.
2. Q: Why is `orders.order_total` a case where the description has to carry a warning rather than a definition?
   A: Because the header value does not reconcile with the line-level detail - 604,065.00 summed on deduped orders against 851,617.69 in line revenue - so a description that only says "the order total" invites someone to sum it as if it were revenue.
3. Q: Why verify `bruin ai enhance` output against the data instead of publishing it directly?
   A: Because generated descriptions are a draft that can be plausible and wrong, and published research found LLM-generated context can lower task success while raising cost - a wrong description is worse than no description because it is trusted.

## Task
Open `pipeline/assets/mart/weekly_category_revenue.sql`, the asset written in lesson 10. For every
column, run a query against the data to check what you are about to claim, then write a
`description` from what the query shows. Add a `tags` entry marking the asset's layer
(`layer:mart`). Run `bruin validate` when you are done.

## Rubric (for `review my work`)
- [ ] Every column in `pipeline/assets/mart/weekly_category_revenue.sql` has a non-empty `description`.
- [ ] No description restates the column name (for example, no `"the category"` on a `category` column).
- [ ] At least one description states a caveat backed by a query the student actually ran, not an assumption.
- [ ] The asset's `tags` include a `layer:mart` entry.
- [ ] `bruin validate` passes with no errors.

## Done signal
Confirm every column has a description, at least one carries a verified caveat rather than a
paraphrase, and `bruin validate` is clean. Carry forward: a description is a claim about the data,
and an unverified claim is a guess wearing a label.
