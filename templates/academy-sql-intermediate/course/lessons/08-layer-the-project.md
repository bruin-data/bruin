# Lesson 08: layer-the-project

## Objectives
- Build the staging layer: one asset per source table under `pipeline/assets/staging/`.
- Choose `QUALIFY` or `DISTINCT` correctly for each table's duplicate rows, and say why.
- Name the rule that keeps a mart asset from ever reading a raw table directly.

## Concepts to teach
Three layers, each with one job. **Staging** is one asset per source table, named
`stg_<table>.sql`, and it cleans and nothing else: rename, cast, trim, standardise casing,
deduplicate. No joins, no business logic, no metric decisions - a staging asset stays boring on
purpose, so anyone reading it can trust every row without checking the source. **Core** is the
layer where the business's entities and facts live: customers, orders, order lines, one asset per
concept, one grain per asset, and that grain stated in the asset's `description`. Joins live here,
because a fact needs its dimensions to mean anything. **Mart** is the shaped answer: one asset per
question or dashboard, built from core, not from source. Other tools call this bronze, silver, gold,
or staging, intermediate, marts - the names differ and the discipline does not.

`depends` is what turns three folders into a lineage graph rather than three piles of files. Every
asset names the assets it needs by asset name, and Bruin builds them in that order. The rule that
makes the graph mean something: **a mart asset never reads a raw table directly.** If
`weekly_category_revenue` names `order_items` in its `depends`, something has skipped a layer, and
the cleaning that layer was supposed to do - the casing fix, the deduplication - never happens for
that asset. Trace a mart asset's `depends` back far enough and it should always land on staging,
never on `pipeline/assets/generate/`.

One warning about the word "standardise", because the rubric below will pass an asset that still
carries the bug. Folding case and trimming whitespace is not the same as folding spellings.
`UPPER(TRIM('U.S.A.'))` is `U.S.A.`, not `USA`, and `UPPER(TRIM('complete'))` is `COMPLETE`, not
`COMPLETED`. Both survive a correct-looking staging asset and both change an answer later. Deciding
that two different strings mean the same thing is a mapping you write down, not a function you
call, and staging is where it belongs.

This project ships two kinds of duplicate, and they need two different fixes, which is the reason
`stg_orders.sql` and `stg_order_items.sql` will not look alike. Twelve orders were resent by the
source system with a changed status and a later `_loaded_at` - the two rows are not identical, so
`DISTINCT` keeps both. The fix is
`QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1`, which keeps the
latest row per `order_id` and discards the rest. Fifteen `order_items` rows, by contrast, are
byte-identical copies loaded twice - there `DISTINCT` is the entire fix, and a window function
would be solving a problem that is not there. `customers` needs the same `DISTINCT` treatment as
`order_items`: its ten duplicated `customer_id` values are exact repeats.

## Quiz
1. Q: Why does `stg_orders.sql` need `QUALIFY ROW_NUMBER() ... = 1` instead of `DISTINCT`?
   A: Because the duplicate `order_id` rows are not identical - one was resent with a different `order_status` and a later `_loaded_at` - so `DISTINCT` would keep both copies. Ranking by `_loaded_at` and keeping rank 1 keeps only the latest.
2. Q: Your staging asset applies `UPPER(TRIM(country))` and `COUNT(DISTINCT country)` still returns 13 rather than 12. What is left?
   A: A spelling variant, not a casing variant. `U.S.A.` survives case folding and whitespace trimming because it is a genuinely different string, so it needs an explicit mapping that says it means the same country as `USA`.
3. Q: A staging asset joins `orders` to `customers` to attach the customer's country. What layer rule does this break?
   A: Staging does cleaning only, no joins and no business logic. Attaching another table's columns is core-layer work; putting it in staging blurs the boundary that makes staging trustworthy without a review of the SQL.
4. Q: You trace a mart asset's `depends` chain and it ends at `pipeline/assets/generate/orders.sql`. What does that tell you?
   A: The mart asset, or something it depends on, has skipped the staging layer, so it is reading data that was never deduplicated or cleaned - the rule that a mart asset never reads a raw table directly has been broken somewhere in the chain.

## Task
Build the staging layer. Under `pipeline/assets/staging/`, write one asset per source table,
named `stg_<table>.sql`: `stg_dates.sql`, `stg_stores.sql`, `stg_products.sql`,
`stg_customers.sql`, `stg_orders.sql`, `stg_order_items.sql`, `stg_fx_rates.sql`. Each one
standardises text casing and trims whitespace, folds the spelling variants that case folding leaves
behind, casts types explicitly, deduplicates on the natural key, and carries a `description` stating
what one row represents. Do not edit anything under
`pipeline/assets/generate/`. Run `bruin validate` before `bruin run`, and report the row count
before and after for each table.

## Rubric (for `review my work`)
- [ ] Seven staging assets exist, one per source table, each named `stg_<table>.sql`.
- [ ] Reported row counts before and after match exactly: `dates` 1,096 to 1,096; `stores` 6 to 6; `products` 60 to 60; `customers` 510 to 500; `orders` 1,212 to 1,200; `order_items` 2,895 to 2,880; `fx_rates` 5,480 to 5,480.
- [ ] `stg_orders.sql` uses `QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1`, not `DISTINCT`.
- [ ] `stg_order_items.sql` uses `DISTINCT`, not `QUALIFY`.
- [ ] No file under `pipeline/assets/staging/` contains a `JOIN`.
- [ ] `stg_customers` maps every spelling of the home country to one value, so `SELECT COUNT(DISTINCT country) FROM stg_customers` returns 12, not 13 or 16. Case folding alone leaves `U.S.A.` standing apart and returns 13.
- [ ] `stg_orders` maps every spelling of the finished state to one value, so `SELECT COUNT(DISTINCT order_status) FROM stg_orders WHERE UPPER(order_status) LIKE 'COMPLET%'` returns 1. Case folding alone leaves `COMPLETE` apart from `COMPLETED` and returns 2.
- [ ] The student says what they did with the 24 rows that have no `order_status`. Leaving them NULL and mapping them to something like `UNKNOWN` are both defensible; not having decided is not. Do not grade this on a row count, because the two choices give different ones.

## Done signal
Confirm all seven assets exist, `bruin validate` and `bruin run` both succeed, every
before-and-after count matches the table above exactly, the country check returns 12, the
completed-state check returns 1, and the student has stated a policy for the missing statuses. Carry forward: everything built from here
on reads staging, never `pipeline/assets/generate/`, directly.
