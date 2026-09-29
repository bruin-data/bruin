# Lesson 06: break-it-on-purpose

## Objectives
- Predict which check should catch four controlled breakages.
- Read the actual failure rather than guessing from the SQL.
- Repair each defect and rerun the relevant check.

## Concepts to teach
The failure drill changes one thing at a time: a duplicate key, an unexpected category, an orphan foreign key, and a broken grain. Predict first, run second, and record the exact error. The last case is valuable because a query can still return plausible totals after its grain changes. The shipped category check is an `accepted_values` check. The shipped orphan row is hidden because the fact query projects the nullable dimension key, so the built-in relationship check passes; the controlled edit projects the raw source key and makes the relationship failure visible.

An agent that changes four files at once erases the evidence. Keep each experiment isolated and restore the source before starting the next one. The check that fires latest tells you where your contract is weak.

## Quiz
1. Q: Why predict before running?
   A: The prediction makes the checkable hypothesis explicit and lets the error test the reasoning.
2. Q: What does an orphan foreign key mean?
   A: A child key has no matching row in the referenced parent table.
3. Q: Why restore after each breakage?
   A: A later experiment must have one known starting state and one cause.

## Task
In a disposable copy of the project, start from a clean shipped run and perform these exact edits
one at a time. Run only the named asset/check in `dev`, so the intentional `churn_risk` failure does
not hide the drill result; use `bruin validate` first, then the scoped command, and restore the file
before the next case.

1. Duplicate key: in `pipeline/assets/core/dim_customer.sql`, change `FROM stg_customers` to
   `FROM (SELECT * FROM stg_customers UNION ALL SELECT * FROM stg_customers WHERE customer_id = 1) AS duplicated`. Run
   `bruin run --environment dev --selector fqn:dim_customer pipeline`. Expect the
   `dim_customer:customer_id:unique` check to fail. Restore the original `FROM stg_customers`.
2. Unexpected category: in `pipeline/assets/core/fct_order_lines.sql`, change the fallback
   literal `COALESCE(p.category_name, 'Unknown')` to `COALESCE(p.category_name, 'Unexpected')`.
   Run `bruin run --environment dev --selector fqn:fct_order_lines pipeline`. Expect
   `fct_order_lines:category_name:accepted_values` to fail. Restore `Unknown`.
3. Orphan product: first record that the clean shipped query passes its relationship check because
   its `p.product_id` is NULL for the orphan. Then, in the same asset, change `p.product_id` in the
   first select list to `i.product_id`. Run the same scoped command. Predict a relationship failure
   and record the observed failure for the 14 orphan rows; restore `p.product_id`.
4. Broken grain: in `pipeline/assets/mart/weekly_category_revenue.sql`, remove `category_name`
   from `GROUP BY 1, 2` so it reads `GROUP BY 1`. Run
   `bruin run --environment dev --selector fqn:weekly_category_revenue pipeline`. Expect DuckDB's
   grouping error that `category_name` must appear in the `GROUP BY` clause (or the engine's exact
   equivalent), even though the intended total still looks plausible. Restore `GROUP BY 1, 2`.

Record each prediction, exact output, repair, and rerun result in `docs/failure-drill.md`. Do not
leave the shipped source changed.

## Rubric (for `review my work`)
- [ ] Records all 4 breakages in the required order with one prediction each.
- [ ] Names the check or error that actually fired for each breakage.
- [ ] Records both the clean orphan relationship pass as a contract gap and the 14-row failure after projecting the source key.
- [ ] Shows that each repair restored the run and identifies which breakage was caught latest.

## Done signal
You can use a failing run as evidence about a contract. Carry forward: the next lesson chooses how much data a run should rewrite.
