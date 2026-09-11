# Runbook

## The pipeline failed. What now?

1. Read the error: which asset, which check, and which message?
2. Run `bruin lineage <asset>` and check whether the cause is upstream.
3. Run `bruin render <asset> --start-date <date> --end-date <date>` and inspect the SQL.
4. Query the upstream table directly with `bruin query --description "test the hypothesis"`.
5. Check `logs/runs/retail/` for the last successful run.
6. Form one hypothesis, test it with one query, then fix it.

## Known issues

The shipped `churn_risk` asset intentionally fails at runtime. Record other findings here with the
query that diagnoses them.

## Backfill procedure

Use the business-time range identified by the late-arrival audit. For the shipped data, the
affected range is 2024-10-01 through 2024-12-31. From the project root (the directory containing
`.bruin.yml`):

1. State the hypothesis and show the command before running it:
   `bruin run --start-date 2024-10-01 --end-date 2024-12-31 pipeline/assets/mart/weekly_category_revenue.sql`
2. Check that the targeted mart succeeded and that no unrelated asset was included.
3. Reconcile with:
   `bruin query --connection duckdb-default --description "reconcile October through December late-arrival totals" --limit 100 --query "$(cat queries/reconciliation/monthly-totals.sql)"`
4. Compare the three affected months with `docs/late-data-findings.md`. The revenue deltas should
   be 10,681.87 for October, 12,493.10 for November, and 6,030.35 for December.

Do not issue raw `DELETE`, `DROP`, or `TRUNCATE` statements. Bruin's incremental materialization
owns replacement of the selected business-time range; use `--start-date` and `--end-date` to keep
the repair bounded. If a full pipeline run is required, the shipped `churn_risk` failure is
intentional and unrelated to this backfill.
