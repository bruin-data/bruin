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

TODO. Fill this in after completing the late-arrival lesson. Include the affected business-time
range, the command, and the reconciliation query.
