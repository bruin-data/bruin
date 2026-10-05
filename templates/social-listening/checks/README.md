# Invariant checks

Each `.sql` file here returns the rows that break one of the template's
guarantees. An empty result means the guarantee holds. They complement the
column and custom checks inside the assets, which run on every `bruin run`;
these are cross-table checks you run after a backfill, a configuration change
or an upgrade.

```bash
bash scripts/run_checks.sh            # all checks, exits non-zero on any violation
bash scripts/run_checks.sh --env production
```

| File | Guarantee |
| --- | --- |
| `no_duplicate_raw_rows.sql` | Re-running a window never duplicates a raw record. |
| `no_duplicate_alerts.sql` | One alert per content item, destination and routing policy. |
| `no_double_delivery.sql` | No alert is delivered twice. |
| `model_failures_have_no_scores.sql` | A failed or invalid model call never produces a model score. |
| `queue_items_are_explainable.sql` | Every queue item has a source link, matched text, rule, reasons and all score components. |
| `drafts_are_review_only.sql` | Reply drafts need human approval, and the table has no publishing columns. |
| `redactions_are_applied.sql` | Redacted records are absent from raw, staging and the queue. |
