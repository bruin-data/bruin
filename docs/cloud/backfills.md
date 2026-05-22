# Backfills

A **backfill** in Bruin Cloud is a time-bounded re-processing of a pipeline across a historical date range. Instead of one run, a backfill splits a date range into many runs — typically one per scheduled interval — and tracks them as a single unit. Each child run uses the same interval semantics as a CLI run, so [built-in variables](/variables/built-in) like `start_timestamp` and `end_timestamp` resolve to that child's window.

Use a backfill when you need to:

- Process historical data after adding a new pipeline.
- Reprocess after fixing a bug in upstream data.
- Catch up after a maintenance window.

## Create a backfill

Backfills are created from the **New run** modal on a pipeline's page. See [New run](/cloud/pipelines#new-run) for the full walkthrough. The two split modes:

- **Auto-split by schedule** — one run per scheduled interval. A 23-day range on a daily schedule produces 23 runs.
- **Manual split** — pick the chunk size and unit yourself. Useful when daily granularity is too fine or too coarse.

Splits are capped below 250 jobs.

## The backfill detail page

Once a backfill is created, open it from the **Backfills** tab on the pipeline page.

### Header

The header summarises the backfill at a glance:

- **Status** — running, success, failed, queued, or unknown (when child runs haven't been scheduled yet).
- **Run count** with a failed-runs breakdown.
- **Date range(s)** covered.
- **Created at** timestamp.

### Progress bar

A visual timeline of every run across the interval, colour-coded for success, failure, and unknown. Hover a segment to see the interval it covers and jump to that run.

### Runs table

Paginated list of every child run (30 per page, with auto-load on first visit). Columns:

- **Run ID** — links to the run detail.
- **Interval start / end** — the data window for that specific run.
- **Duration**
- **Status**

### Summary sidebar

Reiterates the headline stats — status, total runs, creation date — and shows a per-status breakdown (count and percentage) for quick triage.

## Actions

### Mark all runs as

Bulk-flip the status of every run in the backfill to **success** or **failed**. Useful when:

- You've confirmed the data is fine despite a misleading failure status.
- You want to invalidate the whole range and trigger a rerun upstream.

Bruin polls every 3 seconds while the mark operation propagates.

### Exclude assets across all runs

Select one or more assets in the pipeline and mark them as **skipped** in every child run. The selected assets are treated as already-succeeded so they don't actually execute. Useful when a single expensive asset doesn't need backfilling but the rest do.

### Per-run actions

For retries, cancels, and per-asset reruns inside a single backfill run, open that run from the table and use the [run detail](/cloud/runs#run-detail) controls.

## Status states

| State | Meaning |
|---|---|
| **Queued** | Backfill created, child runs not yet scheduled |
| **Running** | At least one child run is in flight |
| **Success** | All child runs finished successfully |
| **Failed** | One or more child runs failed |
| **Unknown** | Child runs not yet visible to the scheduler |

## Limits and behaviour

- **Up to 250 child runs** per backfill.
- Child runs honour the pipeline's normal asset dependencies — assets within each run still wait on their upstreams.
- Cross-pipeline sensors apply: if an asset in the backfill depends on an external upstream via [URI](/cloud/cross-pipeline), it waits for that upstream interval to complete.
- Marking the backfill as success or failed is recorded in the [audit log](/cloud/audit-logs).

## Related

- [Pipelines → New run](/cloud/pipelines#new-run) — where a backfill is created.
- [Runs](/cloud/runs) — drilling into an individual run inside a backfill.
- [Cross-pipeline dependencies](/cloud/cross-pipeline) — how external sensors behave during a backfill.
- [Built-in variables](/variables/built-in) — `start_timestamp` / `end_timestamp` per child interval.
- [Interval modifiers](/assets/interval-modifiers) — shift the data window an asset sees within each interval.
- [`bruin run`](/commands/run) — the underlying command, especially `--start-date` / `--end-date` semantics.
- [FAQ → Can I skip a single asset from scheduled runs?](/cloud/faq#can-i-skip-a-single-asset-from-scheduled-runs) — similar pattern using *exclude assets*.
