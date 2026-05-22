# Runs

The **Runs** page is your control room for pipeline executions across every pipeline. Filter the history, watch in-flight runs, drill into per-asset logs, mark a run as success or failure, or rerun what failed. Each cloud run is the equivalent of [`bruin run`](/commands/run) executed against the right interval — same engine, same flags, just orchestrated and observed in the UI.

Open it from the top nav: **Runs**.

## The Runs page

`/runs` lists every run across every pipeline in your team. The page polls every 10 seconds while you're looking at it, so anything that finishes shows up automatically. Polling pauses after 10 minutes of idle time.

### Filters

- **Pipeline** — multi-select.
- **Status** — multi-select (running, success, failed).
- **Date range** — calendar picker plus quick presets (last 24h, 7d, 30d, custom).

### Summary cards

Two cards sit above the table:

- **Stats** — total runs loaded, plus a breakdown by Running / Success / Failed.
- **Duration** — median, average, P90, P95 across the loaded runs.

A bar chart underneath plots each run's duration, coloured green for success and red for failure. Click any bar to jump straight to that run's detail.

### Table

Columns are configurable. The defaults are:

- **Pipeline** (with link)
- **Run ID**
- **Started at**, **Finished at**
- **Interval start**, **Interval end** (the data window the run covers)
- **Duration**
- **Status**

Use **Load more** at the bottom to page back through older runs.

### Actions

The header has two action buttons (depending on your permissions):

- **New run** — opens the [New Run modal](/cloud/pipelines#new-run) to trigger any pipeline. Requires `pipeline:run:trigger`.
- **Export via email** — email yourself a CSV of the current view. Requires `pipeline:run:list`.

## Run detail

Click a run to open its detail view. Two tabs:

### Assets tab

The default view. From top to bottom:

- A large **status badge** with run ID, started / finished timestamps, interval, and total duration.
- A **donut chart** of asset types in the run (SQL, Python, ingestr, etc.).
- A **Skip skipped tasks** toggle to hide assets that didn't actually execute.
- **Asset rows** — one per asset, showing name, status, duration, and number of failed checks. Double-click a row to expand for sub-step details (main task, column checks, custom checks, prerequisites).

### Timeline tab

A Gantt-style range chart showing when each asset (and its checks) started and finished, plus any prerequisite waits for cross-pipeline dependencies. Useful for spotting bottlenecks.

### Header actions

- **Re-run** — dropdown with **Re-run all assets** and **Re-run failed assets** (disabled if there are no failures). Both let you add a **note** and **tags** that show up on the resulting run.
- **Mark as** — dropdown with **success** / **failed**. Sets the status of the run (and optionally all asset instances inside it). Useful when an upstream issue means the data is fine even though the run reported a failure, or vice versa.
- **New run** — trigger another run of the same pipeline.

## Asset instance

Click any asset row inside a run to open its **asset instance** page. This is the per-asset view of one execution. It shows:

- The status badge, start / end times, duration.
- **Logs** (if available).
- A **Retry** button to rerun just this asset (with the option to cascade to downstreams).
- **Mark as** — mark a single asset's instance as success or failed without affecting the rest of the run.
- Sub-step breakdown: main task, column checks, custom checks, prerequisites (sensors for [cross-pipeline dependencies](/cloud/cross-pipeline)).

## External dependencies and sensors

When a run includes assets that depend on something in another pipeline, you'll see **prerequisites** on those asset instances — the sensors that waited for the upstream. If an upstream pipeline is fine but the sensor is stuck, mark the asset instance as success to release the downstream.

## Why mark a run as success or failed?

Use **Mark as** when:

- The run failed for a reason that doesn't actually invalidate the data (e.g. a transient warning treated as an error). Marking success unblocks downstream runs.
- The run reported success but you've discovered the data is wrong. Marking failure surfaces it in dashboards and prompts a backfill.

Manual status changes are recorded in the [audit log](/cloud/audit-logs).

## Concurrent and in-flight runs

There's no special grouping for concurrent runs — each appears on its own row in the table. While a run is in progress, the table polls every 2 seconds for that row so the duration ticks up live. A spinning indicator in the header tells you a refresh is in progress.

## Related

- [Pipelines](/cloud/pipelines) — enable, configure, and trigger pipelines.
- [Backfills](/cloud/backfills) — multi-interval runs over historical data.
- [Scheduled Agents](/cloud/ai-agents/scheduled) — recurring AI-driven runs scheduled separately from pipelines.
- [Notifications](/cloud/notifications) — get pinged on success and failure.
- [`bruin run`](/commands/run) — CLI flags for selectors, full-refresh, intervals, and parallelism.
- [`bruin query`](/commands/query) — run an ad-hoc query against a connection (handy when triaging a failed asset).
