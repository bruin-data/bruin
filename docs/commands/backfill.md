# Backfill

`bruin backfill` splits a historical range into partitions, runs each partition through `bruin run`, and saves enough state to resume failed or interrupted work locally.

```bash
bruin backfill path/to/pipeline \
  --start-date 2019-01-01 \
  --end-date 2026-08-01 \
  --partition daily \
  --timezone UTC \
  --max-parallel 4 \
  --workers 4
```

The command prints a backfill ID and a resume command before starting. It returns a nonzero exit code when partitions fail or the invocation is cancelled.

## Preview and JSON

```bash
bruin backfill path/to/pipeline \
  --start-date 2019-01-01 --end-date 2026-08-01 \
  --partition monthly --dry-run --output json
```

A dry run does not execute assets or create backfill state. Its output includes the normalized inputs, effective parallelism, partition IDs, exact intervals, and aggregate counts. Planning requires the local configuration so the selected environment and connection limits can be resolved.

Use `--output json` without `--dry-run` to execute and emit the final state as one JSON object. Child output and progress go to stderr. JSON contains `backfill`, `options`, `summary`, `partitions`, `page`, and `has_more`. Output defaults to the first 1,000 partitions; use `--offset 1000 --limit 1000` to inspect the next page, or `--limit 0` for every partition. These flags only control output: execution and aggregate counts always cover the full plan. Each partition includes its status and attempts, with child run IDs, timestamps, and errors. `summary.skipped` counts partitions excluded from this invocation; these partitions also retain their underlying status, such as `succeeded`.

## Dates and timezones

The default timezone is explicitly `UTC`. Use an IANA name such as `America/New_York` for local calendar boundaries. Unzoned timestamps are interpreted in that timezone; timestamps with offsets identify an instant and are converted into the selected timezone.

- **Date-only end values are inclusive.** `--start-date 2024-01-01 --end-date 2024-01-03` includes January 1, 2, and 3.
- **Timestamp end values are exclusive.** To process exactly one hour, use `--start-date 2024-01-01T00:00:00Z --end-date 2024-01-01T01:00:00Z`.
- `daily`, `weekly`, `monthly`, and `yearly` align to local calendar boundaries. Weeks start on Monday. Partial first and last partitions are clipped to the requested range.
- `hourly` and positive Go durations such as `6h` or `90m` use elapsed time, anchored at the requested start. Across daylight-saving changes, a local day can contain 23 or 25 hours.
- Timestamp and duration precision is limited to whole microseconds, matching Bruin's timestamp variables.

The plan stores non-overlapping half-open intervals `[start, end)`. Each child receives `--start-date start --end-date end-minus-one-microsecond`, preserving Bruin's inclusive `end_timestamp` and `end_date` conventions. `--apply-interval-modifiers` applies the asset's modifiers through the normal run machinery, after partition planning. Those modifiers can intentionally make the effective query ranges overlap.

Initialize incremental destination tables using the usual pipeline setup before backfilling. Use a materialization that accumulates historical results, such as `append`, `merge`, or `time_interval`. A `create+replace` asset replaces its table on every partition. For `time_interval`, choose a time granularity compatible with the partition size: hourly partitions need timestamp granularity, since date granularity replaces a whole date. Full refresh and streaming runs are not supported by this command.

## Resume, retries, and failure handling

```bash
# Retry failed and interrupted partitions, and run partitions that never started.
bruin backfill --continue <backfill-id>

# Retry only partitions whose last attempt failed.
bruin backfill --continue <backfill-id> --rerun failed

# Run queued or interrupted partitions only.
bruin backfill --continue <backfill-id> --rerun missing

# Explicitly repeat every partition, including successes.
bruin backfill --continue <backfill-id> --rerun all

# Inspect saved progress without executing anything.
bruin backfill --continue <backfill-id> --dry-run --output json
```

Successful partitions are skipped by default. `--retries N` permits up to N additional attempts for each selected partition in the current invocation. Every attempt has its own child run ID and keeps its history.

`--on-failure` controls what happens after a partition exhausts its retries:

| Policy | Behavior |
| --- | --- |
| `stop` (default) | Stop launching partitions; allow active child runs to finish. |
| `continue` | Continue processing the remaining partitions. |
| `fail-fast` | Stop launching partitions and cancel active child runs. |

Ctrl+C and SIGTERM stop scheduling and propagate cancellation to active children. Bruin waits for children to exit, allowing up to 30 seconds for graceful shutdown on Unix. On Windows, child process trees are terminated. Queued and interrupted partitions remain resumable. A crash between a database commit and writing the successful partition record can cause that partition to execute again, so use idempotent materializations when resuming work with side effects.

Resume uses the saved target, date range, timezone, partitioning, environment, selectors, variables, and other run flags. Create a new backfill to change those inputs. Execution controls (`--max-parallel`, `--workers`, `--retries`, `--reverse`, and `--on-failure`) may be changed when resuming. Pipeline files and connection credentials are read again, allowing a broken asset or credential to be fixed before retrying.

## Selectors and run options

Pass a pipeline directory or a single asset file. Existing selection syntax and variable overrides work on every partition. Initial invocations also accept `BRUIN_VARS`, `BRUIN_CONFIG_FILE`, and `BRUIN_QUERY_ANNOTATIONS`; resume reuses their saved values:

```bash
bruin backfill path/to/pipeline \
  --start-date 2024-01-01 --end-date 2024-12-31 \
  --partition monthly --selector '+tag:finance' \
  --environment development --var 'region="eu"'
```

`--selector` supports upstream/downstream operators, tags, paths, unions, and intersections, just as [`bruin run`](/commands/run) does. `--tag`, `--exclude-tag`, `--downstream`, `--variant`, `--only`, `--config-file`, interval modifiers, and per-run `--timeout` are also available. Production environments require `--force` on the initial invocation.

## Parallelism and local storage

`--max-parallel` bounds the number of active partitions and defaults to 1. `--workers` bounds the asset workers inside each child and defaults to 16. Normal asset scheduling and per-connection limits still apply inside each child.

The planner also conservatively reserves each child's maximum possible use of every configured connection in the selected environment. For a connection limit L and W workers, the partition concurrency is capped at `max(1, floor(L / min(W, L)))`. This accounts for dynamically selected connections; an unused configured connection can also reduce concurrency. The effective cap is printed and included in JSON.

An environment containing a writable local DuckDB connection runs partitions sequentially because DuckDB does not allow concurrent writer processes on the same database file. Read-only DuckDB connections permit concurrent partitions. These limits coordinate the children of one backfill invocation; they do not coordinate separately launched backfills or other CLI processes.

State is stored under `<repository>/logs/backfills/<backfill-id>/`. Use `--state-dir` to choose another root, and supply it again when resuming. The store contains:

- `manifest.json`: immutable inputs used to regenerate the plan.
- `partitions/<partition-id>.json`: independently and atomically written partition state.
- `children/<child-run-id>.log`: logs for each attempt.

A process lock prevents simultaneous execution of the same backfill. Partition identities are deterministic hashes of the target, environment, run options (including selectors and variables), and interval. Changing execution order does not change them. Queued partitions are represented by the plan until their first attempt; Bruin generates and reads partitions lazily instead of loading an entire historical plan into memory.

The store is local, and its files include saved variable overrides. Keep it out of version control. Ordinary child run records also appear under `logs/runs` with the backfill group ID and partition count.
