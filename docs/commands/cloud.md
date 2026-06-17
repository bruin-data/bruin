# `cloud` Command

The `cloud` command lets you interact with [Bruin Cloud](https://cloud.getbruin.com) directly from your terminal. Instead of switching between the CLI and the web dashboard, you can list projects, check pipeline runs, diagnose failures, and even chat with AI agents — all without leaving your editor.

```bash
bruin cloud <subcommand> [flags]
```

## Authentication

Every `cloud` subcommand needs an API key. Bruin resolves it in this order:

1. **`--api-key` flag** — pass it directly on the command line
2. **`BRUIN_CLOUD_API_KEY` environment variable** — great for CI/CD
3. **`.bruin.yml` connection** — the most convenient option for local development

To set up the `.bruin.yml` approach, add a `bruin` connection to any environment:

```yaml
# .bruin.yml
environments:
  default:
    connections:
      bruin:
        - name: "cloud"
          api_token: "your-api-key-here"
```

Once that's in place, you can drop the `--api-key` flag entirely:

```bash
# no --api-key needed!
bruin cloud projects list
```

## Global Flags

These flags are available on all `cloud` subcommands:

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--api-key` | str | - | Bruin Cloud API key. Also reads from `BRUIN_CLOUD_API_KEY` env var or `.bruin.yml`. |
| `--output`, `-o` | str | `plain` | Output format: `plain` or `json`. Use `json` for scripting. |

## Subcommands

### `projects`

List all projects you have access to. This is usually the first command you'll run — the project ID you see here is what you'll pass to other commands.

```bash
bruin cloud projects list
```

**Example output:**
```
+--------------------+------+--------+
| ID                 | REPO | STATUS |
+--------------------+------+--------+
| buraktestpipeline  | ...  | active |
| analytics-prod     | ...  | active |
+--------------------+------+--------+
```

---

### `pipelines`

Manage pipelines within a project.

#### `list`

List all pipelines in a project:

```bash
bruin cloud pipelines list --project-id <project-id>
```

#### `get`

Get details for a specific pipeline:

```bash
bruin cloud pipelines get --project-id <project-id> --name <pipeline-name>
```

#### `errors`

Show validation errors for pipelines in a project:

```bash
bruin cloud pipelines errors --project-id <project-id>
```

#### `enable` / `disable`

Enable or disable pipelines. You can target specific pipelines or all of them at once:

```bash
# Enable a specific pipeline
bruin cloud pipelines enable --project-id <project-id> --pipeline <pipeline-name>

# Disable all pipelines in a project
bruin cloud pipelines disable --project-id <project-id>
```

#### `delete`

Delete a pipeline:

```bash
bruin cloud pipelines delete --project-id <project-id> --pipeline <pipeline-name>
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--project-id`, `-p` | str | - | Project ID (required) |
| `--name` | str | - | Pipeline name (for `get`) |
| `--pipeline` | str | - | Pipeline name (for `enable`, `disable`, `delete`) |

---

### `runs`

View, trigger, and manage pipeline runs. This is where you'll spend most of your time when debugging.

#### `list`

List recent runs for a pipeline:

```bash
bruin cloud runs list --project-id <project-id> --pipeline <pipeline-name>
```

You can filter by status to quickly find failures:

```bash
bruin cloud runs list --project-id <project-id> --pipeline <pipeline-name> --status failed
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--project-id`, `-p` | str | - | Project ID (required) |
| `--pipeline` | str | - | Pipeline name (required) |
| `--status` | str | - | Filter by status: `running`, `succeeded`, `failed` |
| `--limit` | int | `20` | Maximum number of results |
| `--offset` | int | `0` | Number of results to skip |

#### `get`

Get detailed information about a specific run:

```bash
bruin cloud runs get --project-id <project-id> --run-id <run-id>
```

#### `trigger`

Manually trigger a new pipeline run:

```bash
bruin cloud runs trigger --project-id <project-id> --pipeline <pipeline-name>
```

You can also specify a date range:

```bash
bruin cloud runs trigger \
  --project-id <project-id> \
  --pipeline <pipeline-name> \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

The flags mirror `bruin run` for the options the Cloud trigger API supports.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--start-date` | str | - | Start date for the run (YYYY-MM-DD) |
| `--end-date` | str | - | End date for the run (YYYY-MM-DD) |
| `--asset`, `--assets` | []str | - | Select assets to run by name (repeatable or comma-separated). |
| `--tag`, `-t` | []str | - | Tag the run (repeatable). A run-level label shown in the Cloud activity log — **not** an asset filter. |
| `--full-refresh`, `-r` | bool | `false` | Full-refresh every asset in the run. |
| `--full-refresh-asset` | []str | - | Full-refresh only the named asset(s); repeatable or comma-separated. Combine with `--asset` to refresh a subset. Mutually exclusive with `--full-refresh`. |
| `--var` | []str | - | Override pipeline variables, as `key=value` where the value is JSON (strings need quotes, e.g. `'env="prod"'`). Can be used multiple times, or pass one JSON object. |
| `--note` | str | - | Attach a note to the run; shown in the Cloud activity log. |
| `--split` | str | - | Trigger a backfill, splitting the date range into one run per interval by unit: `minute`, `hour`, `day`, `week`, `month`, `year`. |
| `--chunk-size` | int | `1` | Number of split units per batch (used with `--split`). |

**Run only selected assets.** Select assets by name with `--asset` (repeatable or
comma-separated). Without a selection, the whole pipeline runs.

```bash
# Run a single asset
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --asset my_asset

# Select several assets at once (comma-separated or repeated --asset)
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --asset raw_events,daily_summary
```

> [!NOTE]
> `--tag` labels the run (it shows in the Cloud activity log); it does **not** select
> assets. Select assets by name with `--asset`.

**Full refresh.** Pass `--full-refresh` (bare, no value) to truncate **every** asset in
the run before running, or `--full-refresh-asset <name>` to truncate only specific
assets (repeatable or comma-separated, e.g. `--full-refresh-asset asset_a,asset_b`).
`--full-refresh-asset` can be combined with `--asset` to refresh a subset of the selected
assets while the rest run normally; the two full-refresh flags are mutually exclusive.

```bash
# Full-refresh the whole pipeline (every asset)
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --full-refresh

# Run the whole pipeline, but full-refresh only standalone_report
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --full-refresh-asset standalone_report

# Full-refresh a couple of specific assets (runs the whole pipeline, refreshing those two)
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --full-refresh-asset raw_events,daily_summary

# Run raw_events + daily_summary, but full-refresh only raw_events
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --asset raw_events,daily_summary --full-refresh-asset raw_events

# Run raw_events + daily_summary and full-refresh both (--full-refresh covers the selection)
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --asset raw_events,daily_summary --full-refresh
```

> [!NOTE]
> `--full-refresh-asset` without `--asset` still runs the **whole pipeline** — it only
> changes which assets are truncated first. When you narrow the run with `--asset`, each
> named `--full-refresh-asset` has to be part of that selection.

**Override pipeline variables.** Each `--var` is `key=value`, where the **value is parsed
as JSON**. So a string must be quoted (`"prod"`), while numbers and booleans are written
bare. Repeat `--var` for multiple keys, or pass a whole JSON object at once.

```bash
# String value — note the JSON quotes (wrapped in single quotes for the shell)
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --var 'env="prod"'

# Several variables of different types
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --var 'env="prod"' --var retries=3 --var debug=true

# Or pass them all as one JSON object
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --var '{"env":"prod","retries":3}'
```

**Split a range into batches (monthly, weekly, …).** With `--split`, the trigger
becomes a **backfill**: The date range is splitted into one run per interval
(by unit and chunk size) as a single backfill. This is
how you backfill selected assets with monthly batches:

```bash
# One run per month across the quarter, for a single asset
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-04-01 \
  --split month \
  --asset my_asset
```

Use `--chunk-size` to group several split units into each batch. For example weekly
batches via 7-day chunks, or two-month batches:

```bash
# One run per week (7-day chunks)
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-02-12 \
  --split day --chunk-size 7

# One run per two months across the year
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2025-01-01 \
  --split month --chunk-size 2
```

> [!NOTE]
> `--split` creates a backfill.
> Without `--split`, the command triggers a single normal run.

> [!NOTE]
> For a backfill, `--end-date` is **exclusive**: the range is split as
> `[start-date, end-date)`, so the last interval ends just before `--end-date`. To
> include a final period, pass the date one period past it — e.g. `--end-date 2024-01-04`
> to cover `2024-01-03` with `--split day`.

#### `rerun`

Re-run a previous pipeline run. Useful when a transient issue caused a failure:

```bash
bruin cloud runs rerun --project-id <project-id> --run-id <run-id>
```

To rerun only the assets that failed:

```bash
bruin cloud runs rerun --project-id <project-id> --run-id <run-id> --only-failed
```

#### `mark-status`

Manually mark a run as succeeded or failed:

```bash
bruin cloud runs mark-status --project-id <project-id> --run-id <run-id> --status succeeded
```

#### `diagnose`

**This is the one you'll reach for when something breaks.** Instead of chaining together `runs list` → `runs get` → `instances list` → `instances get` → `instances logs`, the `diagnose` command does it all in a single shot. It fetches the run, finds every failed asset, and prints the failure details with error messages and check results.

```bash
# Diagnose the latest run of a pipeline
bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline-name> --latest

# Diagnose a specific run by ID
bruin cloud runs diagnose --project-id <project-id> --run-id <run-id>
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--latest` | bool | `false` | Automatically pick the most recent run |
| `--run-id` | str | - | Specific run ID to diagnose |

**Example output:**
```
=== Run Diagnosis ===
  Run ID:    manual__2026-03-06T20:01:11.741565+00:00
  Project:   buraktestpipeline
  Pipeline:  custom-check-test
  Status:    failed
  Start:     2026-03-06 20:22:29
  End:       2026-03-06 20:23:04
  Duration:  00:00:34

=== Assets (1 total, 1 failed) ===
+-------------------------------------+--------+---------------+----------+
| ASSET                               | TYPE   | STATUS        | DURATION |
+-------------------------------------+--------+---------------+----------+
| test_dataset.custom_check_fail_test | bq.sql | checks_failed | 20.8s    |
+-------------------------------------+--------+---------------+----------+

=== Failure Details ===

--- test_dataset.custom_check_fail_test / custom check: this_check_will_fail ---
  Result: 1 (expected: 999)
  Error: custom check 'this_check_will_fail' has returned 1 instead of the expected 999
```

> [!TIP]
> The `diagnose` command is especially handy when used with `--latest` — you don't even need to know the run ID. Just point it at a pipeline and it tells you what went wrong.

---

### `assets`

Browse assets across your project.

#### `list`

List all assets in a project, optionally filtered to a specific pipeline:

```bash
bruin cloud assets list --project-id <project-id>
bruin cloud assets list --project-id <project-id> --pipeline <pipeline-name>
```

#### `get`

Get details for a specific asset:

```bash
bruin cloud assets get --project-id <project-id> --pipeline <pipeline-name> --asset <asset-name>
```

---

### `instances`

Instances represent individual asset executions within a run. These commands are useful when you need to drill down into exactly what happened during a specific run.

#### `list`

List asset instances for a run:

```bash
bruin cloud instances list --project-id <project-id> --run-id <run-id>
```

#### `get`

Get details for a specific asset instance:

```bash
bruin cloud instances get --project-id <project-id> --run-id <run-id> --asset <asset-name>
```

#### `logs`

View execution logs for an asset instance:

```bash
bruin cloud instances logs --project-id <project-id> --run-id <run-id> --asset <asset-name>
```

You can also filter logs by execution step:

```bash
bruin cloud instances logs \
  --project-id <project-id> \
  --run-id <run-id> \
  --asset <asset-name> \
  --step-name "main"
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--asset` | str | - | Asset name |
| `--step-id` | str | - | Filter by step ID |
| `--step-name` | str | - | Filter by step name |
| `--try-number` | int | - | Filter by try number |

#### `failed-logs`

A shortcut to get logs for all failed assets in a run — no need to figure out which assets failed first:

```bash
bruin cloud instances failed-logs --project-id <project-id> --run-id <run-id>
```

---

### `glossary`

Access the data glossary for your project.

#### `list`

List all glossary entities:

```bash
bruin cloud glossary list --project-id <project-id>
```

#### `get`

Get details for a specific glossary entity:

```bash
bruin cloud glossary get --project-id <project-id> --entity <entity-name>
```

---

### `agents`

Interact with Bruin Cloud AI agents from the terminal.

#### `list`

List available agents:

```bash
bruin cloud agents list --project-id <project-id>
```

#### `send`

Send a message to an agent:

```bash
bruin cloud agents send \
  --project-id <project-id> \
  --agent-id <agent-id> \
  --message "What pipelines failed today?"
```

To continue an existing conversation, pass a thread ID:

```bash
bruin cloud agents send \
  --project-id <project-id> \
  --agent-id <agent-id> \
  --thread-id <thread-id> \
  --message "Tell me more about that failure"
```

#### `status`

Check the status of a message (useful for async agent responses):

```bash
bruin cloud agents status \
  --project-id <project-id> \
  --agent-id <agent-id> \
  --thread-id <thread-id> \
  --message-id <message-id>
```

#### `threads`

List all threads for an agent:

```bash
bruin cloud agents threads --project-id <project-id> --agent-id <agent-id>
```

#### `messages`

List all messages in a thread:

```bash
bruin cloud agents messages \
  --project-id <project-id> \
  --agent-id <agent-id> \
  --thread-id <thread-id>
```

---

## Common Workflows

### "My pipeline failed, what happened?"

The fastest path from "something broke" to "here's why":

```bash
bruin cloud runs diagnose --project-id my-project --pipeline my-pipeline --latest
```

That's it. One command.

### Rerun only the failed assets

When a transient issue caused a partial failure, you don't need to rerun everything:

```bash
# First, find the run ID
bruin cloud runs list --project-id my-project --pipeline my-pipeline --status failed

# Then rerun only the failures
bruin cloud runs rerun --project-id my-project --run-id <run-id> --only-failed
```

### Trigger a backfill

Need to reprocess a date range? Trigger a run with explicit dates:

```bash
bruin cloud runs trigger \
  --project-id my-project \
  --pipeline my-pipeline \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

To reprocess in batches — one run per month, week, or day — add `--split` (and
optionally `--chunk-size`). Combine it with an `--asset` selection to backfill just
part of the pipeline:

```bash
# One run per month across the year, for a single asset
bruin cloud runs trigger \
  --project-id my-project \
  --pipeline my-pipeline \
  --start-date 2024-01-01 \
  --end-date 2025-01-01 \
  --split month \
  --asset reporting_summary
```

### Script it with JSON output

All commands support `--output json` for easy integration with `jq` and other tools:

```bash
# Get failed runs as JSON and extract run IDs
bruin cloud runs list \
  --project-id my-project \
  --pipeline my-pipeline \
  --status failed \
  --output json | jq '.[].run_id'
```

## Related Topics

- [Bruin Cloud Overview](/cloud/overview) — What Bruin Cloud is and how it works
- [Cloud MCP](/cloud/mcp-setup) — AI agent integration with Bruin Cloud
- [Run Command](/commands/run) — Running pipelines locally
