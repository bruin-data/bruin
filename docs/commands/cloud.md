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
| `--team` | str | - | Act on this team (company prefix) instead of your current team. Also reads from `BRUIN_CLOUD_TEAM`. Only applies to personal API keys, and only for teams you belong to. Omit it to fall back to the default set with [`cloud config set-team`](#config). |
| `--output`, `-o` | str | `plain` | Output format: `plain` or `json`. Use `json` for scripting. |

## Subcommands

### `teams`

List the teams your token can act on. Run this to discover the company prefix to pass to `--team` (or `BRUIN_CLOUD_TEAM`) — needed when your personal API key belongs to more than one team.

```bash
bruin cloud teams list
```

**Example output:**
```
+----+--------+----------------+
| ID | NAME   | COMPANY PREFIX |
+----+--------+----------------+
| 1  | Acme   | acme           |
| 2  | Globex | globex         |
+----+--------+----------------+
```

> [!TIP]
> If your token belongs to more than one team, set a default once with
> `bruin cloud config set-team <company_prefix>` and you can skip `--team` on
> every command. See [`config`](#config) below.

---

### `config`

Manage local CLI settings for Bruin Cloud. The main use is a **default team**: set it once and every `cloud` command targets that team unless you override it with `--team`.

The default team is stored under a top-level `cloud` key in your `.bruin.yml`, alongside the cloud token:

```yaml
# .bruin.yml
cloud:
  default_team: acme
```

#### `set-team`

Set the default team (its company prefix — see `bruin cloud teams list`):

```bash
bruin cloud config set-team acme
```

Bruin does a best-effort check that your current token can reach that team and warns if it can't, but still saves the value — tokens and team membership change, and the API validates the team on each request.

#### `get-team`

Print the current default team, or `none` if unset:

```bash
bruin cloud config get-team
```

#### `unset-team`

Clear the default team:

```bash
bruin cloud config unset-team
```

**How the team is resolved.** For every command that targets a team, Bruin picks, in order:

1. the `--team` flag (or `BRUIN_CLOUD_TEAM`) — always wins
2. the `cloud.default_team` from `.bruin.yml`
3. nothing — the API infers the team from your token

So a single-team token needs no default at all; a multi-team token can either pass `--team` each time or set a default once and forget it.

---

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

On success, the command prints the created run ID so you can match the invocation to its logs and status. With `--output json`, the response includes the ID in `run_id`.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--start-date` | str | - | Start date for the run (YYYY-MM-DD) |
| `--end-date` | str | - | End date for the run (YYYY-MM-DD) |
| `--asset`, `--assets` | []str | - | Select assets to run by their full name, e.g. `schema.table` (repeatable or comma-separated). |
| `--downstream` | bool | `false` | Also run everything downstream of the selected `--asset`(s). Requires `--asset`. |
| `--tag`, `-t` | []str | - | Tag the run (repeatable). A run-level label shown in the Cloud activity log — **not** an asset filter. |
| `--full-refresh`, `-r` | bool | `false` | Full-refresh the assets in the run: the `--asset` selection if given, otherwise every asset. |
| `--var` | []str | - | Override pipeline variables, as `key=value` where the value is JSON (strings need quotes, e.g. `'env="prod"'`). Can be used multiple times, or pass one JSON object. |
| `--note` | str | - | Attach a note to the run; shown in the Cloud activity log. |
| `--split` | str | - | Trigger a backfill, splitting the date range into one run per interval by unit: `minute`, `hour`, `day`, `week`, `month`, `year`. |
| `--chunk-size` | int | `1` | Number of split units per batch (used with `--split`). |

**Run only selected assets.** Select assets with `--asset` using their **full name** (`schema.table`, repeatable or comma-separated). Without a selection, the whole pipeline runs.

```bash
# Run a single asset (full schema.table name)
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --asset analytics.raw_events

# Select several assets at once (comma-separated or repeated --asset)
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --asset analytics.raw_events,analytics.daily_summary
```

**Include downstream assets.** Add `--downstream` to also run everything that depends on the selected `--asset`(s), following the pipeline's dependency graph. It requires `--asset`.

```bash
# Run raw_events and everything downstream of it
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --asset analytics.raw_events --downstream
```

**Full refresh.** Pass `--full-refresh` (bare, no value) to truncate assets before running. It covers the `--asset` selection when you give one, otherwise every asset in the pipeline.

```bash
# Full-refresh the whole pipeline (every asset)
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --full-refresh

# Run only one asset, with full refresh
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --asset analytics.standalone_report --full-refresh

# Run two assets and full-refresh both (the selection)
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --asset analytics.raw_events,analytics.daily_summary --full-refresh
```

> [!NOTE]
> `--full-refresh` truncates whatever the run covers: with `--asset` it refreshes only
> the selected assets, without it the whole pipeline.

**Override pipeline variables.** Each `--var` is `key=value`, where the **value is parsed as JSON**. So a string must be quoted (`"prod"`), while numbers and booleans are written bare. Repeat `--var` for multiple keys, or pass a whole JSON object at once.

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

**Split a range into batches (monthly, weekly, …).** With `--split`, the trigger becomes a **backfill**: the date range is split into one run per interval (by unit and chunk size) as a single backfill. This is how you backfill selected assets with monthly batches:

```bash
# One run per month across the quarter, for a single asset
bruin cloud runs trigger \
  --project-id <project-id> --pipeline <pipeline-name> \
  --start-date 2024-01-01 --end-date 2024-04-01 \
  --split month \
  --asset my_asset
```

Use `--chunk-size` to group several split units into each batch. For example weekly batches via 7-day chunks, or two-month batches:

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
> A split trigger prints the backfill ID and its Bruin Cloud tracking URL instead
> of a run ID because the individual runs are created asynchronously.

> [!NOTE]
> For a backfill, `--end-date` is **exclusive**: the range is split as
> `[start-date, end-date)`, so the last interval ends just before `--end-date`. To
> include a final period, pass the date one period past it — e.g. `--end-date 2024-01-04`
> to cover `2024-01-03` with `--split day`.

When a `--split` trigger succeeds, the command prints a link to track the backfill in the dashboard:

```text
Successfully triggered backfill (split by day, chunk size 1) for pipeline 'my-pipeline' in project 'my-project'
Track this backfill at: https://cloud.getbruin.com/<company>/projects/my-project/pipelines/my-pipeline/backfills/<backfill-id>
```

Use `bruin cloud backfills` (below) to inspect the backfill and its runs from the CLI.

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

### `backfills`

Inspect backfills — the grouped runs created by `runs trigger --split`.

#### `list`

List recent backfills, optionally filtered by project and pipeline:

```bash
bruin cloud backfills list --project-id <project-id> --pipeline <pipeline-name>
```

Each row shows the backfill ID (the `multiple_action_id`), its overall interval, and how many runs it fanned out into. Use `--limit` to control how many backfills are returned (default 20).

#### `runs`

List the individual runs that make up a single backfill, using the backfill ID from `backfills list`:

```bash
bruin cloud backfills runs --id <backfill-id>
```

Use `--limit` / `--offset` to page through the runs.

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

Interact with Bruin Cloud AI agents from the terminal. Agents are scoped to your account (team) via the API key, not to a specific project, so these commands do not take a `--project-id`.

#### `list`

List available agents:

```bash
bruin cloud agents list
```

#### `usage-stats`

Show AI usage across the agents you can see — total messages/threads, the current month's counters, and a per-agent breakdown. Defaults to the last 30 days; pass `--days`, or an explicit `--start-date`/`--end-date` window. Use `--output json` for the full payload (per-day/per-month/per-user series).

```bash
bruin cloud agents usage-stats
bruin cloud agents usage-stats --days 7
bruin cloud agents usage-stats --start-date 2026-01-01 --end-date 2026-01-31
bruin cloud agents usage-stats --output json
```

#### `delete`

Delete an agent. Cascades to its scheduled agents, dashboards and chat threads, and revokes its Cloud-CLI token. Requires an owner or team admin.

```bash
bruin cloud agents delete --agent-id <agent-id>
```

#### `connections`

List the connections available to an agent — names and types only (never credential values). Use it to pick a connection the agent can actually query, e.g. for a dashboard's `connection`:

```bash
bruin cloud agents connections --agent-id 7
bruin cloud agents connections --agent-id 7 --output json
```

##### `connections add`

Add a connection to an agent's connection set so it can query it. By default the connection is read from your local `.bruin.yml` by name; alternatively pass the credentials inline with `--credentials` (JSON) and `--type`. Credential values are sent to Bruin Cloud but never printed.

```bash
# From local .bruin.yml (reads type + credentials for the named connection)
bruin cloud agents connections add --agent-id 7 --name my_postgres

# Inline
bruin cloud agents connections add \
  --agent-id 7 \
  --name my_postgres \
  --type postgres \
  --credentials '{"username":"u","password":"p","host":"db.example.com","database":"main"}'
```

#### `mcp`

Manage an agent's external MCP servers (Linear, GitHub, Notion, …). Each MCP kind is backed by a `bruin.yml` connection from the agent's dev-env set.

`list` shows the agent's current picks plus the connections eligible for each kind:

```bash
bruin cloud agents mcp list --agent-id 7
bruin cloud agents mcp list --agent-id 7 --output json
```

`set` attaches or updates one kind (leaving the others intact):

```bash
bruin cloud agents mcp set --agent-id 7 --kind linear --connection my-linear
```

`remove` detaches a kind:

```bash
bruin cloud agents mcp remove --agent-id 7 --kind linear
```

#### `send`

Send a message to an agent:

```bash
bruin cloud agents send \
  --agent-id <agent-id> \
  --message "What pipelines failed today?"
```

To continue an existing conversation, pass a thread ID:

```bash
bruin cloud agents send \
  --agent-id <agent-id> \
  --thread-id <thread-id> \
  --message "Tell me more about that failure"
```

#### `status`

Check the status of a message (useful for async agent responses):

```bash
bruin cloud agents status \
  --agent-id <agent-id> \
  --thread-id <thread-id> \
  --message-id <message-id>
```

#### `threads`

List an agent's threads (active by default; pass `--archived` for archived ones):

```bash
bruin cloud agents threads --agent-id <agent-id>
bruin cloud agents threads --agent-id <agent-id> --archived
```

Manage a thread with the subcommands:

```bash
# Rename
bruin cloud agents threads rename --agent-id <agent-id> --thread-id <thread-id> --title "New title"

# Archive / restore (archiving also unpins)
bruin cloud agents threads archive   --agent-id <agent-id> --thread-id <thread-id>
bruin cloud agents threads unarchive --agent-id <agent-id> --thread-id <thread-id>

# Delete
bruin cloud agents threads delete --agent-id <agent-id> --thread-id <thread-id>
```

#### `messages`

List all messages in a thread:

```bash
bruin cloud agents messages \
  --agent-id <agent-id> \
  --thread-id <thread-id>
```

#### `export-thread`

Export a whole thread as JSON (thread, agent, and every message pair with input/output, agent logs, and query logs). Prints to stdout, or writes to a file with `--file`:

```bash
bruin cloud agents export-thread \
  --agent-id <agent-id> \
  --thread-id <thread-id>

bruin cloud agents export-thread \
  --agent-id <agent-id> \
  --thread-id <thread-id> \
  --file thread.json
```

### `connections`

Manage the connections stored in Bruin Cloud. Connections live in your team's vault and are shared by your cloud pipelines.

#### `add`

Push a connection to Bruin Cloud. By default it reads the connection straight from your local `.bruin.yml`, so you don't have to retype credentials:

```bash
# Reads the "my_pg" connection from the selected environment in .bruin.yml
bruin cloud connections add --name my_pg

# Pick a specific environment
bruin cloud connections add --name my_pg --environment prod

# Point at a specific config file
bruin cloud connections add --name my_pg --config-file ./path/to/.bruin.yml
```

When `--environment` is omitted, the `default_environment` from `.bruin.yml` is used (falling back to `default`).

For service-account based connections (BigQuery, GCS, Spanner, …) the CLI reads the local `service_account_file` and uploads its contents, since the cloud runner can't reach your local disk. A relative `service_account_file` is resolved against the `.bruin.yml` directory.

To add a connection without a local `.bruin.yml` (e.g. in CI), pass the credentials inline. `--type` is required in this mode:

```bash
bruin cloud connections add \
  --name my_pg \
  --type postgres \
  --credentials '{"username":"u","password":"p","host":"db.example.com","port":5432,"database":"prod"}'
```

The credentials object uses the same snake_case field names as `.bruin.yml`.

#### `list`

List the connections in your team's cloud vault (name and type):

```bash
bruin cloud connections list
bruin cloud connections list --output json
```

#### `delete`

Delete a connection by name:

```bash
bruin cloud connections delete --name my_pg
```

### `connection-sets`

Manage connection **sets** — named bundles of connections an agent runs against (assigned to an agent via its connection set). Distinct from individual connections. Credentials are write-only: reads never return config values, and create/update always send a full config per connection (read from the local `.bruin.yml`).

#### `list`

List the team's connection sets:

```bash
bruin cloud connection-sets list
```

#### `get`

List the connections in a set (names and types only — never secret values):

```bash
bruin cloud connection-sets get --set-id 7
```

#### `create`

Create a set from connections in the local `.bruin.yml` (repeat `--connection`):

```bash
bruin cloud connection-sets create --name prod \
  --connection my_pg --connection my_bq
```

Use `--skip-validation` to skip the live connection test, and `--environment` / `--config-file` to pick a specific `.bruin.yml` / environment.

#### `update`

Replace a set's connections — the set becomes exactly the connections you pass:

```bash
bruin cloud connection-sets update --set-id 7 --connection my_pg
```

#### `delete`

Delete a set (refused while an agent is still assigned to it):

```bash
bruin cloud connection-sets delete --set-id 7
```

To assign a set to an agent, use `bruin cloud agents update --agent-id <id> --connection-set-id <set-id>` (or `--connection-set-id 0` to detach).

### `dashboards`

Read the dashboards in your Bruin Cloud team — useful for inspecting or version-controlling a dashboard's definition.

#### `list`

List the team's dashboards (id, title, visibility, last updated):

```bash
bruin cloud dashboards list
bruin cloud dashboards list --output json
```

#### `get`

Get a single dashboard including its definition (`state`). By default the caller
gets the editable definition if it has edit access (the draft if one is pending,
otherwise the published state), and the published state otherwise. The response's
`has_draft`/`is_published` flags report which states exist, so a dashboard that
only has a pending draft isn't mistaken for empty.

Use `--state draft` or `--state published` to fetch a specific one. `published` is
open to any viewer; `draft` needs edit access (drafts aren't exposed to non-editors).

```bash
bruin cloud dashboards get --dashboard-id 42

# Fetch a specific state explicitly
bruin cloud dashboards get --dashboard-id 42 --state draft
bruin cloud dashboards get --dashboard-id 42 --state published

# Full payload, incl. the definition, as JSON
bruin cloud dashboards get --dashboard-id 42 --output json
```

#### `versions`

List a dashboard's version history — each snapshot's id, kind (draft/published),
author, source (ui/api), and time. Metadata only; use `version` to read a
snapshot's definition.

```bash
bruin cloud dashboards versions --dashboard-id 42
bruin cloud dashboards versions --dashboard-id 42 --output json
```

#### `version`

Get a single version snapshot including its full definition (`state`) — e.g. to see
what a past version contained or to reconstruct it.

```bash
bruin cloud dashboards version --dashboard-id 42 --version-id 108
bruin cloud dashboards version --dashboard-id 42 --version-id 108 --output json
```

#### `create`

Create a dashboard from a definition. The definition is written to the dashboard's **draft** — it is never published automatically; publish it from the Bruin Cloud UI.

Pass `--agent-id` to bind the dashboard to an agent so its canvas chat and refresh work. If omitted, the server falls back to the agent encoded in a Cloud-CLI token; a generic team token has none, so the dashboard opens without a chat panel.

```bash
# Title only (empty draft)
bruin cloud dashboards create --title "Q1 Revenue"

# Bind an agent so the dashboard's chat works
bruin cloud dashboards create --title "Q1 Revenue" --agent-id 7

# With a definition, inline or from a file
bruin cloud dashboards create --title "Q1 Revenue" --visibility team --state '{"widgets":[]}'
bruin cloud dashboards create --title "Q1 Revenue" --state-file ./dashboard.json
```

#### Dashboard definition (the `--state` / `--state-file` value)

The definition is a YAML or JSON object with these top-level keys:

| Key | Required | Description |
|-----|----------|-------------|
| `name` | yes | dashboard name |
| `connection` | for SQL widgets | a connection **the bound agent has** (see `bruin cloud connections list`). The canvas runs every widget's SQL against this connection; a name the agent doesn't have makes the queries fail |
| `description` | no | free text |
| `filters` | no | interactive controls that feed widget SQL via filter placeholders (see **Filters** below) |
| `rows` | yes | the layout — a list of rows, each holding widgets |

A **row** is `{ tab?: string, widgets: [...] }`. Widgets sit on a 12-column grid; each widget's `col` values within a row must sum to ≤ 12.

**Tabs** are expressed **per row** with a `tab:` field — there is **no top-level `tabs:` key**. Rows sharing a `tab` name render together under that tab; rows with no `tab` render above the tab bar (put shared KPIs there). Omit `tab` everywhere for a flat dashboard.

A **widget** is:

| Field | Applies to | Description |
|-------|-----------|-------------|
| `id` | all | unique, stable string |
| `type` | all | `metric`, `chart`, or `table` |
| `name` | all | widget title |
| `col` | all | column span 1–12 |
| `sql` | all | the query (BigQuery/Snowflake/… dialect of the connection) |
| `chart` | chart | `bar`, `line`, `area`, `scatter`, `histogram`, `heatmap`, `pie`, `donut` |
| `x`, `y` | chart | axis encodings — `{ field, type, format? }`; `y.field` is a **list** |
| `value` | metric | `{ field, type, format? }` for the single metric value |

Encoding `type` is `date`, `number`, or `category`. `format` is a d3 format string (e.g. `"$,.0f"`, `".1%"`, `"%b %Y"`).

Example (`dashboard.yaml`):

```yaml
name: Revenue Overview
connection: my_warehouse   # must be one of the bound agent's connections
rows:
  # No tab → always visible, above the tab bar
  - widgets:
      - { id: m_rev, type: metric, name: Total Revenue, col: 4,
          sql: "SELECT SUM(amount) AS revenue FROM orders",
          value: { field: revenue, type: number, format: "$,.0f" } }
  - tab: Trends
    widgets:
      - { id: c_rev, type: chart, chart: line, name: Revenue over time, col: 12,
          sql: "SELECT month, SUM(amount) AS revenue FROM orders GROUP BY 1 ORDER BY 1",
          x: { field: month, type: date, format: "%b %Y" },
          y: { field: [revenue], type: number, format: "$,.0f" } }
  - tab: Detail
    widgets:
      - { id: t_orders, type: table, name: Recent orders, col: 12,
          sql: "SELECT id, customer, amount FROM orders ORDER BY id DESC LIMIT 20" }
```

**Filters** are interactive controls a viewer changes; each feeds widget SQL through a placeholder named after the filter. A filter is:

| Field | Description |
|-------|-------------|
| `name` | the placeholder name used in a widget's SQL |
| `type` | `date`, `date-range`, `number`, `select`, or `text` |
| `default` | initial value. `date`: `TODAY`, `TODAY+/-N`, or `YYYY-MM-DD`; `date-range`: a preset string like `last_30_days` (resolves to a start/end pair); `select` with `multiple: true`: a list (`[]` = no filtering) |
| `multiple` | `select` only — allow selecting several values |
| `options` | for `select`/`date-range` — `values: [...]` (static) or `query: "SELECT ..."` (one column), with an optional `connection` for that query, and `presets: [...]` to choose which date-range presets to show |

```yaml
filters:
  - name: start_date
    type: date
    default: "TODAY-30"
  - name: region
    type: select
    multiple: true
    default: []
    options: { query: "SELECT DISTINCT region FROM orders ORDER BY 1" }
```

A widget's SQL references a filter with a double-brace placeholder named after the filter. Single-value filters (`date`, `text`, `number`, `select`) resolve to one value; a `date-range` filter resolves to a `.start` / `.end` pair; a multi-select (`select` with `multiple: true`) resolves to a list, so guard it and expand it:

```sql
SELECT * FROM orders
WHERE created_at >= '{{ filters.start_date }}'                 -- date / text / number / single select
  AND order_date BETWEEN '{{ filters.period.start }}'          -- date-range endpoints
                     AND '{{ filters.period.end }}'
  {% if filters.region %}                                      -- multi-select: empty list = no filter
  AND region IN ({% for r in filters.region %}'{{ r }}'{% if not loop.last %}, {% endif %}{% endfor %})
  {% endif %}
```

Filters only act on `sql` widgets, so a dashboard built purely from inline data can't use them.

#### `update`

Update an existing dashboard's title, visibility, or definition. Only the flags you pass are changed. Like `create`, a new definition is written to the dashboard's **draft** — never published automatically; publish it with `dashboards publish` or from the Bruin Cloud UI. Changing visibility requires manage-access (the dashboard creator or a team admin).

```bash
# Rename
bruin cloud dashboards update --dashboard-id 42 --title "Q1 Revenue (final)"

# Replace the definition, inline or from a file
bruin cloud dashboards update --dashboard-id 42 --state '{"widgets":[]}'
bruin cloud dashboards update --dashboard-id 42 --state-file ./dashboard.json

# Change visibility
bruin cloud dashboards update --dashboard-id 42 --visibility team
```

#### `publish`

Publish a dashboard's pending draft so it goes live — the same as clicking Publish in the Bruin Cloud UI. `create` and `update` only ever write the draft, so this is how a dashboard built or edited from the CLI becomes visible to the team. Requires edit rights on the dashboard; errors if there is no draft to publish, and repo (DAC) dashboards are published from the repo.

```bash
bruin cloud dashboards publish --dashboard-id 42
```

#### `delete`

Delete a dashboard so it stops appearing. Requires manage-access (the dashboard creator or a team admin); repo (DAC) dashboards are managed in the repo and can't be deleted here.

```bash
bruin cloud dashboards delete --dashboard-id 42
```

---

### `scheduled-agents`

Manage scheduled agents — cron-based recurring agent tasks.

#### `list` / `get`

```bash
bruin cloud scheduled-agents list
bruin cloud scheduled-agents get --scheduled-agent-id 42
bruin cloud scheduled-agents get --scheduled-agent-id 42 --output json
```

#### `create`

Create a scheduled agent from a plan. It is stored as an inactive **draft** — a human reviews and activates it from the Bruin Cloud UI; the CLI never activates a run. Pass the plan with convenience flags, or the full plan via `--state-file` (JSON or YAML with `schedule`, `instructions`, `verified_sqls`, `memory`, ...).

```bash
bruin cloud scheduled-agents create --agent-id 7 --title "Daily revenue" \
  --cron "0 9 * * *" --timezone UTC --instructions "Summarise yesterday's revenue."

bruin cloud scheduled-agents create --agent-id 7 --state-file ./plan.yaml
```

#### `update`

Update a run's title or plan. Only the flags you pass change. Activation stays in the UI.

```bash
bruin cloud scheduled-agents update --scheduled-agent-id 42 --cron "0 8 * * 1"
bruin cloud scheduled-agents update --scheduled-agent-id 42 --state-file ./plan.yaml
```

#### `trigger`

Run a scheduled agent now, off its schedule — the "Run now" action. The schedule is untouched (the next scheduled run still fires as planned). Refused while a run is already in progress. Prints the execution and thread ids.

```bash
bruin cloud scheduled-agents trigger --scheduled-agent-id 42
```

#### `delete`

Delete a scheduled agent so it stops firing.

```bash
bruin cloud scheduled-agents delete --scheduled-agent-id 42
```

#### `run-states`

Manage a scheduled agent's **run-state** files — the markdown "memory" the agent persists across runs (keyed by name, upserted on write). Reads and writes both require only the `scheduled-agent:list` ability, so any Cloud-CLI-enabled agent can manage its own run state.

```bash
# List the files on a scheduled agent
bruin cloud scheduled-agents run-states list --scheduled-agent-id 42

# Print one file's content (redirect to save it)
bruin cloud scheduled-agents run-states get --scheduled-agent-id 42 --name memory.md > memory.md

# Create or replace a file (upsert), inline or from a file
bruin cloud scheduled-agents run-states set --scheduled-agent-id 42 --name memory.md --content "notes..."
bruin cloud scheduled-agents run-states set --scheduled-agent-id 42 --name memory.md --content-file ./memory.md

# Delete a file
bruin cloud scheduled-agents run-states delete --scheduled-agent-id 42 --name memory.md
```

---

### `skills`

Manage the team **skill library** — named instruction snippets you attach to agents. Team-scoped (you only see your own team's skills).

```bash
# List
bruin cloud skills list

# Create (body inline or from a file; --all-agents applies it to every agent)
bruin cloud skills create --name reporting --description "How to write reports" --body "Be concise."
bruin cloud skills create --name reporting --description "How to write reports" --body-file ./skill.md --all-agents

# Update (only the fields you pass change)
bruin cloud skills update --skill-id 7 --body-file ./skill.md
bruin cloud skills update --skill-id 7 --all-agents=false

# Delete
bruin cloud skills delete --skill-id 7

# Set which agents have the skill (replaces the set; omit --agent-id to detach all)
bruin cloud skills set-agents --skill-id 7 --agent-id 3 --agent-id 5
```

---

### `cost`

Explore warehouse costs, mirroring the Cost Explorer in the Bruin Cloud UI.

#### `schema`

List the dimensions, filter fields, and time buckets the cost explorer supports for a platform:

```bash
bruin cloud cost schema
bruin cloud cost schema --platform databricks --output json
```

#### `explorer`

Show cost breakdowns over a date range. Group by a dimension, bucket over time, and filter:

```bash
# Total cost for July
bruin cloud cost explorer --start-date 2026-07-01 --end-date 2026-07-31

# Most expensive pipelines
bruin cloud cost explorer --start-date 2026-07-01 --end-date 2026-07-31 --dimension pipeline_id

# Daily trend for two pipelines, as JSON
bruin cloud cost explorer --start-date 2026-07-01 --end-date 2026-07-31 \
  --dimension asset_name --time-dimension day \
  --filter pipeline_id:in:daily-etl --filter pipeline_id:in:ml-features --output json
```

Filters are `field:op:value`; repeat `--filter` for multiple. For op `in`, repeat `--filter` once per value (values for the same field are combined). Large result sets paginate with `--limit` and `--offset` (the response reports the next offset).

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

To reprocess in batches — one run per month, week, or day — add `--split` (and optionally `--chunk-size`). Combine it with an `--asset` selection to backfill just part of the pipeline:

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
