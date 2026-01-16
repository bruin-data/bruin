# Pipeline

## Overview

A pipeline is a group of assets that are executed together in the right order.
For instance, if you have an asset that ingests data from an API, and another one that creates another table from the
ingested data, you have a pipeline.

A pipeline is defined with a `pipeline.yml` file, and all the assets need to be under a folder called `assets` next to
this file:

```diff
my-pipeline/
+ ├─ pipeline.yml // you're here :)
  └─ assets/
    ├─ some.asset.yml
    ├─ another.asset.py
    └─ yet_another.asset.sql
```

Here's an example `pipeline.yml`:

```yaml
name: analytics-daily
schedule: "@daily"
start_date: "2024-01-01"

default_connections:
  snowflake: "sf-default"
  postgres: "pg-default"
  slack: "alerts-slack"

tags: [ "daily", "analytics" ]
domains: [ "marketing" ]
meta:
  owner: data-platform
  cost_center: 1234

notifications:
  slack:
    - channel: "#data-alerts"
      success: true
      failure: true
  ms_teams:
    - connection: "teams-default"
      failure: false

catchup: true
metadata_push:
  bigquery: true

retries: 2
concurrency: 4

default:
  rerun_cooldown: 300
  secrets:
    - key: MY_API_KEY
      inject_as: API_KEY
  interval_modifiers:
    start: "-1d"
    end: "-1d"
  hooks:
    pre:
      - query: "LOAD httpfs"
      - query: "SET my_var = 1"
    post:
      - query: "SET my_var = 0"


variables:
  target_segment:
    type: string
    enum: ["self_serve", "enterprise", "partner"]
    default: "enterprise"
  forecast_horizon_days:
    type: integer
    minimum: 7
    maximum: 90
    default: 30
  experiment_cohorts:
    type: array
    items:
      type: object
      required: [name, weight, channels]
      properties:
        name:
          type: string
        weight:
          type: number
        channels:
          type: array
          items:
            type: string
    default:
      - name: enterprise_baseline
        weight: 0.6
        channels: ["email", "customer_success"]
  channel_overrides:
    type: object
    properties:
      email:
        type: array
        items:
          type: string
    default:
      email: ["enterprise_newsletter"]

```

## Table of Contents

- [Name](#name)
- [Schedule](#schedule)
- [Start date](#start-date)
- [Default connections](#default-connections)
- [Tags](#tags)
- [Domains](#domains)
- [Meta](#meta)
- [Notifications](#notifications)
- [Catchup](#catchup)
- [Metadata push](#metadata-push)
- [Retries](#retries)
- [Rerun Cooldown](#rerun-cooldown)
- [Concurrency](#concurrency)
- [Default (pipeline-level defaults)](#default-pipeline-level-defaults)
- [Variables](#variables)

## Available Fields

### Name

Give your pipeline a clear, human-friendly name. It appears in UIs, logs, and tooling , keep it decriptive.

Example:

```yaml
name: analytics-daily
```

---

### **Schedule**

Defines **how often** your pipeline should execute.
This setting is used by your orchestrator (for example, Bruin Cloud or an external scheduler) to automatically trigger
the pipeline at regular intervals.

You can use simple presets like `@daily` or `@hourly`, or define a custom **cron expression** for more granular control.

Example:

```yaml
schedule: "@daily"

# Or run every hour:

schedule: "0 0 * * *"
```

* **Type:** `String`

| Value       | Description                               |
|-------------|-------------------------------------------|
| `@daily`    | Runs once per day (midnight by default)   |
| `@hourly`   | Runs every hour                           |
| `* * * * *` | Custom cron expression (minute precision) |

> In **local or ad-hoc runs**, this field is optional — you can trigger pipelines manually with `bruin run`.

### Start date

Set the earliest date from which runs should be considered. Useful for controlled backfills and catchup runs. When running with full refresh (`--full-refresh`), the pipeline will process data starting from this date.

Example:

```yaml
start_date: "2024-01-01"
```

- **Type:** `String` (ISO 8601 date, e.g., YYYY-MM-DD)

### Default connections

Define per‑platform default connection names that assets inherit automatically. Use this to avoid repeating connection
settings; override at the asset level when an asset needs a different connection.

Example:

```yaml
default_connections:
  snowflake: "sf-default"
  postgres: "pg-default"
  slack: "alerts-slack"
```

- **Type:** `Object (map[string]string)`
- **Default:** `{}`
- Notes: Keys correspond to supported platforms. Keep it short here and see docs/platforms/ for details on
  platform-specific connections.

### Tags

Attach labels to organize your pipeline and to target subsets of work. Useful for filtering in UIs/CLI (e.g., selecting
by tag) and for reporting.

Example:

```yaml
tags: [ "daily", "analytics" ]
```

- **Type:** `String[]`
- **Default:** `[]`

### Domains

Group your pipeline by business domain (e.g., marketing, finance) to improve discoverability and governance. Helps
organize views and ownership in larger repos.

Example:

```yaml
domains: [ "marketing" ]
```

- **Type:** `String[]`
- **Default:** `[]`

### Meta

Add custom key/value annotations for ownership, cost attribution, or anything your team tracks. Great for search,
dashboards, and lightweight governance.

Example:

```yaml
meta:
  owner: data-platform
  cost_center: 1234
```

- **Type:** `Object (map[string]string)`
- **Default:** `{}`

### Notifications

Send alerts when runs succeed or fail so your team stays informed. Choose one or more channels and specify where to
deliver the message (e.g., Slack channel or a webhook connection).

Example:

```yaml
notifications:
  slack:
    - channel: "#data-alerts"
      success: true   # omitting means true
      failure: true
  ms_teams:
    - connection: "teams-default"
      failure: false  # send only on success
```

- **Type:** `Object`

> This is a cloud related feature. See [Notifications](/cloud/notifications) page for more details.

### Catchup

Backfill any missed intervals between start_date and now. Turn this on when you need to automatically recover historical
runs after downtime or late onboarding.

Example:

```yaml
catchup: true
```

- **Type:** `Boolean`
- **Default:** `false`

### Metadata push

Export pipeline and asset metadata to external systems (e.g., a data catalog). Enable when you want lineage, discovery,
or governance powered by your warehouse or catalog tooling.

Example:

```yaml
metadata_push:
  bigquery: true
```

- **Type:** `Object`

Fields:

| Field    | Type    | Default | Description                 |
|----------|---------|---------|-----------------------------|
| bigquery | Boolean | false   | Export metadata to BigQuery |

### Retries

Control resilience to transient failures by retrying tasks/runs a limited number of times. Increase for flaky
networks/services; keep low to surface real issues.

Example:

```yaml
retries: 2
```

- **Type:** `Integer`
- **Default:** `2`

### Rerun Cooldown

Set a delay (in seconds) between retry attempts for failed tasks. This helps prevent overwhelming downstream systems during failures and allows for temporary issues to resolve. When deploying to Airflow, this is automatically translated to `retries_delay` for compatibility.

Example:

```yaml
default:
  rerun_cooldown: 300  # Wait 5 minutes between retries
```

- **Type:** `Integer`
- **Default:** `0` (no delay)

**Special values:**
- `0`: No delay between retries (default behavior)
- `> 0`: Wait the specified number of seconds before retrying
- `-1`: Disable retry delays (same as `0`)

**Inheritance:** Assets inherit the pipeline's default `rerun_cooldown` unless they specify their own value.

### Concurrency

Limit how many runs you can take at the same time for this pipeline in Bruin Cloud.
Defaults to 1 for safety.

Example:

```yaml
concurrency: 4
```

- **Type:** `Integer`
- **Default:** `1`

> [!WARNING]
> Setting concurrency too high can overload downstream systems. Tune based on your warehouse/engine capacity.

### Default (pipeline-level defaults)

Set sensible defaults for all assets in the pipeline so you don’t repeat yourself. Override at the asset level only when
a task needs something different.

See also: [Defaults](/getting-started/concepts#defaults).

Example:

```yaml

default:
  secrets:
    - key: MY_API_KEY
      inject_as: API_KEY
  interval_modifiers:
    start: "-1d"
    end: "-1d"
```

- **Type:** `Object`

Fields:

| Field              | Type                       | Default | Notes                            |
|--------------------|----------------------------|---------|----------------------------------|
| type               | String                     | —       | Default asset type (e.g., "sql") |
| parameters         | Object (map[string]string) | {}      | Arbitrary key/value defaults     |
| secrets            | Array of objects           | []      | See below                        |
| interval_modifiers | Object                     | —       | See [Interval Modifiers](/assets/interval-modifiers) |
| hooks              | Object                     | —       | See [Hooks](/assets/definition-schema#hooks) |

Secrets item:

| Field     | Type   | Default                 | Description              |
|-----------|--------|-------------------------|--------------------------|
| key       | String | —                       | Name of secret to inject |
| inject_as | String | defaults to same as key | Env var or param name    |

### Variables

Define pipeline-scoped parameters with safe defaults so you can change behavior without editing code.
Great for steering business logic (e.g., targeting a customer segment) or tuning data science parameters (e.g., forecast horizons).

See also: [Variables](/getting-started/pipeline-variables).

Example:

```yaml
variables:
  target_segment:
    type: string
    enum: ["self_serve", "enterprise", "partner"]
    default: "enterprise"
  forecast_horizon_days:
    type: integer
    minimum: 7
    maximum: 90
    default: 30
  experiment_cohorts:
    type: array
    items:
      type: object
      required: [name, weight, channels]
      properties:
        name:
          type: string
        weight:
          type: number
        channels:
          type: array
          items:
            type: string
    default:
      - name: enterprise_baseline
        weight: 0.6
        channels: ["email", "customer_success"]
  channel_overrides:
    type: object
    properties:
      email:
        type: array
        items:
          type: string
    default:
      email: ["enterprise_newsletter"]
```

- **Type:** `Object (map[string]variable-schema)`

Variable schema fields (subset):

| Field   | Type   | Required | Notes                                                                                   |
|---------|--------|----------|-----------------------------------------------------------------------------------------|
| type    | String | no       | JSON Schema type: string, integer, number, boolean, object, array, null. See [variables type reference](/getting-started/pipeline-variables#supported-json-schema-keywords) for details and examples. |
| default | any    | yes      | REQUIRED. Must be present; used as the variable value unless overridden                 |
| enum    | Array  | no       | Restrict values to a fixed list (e.g. `enum: ["self_serve", "enterprise", "partner"]`). See [variables documentation](/getting-started/pipeline-variables#supported-json-schema-keywords) for more JSON Schema keywords you can use. |

