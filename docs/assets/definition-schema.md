# Asset Definition

Every Bruin asset has a YAML definition — its name, type, dependencies, columns, quality checks, and so on. How that definition is stored depends on the kind of asset:

- **SQL assets (`.sql`)** embed the definition inside the same file as the query, between `/* @bruin` and `@bruin */` markers. The definition and the SQL query body live together in one `.sql` file.
- **Python assets (`.py`)** embed the definition the same way, between `""" @bruin` and `@bruin """` markers, inside the same `.py` file as the code.
- **YAML assets (`<name>.asset.yml` / `<name>.asset.yaml`)** are standalone YAML files that contain only the definition. They are used for asset types that have no inline code body — for example [ingestr](./ingestr.md), [sensor](./sensor.md), [seed](./seed.md), and [dashboard](./dashboard.md) assets.

::: danger
The definition and the query body of a SQL asset **cannot** be split across two files. A `hello_world.sql` file containing the query plus a sibling `hello_world.asset.yml` containing the definition is **not** a valid pattern — Bruin will treat them as two unrelated assets. For SQL assets, always put the `/* @bruin ... @bruin */` header at the top of the same `.sql` file that contains the query.
:::

Embedding the metadata next to the code reduces friction when things change and keeps everything for an asset in a single file. The definition includes all the details around an asset from its name to the quality checks that will be executed.

Here's an example SQL asset with an inline definition:

```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

owner: my-team@acme-corp.com

depends:
   - hello_python

materialization:
   type: table

rerun_cooldown: 300

tags:
   - dashboard
   - team:xyz

columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
        - name: unique
        - name: not_null
        - name: positive
        - name: accepted_values
          value: [1, 2]

@bruin */

select 1 as one
union all
select 2 as one
```

And here's an example of a standalone YAML asset (e.g. an ingestr or sensor asset) saved as `<name>.asset.yml` — a different kind of asset with no inline code body:

```yaml
name: raw.hello_external
type: ingestr
owner: my-team@acme-corp.com

parameters:
  source_connection: my-source
  source_table: public.hello
  destination: bigquery
```

::: info
Bruin has [an open-source Visual Studio Code](https://marketplace.visualstudio.com/items?itemName=bruin.bruin) extension that does syntax-highlighting for the definition syntax and more.
:::

::: warning
Standalone YAML assets must use the file name suffix `<name>.asset.yml` or `<name>.asset.yaml`. Plain `.yml` files are ignored, since they are typically configuration kept alongside the repo.
:::

## `name`

The name of the asset, used for many things including dependencies, materialization and more. Corresponds to the `schema.table` convention.
Must consist of letters and dot `.` character.

- **Type:** `String`

### Automatic Name Inference from File Path

The `name` field is **optional**. If not provided, Bruin automatically infers the asset name from the file path relative to the `assets/` folder:

- Each directory level becomes a segment of the name, separated by dots (`.`)
- The file name (without extension) becomes the final segment

**Examples:**

| File Path | Inferred Name |
|-----------|---------------|
| `assets/analytics/orders.sql` | `analytics.orders` |
| `assets/staging/trips.py` | `staging.trips` |
| `assets/my_project/finance/revenue.asset.yml` | `my_project.finance.revenue` |

::: warning
If you rely on name inference (i.e. the asset definition does not explicitly set `name`), files placed directly under `assets/` (e.g. `assets/orders.sql`) will infer a single-segment name like `orders`, which most databases will reject since they require at least `schema.table`. Always use at least one folder level under `assets/` when using name inference.
:::

This allows you to organize assets in folders that naturally mirror your database structure without redundantly specifying the name.

### How name segments map to your database

The inferred name is passed directly to your database, so the segments must match your database's naming convention. Different platforms interpret two-segment and three-segment names differently:

| Platform | Two segments (`a.b`) | Three segments (`a.b.c`) |
|----------|---------------------|-------------------------|
| **BigQuery** | `dataset.table` | `project.dataset.table` |
| **Snowflake** | `schema.table` | `database.schema.table` |
| **Databricks** | `schema.table` | `catalog.schema.table` |
| **MSSQL** | `schema.table` | `database.schema.table` |
| **Trino** | `schema.table` | `catalog.schema.table` |
| **DuckDB / MotherDuck** | `schema.table` | `catalog.schema.table` |
| **PostgreSQL** | `schema.table` | Not supported |
| **Redshift** | `schema.table` | Not supported |
| **Synapse** | `schema.table` | Not supported |
| **MySQL** | `database.table` | Not supported |
| **Doris** | `database.table` | Not supported |
| **ClickHouse** | `database.table` | Not supported |

For example, if you are using **BigQuery** and your folder structure is `assets/my_project/finance/revenue.sql`, the inferred name `my_project.finance.revenue` will be interpreted as project `my_project`, dataset `finance`, table `revenue`.

If you are using **Snowflake** with the same structure, it would be interpreted as database `my_project`, schema `finance`, table `revenue`.

For databases that only support two segments (like PostgreSQL, MySQL, or Doris), use a single folder level under `assets/` (e.g. `assets/public/users.sql` → `public.users`). A three-segment name on these platforms is rejected during validation.

### Auto-creation of the database/catalog

For a three-segment name, Bruin ensures the parent container exists before creating the table, where the platform supports it via SQL:

| Platform | Auto-creates the first segment? |
|----------|--------------------------------|
| **Snowflake** | Yes — `CREATE DATABASE IF NOT EXISTS` (needs the `CREATE DATABASE` privilege) |
| **Databricks** | Yes — `CREATE CATALOG IF NOT EXISTS` (needs the `CREATE CATALOG` privilege) |
| **BigQuery** | No — projects are managed outside SQL; the project must already exist |
| **Trino** | No — catalogs are connector configuration; must already exist |
| **DuckDB / MotherDuck** | No — the catalog is an attached database; must already be attached |
| **MSSQL** | No — the database must already exist |

The schema (middle segment) is auto-created on all three-level platforms. Where the database/catalog is not auto-created and does not exist, the run fails with a clear "does not exist" error rather than creating it implicitly.

::: warning Three-segment `ddl` assets that target another database (MSSQL)
A three-segment name can point at a database/catalog other than the one in your connection config, and Bruin writes the table there directly. On **MSSQL** with the `ddl` materialization strategy, schema auto-creation runs in the connection's *current* database, so a `database.schema.table` asset whose database differs from the connection's default requires the target schema to already exist in that database — otherwise the run fails with a "schema does not exist" error. Strategies that create the table directly (e.g. `create+replace`) are unaffected.
:::

**When to explicitly set `name`:**
- When your desired asset name differs from the file path structure
- When following a naming convention that doesn't match your folder layout
- When migrating existing assets with established names

## `uri`

We use `uri` (Universal Resource Identifier) as another way to identify assets. URIs must be unique across all your pipelines and can be used to define [cross pipeline dependencies](../cloud/cross-pipeline).

- **Type:** `String`

## `type`

The type of the asset determines how execution will happen. Must be one of the types listed in <a href="https://github.com/bruin-data/bruin/blob/main/pkg/pipeline/pipeline.go#L31">pkg/pipeline/pipeline.go</a>.

- **Type:** `String`

## `enabled`

Controls whether Bruin should execute the asset. Defaults to `true`.

When set to `false`, Bruin marks the asset as skipped instead of executing it. This is not treated as a failure, and downstream assets can continue running.

```yaml
enabled: false
```

`enabled` can also be a Jinja template. The rendered value must be `true` or `false`.

```yaml
enabled: "{{ var.asset_enabled }}"
```

- **Type:** `Boolean` or templated boolean

## `connection`

The connection name used to run this asset. If omitted, Bruin uses the pipeline-level `default_connections` value for the asset platform in most cases.
For Python assets with `materialization.type: table`, `connection` must be set explicitly on the asset.

```yaml
connection: bigquery-default
```

- **Type:** `String`

## `owner`

The owner of the asset, has no functional implications on Bruin CLI as of today, allows documenting the ownership information. On [Bruin Cloud](https://getbruin.com), it is used to analyze ownership information, used in governance reports and ownership lineage.

- **Type:** `String`

## `tags`

As the name states, tags that are applied to the asset. These tags can then be used while running assets, e.g.:

```bash
bruin run --tag client1
```

- **Type:** `String[]`

## `domains`

Business domains that the asset belongs to. This is used for organizing and categorizing assets by business function or domain.

- **Type:** `String[]`

## `meta`

Additional metadata for the asset stored as key-value pairs. This can be used to store custom information about the asset that doesn't fit into other predefined fields.

- **Type:** `Object`

## `depends`

The list of assets this asset depends on. This list determines the execution order.
In other words, the asset will be executed only when all of the assets in the `depends` list have succeeded.
The items of this list can be just a `String` with the name of the asset in the same pipeline or an `Object` which can contain the following attributes

- `asset` : The name of the asset. Must be on the same pipeline
- `uri` : The URI of the upstream asset. This is used in [cloud](../cloud/overview.md) when you want to have an upstream on a different pipeline. See [uri](#uri) above
- `mode`: can be `full` (a normal dependency) or `symbolic`. The latter being just for the purpose of showing lineage without the downstream actually depending or having to wait on the upstream to run.

```yaml
  - asset: asset_name
    mode: symbolic
```

## `start_date`

The start date for the asset, used when running with full refresh (`--full-refresh`). When specified, the asset will process data starting from this date during full refresh runs (overrides the pipeline's start_date).

- **Type:** `String` (YYYY-MM-DD format)

## `interval_modifiers`

Controls how the processing window is adjusted by shifting the start and end times. Requires the `--apply-interval-modifiers` flag when running the pipeline.

```yaml
interval_modifiers:
  start: -2h    # Shift start time back 2 hours
  end: 1h       # Shift end time forward 1 hour
```

You can also use [Jinja templating](./templating/templating.md) within the interval modifier values for conditional logic:

```yaml
interval_modifiers:
  start: '{% if start_timestamp|date_format("%H") == "00" %}-20d{% else %}0{% endif %}'
```

Supported time units: `ns` (nanoseconds), `ms` (milliseconds), `s` (seconds), `m` (minutes), `h` (hours), `d` (days), `M` (months).
See [interval modifiers](./interval-modifiers) for more details.

- **Type:** `Object`

## `retries`

Number of times the asset is retried on failure before it is marked as failed. If not specified, the asset inherits the pipeline-level [`retries`](../pipelines/definition.md#retries).

```yaml
retries: 3
```

**Special values:**

- unset: inherit the pipeline-level `retries`
- `0`: no retries
- `> 0`: retry the asset up to this many times

The asset-level value is, in turn, the default for the asset's [quality checks](../quality/overview.md#retries), following the resolution chain **check → asset → pipeline**.

- **Type:** `Integer`

## `rerun_cooldown`

Set a delay (in seconds) between retry attempts for failed assets. This helps prevent overwhelming downstream systems during failures and allows for temporary issues to resolve. If not specified, the asset inherits the pipeline's `rerun_cooldown` setting.

```yaml
rerun_cooldown: 300  # Wait 5 minutes between retries
```

**Special values:**

- `0`: No delay between retries (inherit from pipeline if not specified)
- `> 0`: Wait the specified number of seconds before retrying
- `-1`: Disable retry delays completely

When deploying to Airflow, this is automatically translated to `retries_delay` for compatibility.

- **Type:** `Integer`

## `routing`

Runtime routing options for the asset. This is intended for execution environments that can route task traffic through a named gateway.

```yaml
routing:
  egress_gateway: wg-shared-ams3
```

- **Type:** `Object`

| Field          | Type   | Description |
|----------------|--------|-------------|
| egress_gateway | String | Named gateway profile to use for the asset's outbound traffic |

If omitted, the asset inherits `default.routing` from `pipeline.yml` when it is set.

## `materialization`

This option determines how the asset will be materialized. Refer to the docs on [materialization](./materialization) for more details.

## `hooks`

Hooks let you run SQL snippets before and/or after the main asset query. This is useful for setup or cleanup (loading extensions, attaching databases, or writing run logs, etc.).

```yaml
hooks:
  pre:
    - query: "INSTALL httpfs"
    - query: "LOAD httpfs"
  post:
    - query: "SET s3_region=''"
```

Hooks are currently supported for SQL assets. Each hook entry supports a single `query` field and is executed in order. Queries may have a trailing `;` or not. Hook queries support Jinja templating with the same context available to asset queries.

Hooks can also be set as pipeline defaults (see [pipeline defaults](/pipelines/definition#default-pipeline-level-defaults)). Assets inherit default `pre` and `post` hooks independently - defining only `pre` hooks on an asset will still inherit default `post` hooks.

When set as pipeline defaults, hooks accept an optional `applicable_type` list that restricts which SQL asset types inherit them. When omitted, all SQL assets inherit the default hooks; when provided, only assets whose `type` matches an entry in the list inherit them:

```yaml
default:
  hooks:
    applicable_type:
      - duckdb.sql
      - bq.sql
    pre:
      - query: "SET my_var = 1"
    post:
      - query: "SET my_var = 0"
```

Only SQL asset types are valid entries; listing a non-SQL type raises a validation error. `applicable_type` has no effect when set on an individual asset's hooks.

- **Type:** `Object`

## `columns`

This is a list that contains all the columns defined with the asset, along with their quality checks and other metadata. Refer to the [columns](./columns.md) documentation for more details.

## `custom_checks`

This is a list of custom data quality checks that are applied to an asset. These checks allow you to define custom data quality checks in SQL, enabling you to encode any business logic into quality checks that might require more power.

```yaml
custom_checks:
  - name: Client X has 15 credits calculated for June 2024
    description: This client had a problem previously, therefore we want to ensure the numbers make sense, see the ticket ACME-1234 for more details.
    value: 15
    query: |
      SELECT
        count(*)
      FROM `tier2.client_credits`
      where client="client_x"
        and date_trunc(StartDateDt, month) = "2024-06-01"
        and credits_spent = 1
    blocking: true
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | str | - | A descriptive name for the check. |
| `description` | str | `""` | A human-readable description of what the check validates. |
| `query` | str | - | The SQL query to execute. |
| `value` | int | `0` | The expected integer value the query should return to pass. |
| `blocking` | bool | `false` | Whether a failure of this check should block downstream assets. |
