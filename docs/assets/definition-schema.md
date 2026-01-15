# Asset Definition
Assets are defined in a YAML format in the same file as the asset code.
This enables the metadata to be right next to the code, reducing the friction when things change and encapsulating the relevant details in a single file.
The definition includes all the details around an asset from its name to the quality checks that will be executed.

Here's an example asset definition:
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

::: info
Bruin has [an open-source Visual Studio Code](https://marketplace.visualstudio.com/items?itemName=bruin.bruin) extension that does syntax-highlighting for the definition syntax and more.
:::

::: warning
Assets that are defined as YAML files have to have file names as `<name>.asset.yml` or `<name>.asset.yaml`. The regular `.yml` files are not considered as assets, since they might be configuration used within the repo.
:::

## `name`
The name of the asset, used for many things including dependencies, materialization and more. Corresponds to the `schema.table` convention.
Must consist of letters and dot `.` character.
- **Type:** `String`

## `uri`
We use `uri` (Universal Resource Identifier) as another way to identify assets. URIs must be unique across all your pipelines and can be used to define [cross pipeline dependencies](../cloud/cross-pipeline).
- **Type:** `String`

## `type`
The type of the asset determines how execution will happen. Must be one of the types listed in <a href="https://github.com/bruin-data/bruin/blob/main/pkg/pipeline/pipeline.go#L31">pkg/pipeline/pipeline.go</a>.
- **Type:** `String`

## `owner`
The owner of the asset, has no functional implications on Bruin CLI as of today, allows documenting the ownership information. On [Bruin Cloud](https://getbruin.com), it is used to analyze ownership information, used in governance reports and ownership lineage.
- **Type:** `String`

## `tags`
As the name states, tags that are applied to the asset. These tags can then be used while running assets, e.g.:
```bash
bruin run --tags client1
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
  * `asset` : The name of the asset. Must be on the same pipeline
  * `uri` : The URI of the upstream asset. This is used in [cloud](../cloud/overview.md) when you want to have an upstream on a different pipeline. See [uri](#uri) above
  * `mode`: can be `full` (a normal dependency) or `symbolic`. The latter being just for the purpose of showing lineage without the downstream actually depending or having to wait on the upstream to run.


```
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
Hooks are currently supported for SQL assets. Each hook entry supports a single `query` field and is executed in order. 

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
```
