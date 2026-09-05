# Google BigQuery

Google BigQuery is a fully-managed, serverless data platform that enables super-fast SQL queries using the processing power of Google's infrastructure.

Bruin supports BigQuery as a data platform.

## Connection

Google BigQuery requires a Google Cloud Platform connection, which can be added as a configuration item to `connections` in the `.bruin.yml` file complying with the following schema:

```yaml
    connections:
      google_cloud_platform:
        - name: "connection_name"
          project_id: "project-id"
          location: 'your-gcp-region' # see https://cloud.google.com/compute/docs/regions-zones
          
          # Authentication options (choose one):
          
          # Option 1: Use Application Default Credentials (ADC)
          use_application_default_credentials: true
          
          # Option 2: Specify a path to the service account file
          service_account_file: "path/to/file.json"
          
          # Option 3: Specify the service account json directly
          service_account_json: |
            {
              "type": "service_account",
              ...
            }

          # Option 4: Use a pre-minted OAuth access token (short-lived)
          access_token: "ya29...."

          # Optional query safety limits. When set, Bruin dry-runs each BigQuery
          # query before execution and stops if the estimate exceeds either limit.
          max_billable_bytes: 1000000000000
          max_query_cost: 5.00 # USD

          # Optional soft limits for `bruin query` only. Agents receive a
          # descriptive error unless they pass --dangerously-bypass-soft-limits.
          max_billable_bytes_soft: 100000000000
          max_query_cost_soft: 0.50 # USD
```

### Authentication Options

Bruin supports three authentication methods for BigQuery connections, listed in order of precedence:

#### 1. Application Default Credentials (ADC)

When `use_application_default_credentials: true` is set, Bruin will use Google Cloud's [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials#personal).

**Setup:**

```bash
# Authenticate with gcloud, with GOOGLE_APPLICATION_CREDENTIALS environment variable set
gcloud auth login

# Authenticate with gcloud by creating default credential file
gcloud auth application-default login
```

With ADC login there is no need to manage service account files, since it automatically works with gcloud authentication.

**Note:** If you have both ADC enabled and explicit credentials (service account file/JSON), ADC take precedence.

#### 2. Service Account File

Point to a service account JSON file on your filesystem:

```yaml
service_account_file: "/path/to/service-account.json"
```

#### 3. Service Account JSON (Inline)

Embed the service account credentials directly in your configuration:

```yaml
service_account_json: |
  {
    "type": "service_account",
    ...
  }
```

#### 4. OAuth Access Token

Use a pre-minted OAuth access token (e.g. from `gcloud auth print-access-token` or an OAuth flow). Access tokens are short-lived (~1 hour), so this option suits managed environments that inject a fresh token per run:

```yaml
access_token: "ya29...."
```

**Note:** Access-token connections cannot be used for ingestr assets — use a service account for those.

## BigQuery Assets

### `bq.sql`

Runs a materialized BigQuery asset or a BigQuery script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

#### Example: Create a table using table materialization

```bruin-sql
/* @bruin
name: events.install
type: bq.sql
materialization:
    type: table
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```

#### BigQuery table options

The top-level `bigquery` block configures BigQuery-specific table options and partition-scoped merge behavior. Table options are applied whenever Bruin creates a table, including the first run of a `delete+insert`, `merge`, or `time_interval` materialization. Partition expiration is also supported during an SCD2 full refresh.

```bruin-sql
/* @bruin
name: events.install
type: bq.sql
materialization:
  type: table
  partition_by: DATE(ts)
bigquery:
  require_partition_filter: true
  partition_expiration_days: 30
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `require_partition_filter` | Boolean | `false` | Requires queries to filter the table's partitioning column. |
| `partition_expiration_days` | Number | unset | Expires each partition after this many days. Must be positive; fractional days are supported. Set `0` to disable an inherited pipeline default. |
| `partition_key_immutable` | Boolean | `false` | Declares that a row's partition value cannot change for the same merge primary key. Used only by partition-scoped `merge` materializations. |

Enabling `require_partition_filter` or setting a positive `partition_expiration_days` requires `materialization.partition_by`. SCD2 materializations satisfy this requirement for partition expiration with their default `DATE(_valid_from)` partition when `partition_by` is omitted. Partition expiration is not supported for integer-range partitions.

`require_partition_filter` is not supported with SCD2 because its incremental queries must scan all target partitions. It has additional restrictions for incremental strategies: see [Merge and required partition filters](#merge-and-required-partition-filters) below. For `delete+insert` and `time_interval`, `partition_by` must use the configured `incremental_key`; `delete+insert` also requires the incremental key's column type. Bruin rejects these incompatible combinations before BigQuery can create a table that later runs cannot update.

`require_partition_filter` and `partition_expiration_days` are emitted as BigQuery [`CREATE TABLE` options](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#table_option_list), so Bruin only applies them while creating the table. A normal `append`, `merge`, `delete+insert`, `time_interval`, or `truncate+insert` run leaves an existing table's options untouched until a full refresh recreates it. The `ddl` strategy renders `CREATE TABLE IF NOT EXISTS` and is never recreated by a full refresh, so for those assets the options only take effect when the table is first created; change them on an existing table with [`ALTER TABLE SET OPTIONS`](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_table_set_options_statement). Assets that are not materialized as a table ignore the block, so a `default.bigquery` in `pipeline.yml` can be applied pipeline-wide without breaking view assets. `partition_key_immutable` is a merge-safety declaration and is not written as a BigQuery table option.

::: warning Quality checks on tables that require a partition filter
Bruin's built-in column checks (`not_null`, `unique`, `positive`, `accepted_values`, and so on) query the asset without a partition filter, and BigQuery rejects those queries on a table created with `require_partition_filter = TRUE`. If you enable this option, drop the built-in column checks on that asset and express the equivalent assertions as [custom checks](/quality/custom) whose queries filter the partitioning column.
:::

#### Merge and required partition filters

Bruin supports `merge` with `bigquery.require_partition_filter: true` by using a partition-scoped transaction. It materializes the asset query once in a temporary table, collects the exact non-null partition values produced by that query, and then:

1. captures the source rows whose primary keys do not exist in those target partitions;
2. runs an update-only `MERGE` against those target partitions; and
3. inserts the rows captured in step 1.

Step 1 runs before the `MERGE` on purpose. Reading the target again afterwards would re-select any row whose `materialization.incremental_predicate` stopped holding because the `MERGE` updated a column it references, and insert a duplicate of it.

When no column carries `update_on_merge` or `merge_sql` there is nothing to update, so steps 1 and 2 are skipped and a single insert reads the target directly.

Every target read contains a real partition predicate, so BigQuery keeps enforcing the required-filter option and prunes unrelated partitions instead of relying on a tautological filter that can still scan the whole table. The statements run in one transaction and the source query runs only once.

For correctness, the column referenced by `materialization.partition_by` must either be part of the merge primary key or be immutable for a given primary key. If it is not a primary-key column, declare that invariant explicitly:

```bruin-sql
/* @bruin
name: events.by_id
type: bq.sql
materialization:
  type: table
  strategy: merge
  partition_by: event_date
bigquery:
  require_partition_filter: true
  partition_key_immutable: true
columns:
  - name: id
    type: INT64
    primary_key: true
  - name: event_date
    type: DATE
  - name: payload
    type: STRING
    update_on_merge: true
@bruin */

select id, event_date, payload
from staging.events
```

Setting `partition_key_immutable: true` incorrectly can leave an old row behind if the same primary key later arrives with a different partition value. If partition values can move, include the partition column in the primary key, use a strategy that replaces complete partitions, or leave `require_partition_filter` disabled so a normal merge can search the whole target.

Bruin rejects the case it can prove: when `partition_by` is the bare column, a non-primary-key partition column that carries `update_on_merge` or `merge_sql` is rewritten by the merge itself, so `partition_key_immutable` cannot hold for it. With `DATE(column)` or a `*_TRUNC(column, unit)` partition the declaration is still trusted, since a value can change without crossing a partition boundary.

Partition-scoped merge currently supports `DATE`, `DATETIME`, and `TIMESTAMP` partition columns, `DATE(column)`, and the corresponding `*_TRUNC(column, unit)` expressions. The partition column must have a declared type when it is used directly. Source rows with a null partition value are rejected before the target is modified.

For details on how BigQuery uses predicates to prune partitions in DML, see [Using DML with partitioned tables](https://cloud.google.com/bigquery/docs/using-dml-with-partitioned-tables#pruning_partitions_when_using_a_merge_statement).

#### Example: Run a BigQuery script

```bruin-sql
/* @bruin
name: events.install
type: bq.sql
@bruin */

create temp table first_installs as
select 
    user_id, 
    min(ts) as install_ts,
    min_by(platform, ts) as platform,
    min_by(country, ts) as country
from analytics.events
where event_name = "install"
group by 1;

create or replace table events.install
select 
    user_id, 
    i.install_ts,
    i.platform, 
    i.country,
    a.channel
from first_installs as i
join marketing.attribution as a
    using(user_id)
```

### `bq.sensor.table`

Sensors are a special type of assets that are used to wait on certain external signals.

Checks if a table exists in BigQuery, runs by default every 30 seconds until this table is available.

```yaml
name: string
type: string
parameters:
    table: string
    poke_interval: int (optional)
    timeout: duration (optional)
```

**Parameters**:

- `table`: `project-id.dataset_id.table_id` format, requires all of the identifiers as a full name.
- `poke_interval`: The interval between retries in seconds (default 30 seconds).
- `timeout`: How long to wait before the sensor fails. Uses single-unit duration syntax (`s`, `m`, `h`, `d`, `ms`, `ns`), e.g. `1h` or `90m`. Defaults to `24h`. See [Sensor Timeout](/assets/sensor#timeout).

#### Examples

```yaml
# Google Analytics Events that checks if the recent date table is available
name: analytics_123456789.events
type: bq.sensor.table
parameters:
    table: "your-project-id.analytics_123456789.events_{{ end_date | date_format('%Y%m%d') }}"
```

### `bq.sensor.query`

Checks if a query returns any results in BigQuery, runs by default every 30 seconds until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
    poke_interval: int (optional)
    timeout: duration (optional)
```

**Parameters**:

- `query`: Query you expect to return any results
- `poke_interval`: The interval between retries in seconds (default 30 seconds).
- `timeout`: How long to wait before the sensor fails. Uses single-unit duration syntax (`s`, `m`, `h`, `d`, `ms`, `ns`), e.g. `1h` or `90m`. Defaults to `24h`. See [Sensor Timeout](/assets/sensor#timeout).

#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.

```yaml
name: analytics_123456789.events
type: bq.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}")
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.

```yaml
name: analytics_123456789.events
type: bq.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}")
```

### `bq.seed`

`bq.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your BigQuery database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the BigQuery database.

You can define seed assets in a file ending with `.asset.yml` or `.asset.yaml`:

```yaml
name: dashboard.hello
type: bq.seed

parameters:
    path: seed.csv
```

**Parameters**:

- `path`: The path to the CSV file that will be loaded into the data platform. This can be a relative file path (relative to the asset definition file) or an HTTP/HTTPS URL to a publicly accessible CSV file.

> [!WARNING]
> When using a URL path, column validation is skipped during `bruin validate`. Column mismatches will be caught at runtime.

#### Examples: Load csv into a BigQuery database

The examples below show how to load a CSV into a BigQuery database.

```yaml
name: dashboard.hello
type: bq.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```

### `bq.source`

Defines BigQuery source assets for documenting existing tables and views in your BigQuery database. These assets are no-op (they don't execute), but are useful for:

- Documenting existing BigQuery tables and views
- Adding column descriptions and metadata
- Establishing lineage relationships
- Query preview functionality in the VSCode extension

#### Example: Document an existing BigQuery table

```yaml
name: analytics.website_events
type: bq.source
description: "Raw website event data collected from tracking pixels"
connection: google_cloud_platform-default

tags:
  - analytics
  - raw-data
  - events
domains:
  - web-analytics

meta:
  business_owner: "Analytics Team"
  data_steward: "analytics@company.com"
  refresh_frequency: "real-time"

depends:
  - analytics.users
  - analytics.sessions

columns:
  - name: event_id
    type: "STRING"
    description: "Unique identifier for each event"

  - name: user_id
    type: "STRING"
    description: "Identifier of the user who triggered the event"

  - name: event_type
    type: "STRING"
    description: "Type of event (page_view, click, form_submit, etc.)"

  - name: event_timestamp
    type: "TIMESTAMP"
    description: "Timestamp when the event occurred"

  - name: page_url
    type: "STRING"
    description: "URL of the page where the event occurred"
```
