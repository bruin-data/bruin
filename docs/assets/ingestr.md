# Ingestr Assets

[Ingestr](https://github.com/bruin-data/ingestr) is a CLI tool that allows you to easily move data between platforms. Bruin supports `ingestr` natively as an asset type.

Using Ingestr, you can move data from:

* your production databases like:
  * MSSQL
  * MySQL
  * Oracle
* your daily tools like:
  * Notion
  * Google Sheets
  * Airtable
* from other platforms such as:
  * Hubspot
  * Salesforce
  * Google Analytics
  * Facebook Ads
  * Google Ads

to your data warehouses:

* Google BigQuery
* Snowflake
* AWS Redshift
* Azure Synapse
* Postgres

> [!INFO]
> You can read more about the capabilities of ingestr [in its documentation](https://getbruin.com/docs/ingestr/).

## Asset Structure

```yaml
name: string
type: ingestr
connection: string # optional, by default uses the default connection for destination platform in pipeline.yml
materialization: # optional, preferred for destination write strategy
  type: table
  strategy: create+replace | append | merge | delete+insert | truncate+insert
  incremental_key: string
  partition_by: string
  cluster_by:
    - string
parameters:
  source: string # optional, used when inferring the source from connection is not enough, e.g. GCP connection + GSheets source
  source_connection: string
  source_table: string
  destination: bigquery | snowflake | redshift | synapse
  
  # optional
  version: v0 | v1 | v1.x.y
  incremental_strategy: replace | append | merge | delete+insert # legacy alternative to materialization.strategy
  incremental_key: string # legacy alternative to materialization.incremental_key
  schema_contract: evolve | freeze | discard_row | discard_value
  schema_naming: auto | direct | snake_case
  sql_backend: pyarrow | sqlalchemy
  page_size: integer
  loader_file_format: jsonl | csv | parquet
  loader_file_size: integer
  extract_parallelism: integer
  sql_limit: integer
  sql_exclude_columns: string
  no_inference: true|false
  mask: string
  trim_whitespace: true|false
  pipelines_dir: string
  staging_bucket: string
  staging_dataset: string
  stream: true|false
  flush_interval: string
  flush_records: integer
  enforce_schema: true|false # Will ensure that the columns defined in the asset are present in the destination and with the desired types (see https://getbruin.com/docs/bruin/assets/columns.html)
```

## Parameter reference

| Parameter | Required | Ingestr flag | Description |
| --- | --- | --- | --- |
| `connection` | No | `--dest-uri` | Destination connection; defaults to the pipeline's connection for the asset. |
| `source_connection` | Yes | `--source-uri` | Name of the configured source connection. Bruin resolves it to the URI passed to Ingestr. |
| `source` | No | _n/a_ | Overrides the inferred source type. For example, set `gsheets` when reusing a BigQuery connection for Google Sheets. |
| `source_table` | Yes | `--source-table` | Table, sheet, or resource identifier to pull from the source. |
| `file_type` | No | `--source-table` suffix | Appended to the `source_table` as `table#type` for connectors that need a file format hint (`csv`, `jsonl`, `parquet`). |
| `version` | No | _n/a_  | Selects the version of ingestr to install and use.. Valid options are `v1` (latest), `v0` (legacy) or `v1.x.y` (full version specifer) | 
| `materialization` | No | `--incremental-*`, `--partition-by`, `--cluster-by` | Preferred way to define destination write behavior. Supports `type: table` with `create+replace`, `append`, `merge`, `delete+insert`, and `truncate+insert`. |
| `destination` | Yes | `--dest-uri` | Logical destination used to select the target connection; Bruin converts it into the URI supplied to Ingestr. |
| `incremental_strategy` | No | `--incremental-strategy` | Passes the incremental loading strategy (`replace`, `append`, `merge`, `delete+insert`) to Ingestr. |
| `incremental_key` | No | `--incremental-key` | Column that determines incremental progress. When the column is defined with type `date`, Bruin also forwards it through the `--columns` option so Ingestr treats it as a date field. |
| `partition_by` | No | `--partition-by` | Comma-separated list of destination columns to partition by. |
| `cluster_by` | No | `--cluster-by` | Comma-separated list of destination clustering keys. |
| `schema_contract` | No | `--schema-contract` | Controls how Ingestr handles schema changes (`evolve`, `freeze`, `discard_row`, `discard_value`). |
| `loader_file_format` | No | `--loader-file-format` | Overrides the loader file format (`jsonl`, `csv`, `parquet`). |
| `loader_file_size` | No | `--loader-file-size` | Sets the maximum loader file size accepted by Ingestr. |
| `sql_backend` | No | `--sql-backend` | Selects the SQL backend Ingestr should use (`pyarrow` or `sqlalchemy`). |
| `schema_naming` | No | `--schema-naming` | Controls how Ingestr normalizes schema names. Accepted values match the [Ingestr CLI.](https://getbruin.com/docs/ingestr/commands/ingest.html#optional-flags) |
| `page_size` | No | `--page-size` | Sets the fetch page size for SQL sources. |
| `extract_parallelism` | No | `--extract-parallelism` | Limits the number of concurrent extraction workers. |
| `sql_reflection_level` | No | `--sql-reflection-level` | Tunes the amount of schema reflection performed against the source. |
| `sql_limit` | No | `--sql-limit` | Applies a `LIMIT` clause when extracting from the source. |
| `sql_exclude_columns` | No | `--sql-exclude-columns` | List of columns to skip during extraction. |
| `no_inference` | No | `--no-inference` | Uses `columns` as the source schema for schema-less sources instead of inferring types. |
| `mask` | No | `--mask` | Adds a column masking rule such as `email:hash`. |
| `pipelines_dir` | No | `--pipelines-dir` | Directory where Ingestr stores pipeline metadata. |
| `staging_bucket` | No | `--staging-bucket` | Overrides the staging bucket that Ingestr uses for intermediate files. |
| `staging_dataset` | No | `--staging-dataset` | Dataset/schema to use for staging tables. |
| `trim_whitespace` | No | `--trim-whitespace` | Trims leading and trailing whitespace from extracted string values when set to `true`. |
| `stream` | No | `--stream` | Enables continuous ingestion for CDC and message-broker sources. |
| `flush_interval` | No | `--flush-interval` | Flush interval for streaming mode, such as `30s`. |
| `flush_records` | No | `--flush-records` | Number of buffered records that triggers a flush in streaming mode. |
| `enforce_schema` | No | `--columns` | When set to `true`, enforces the column types defined in the asset's `columns` section. Ingestr will create or update the destination table with the specified schema. |

### Column metadata

Define columns on the asset to enrich the metadata passed to Ingestr. Columns flagged as `primary_key: true` are translated into repeated `--primary-key` flags, columns with `mask` are translated into repeated `--mask` flags, and date-typed incremental keys automatically surface through `--columns`. See [Column metadata](./columns.md) for the syntax.

### Custom SQL queries

For SQL sources, `source_table` can be a custom query by prefixing the SQL with `query:`:

```yaml
name: raw.recent_orders
type: ingestr
parameters:
  source_connection: postgres_prod
  source_table: "query:select id, customer_id, updated_at from public.orders where updated_at > :interval_start"
  destination: bigquery
  incremental_strategy: merge
  incremental_key: updated_at

columns:
  - name: id
    type: integer
    primary_key: true
  - name: updated_at
    type: timestamp
```

The incremental key must be returned by the query. For incremental runs, include your own timestamp filtering in the query and use Ingestr's `:interval_start` and `:interval_end` variables when needed.

### Run configuration

Pipeline run options propagate to ingestr automatically:

* When a run defines an interval start or end date, Bruin appends `--interval-start` and `--interval-end` with the resolved timestamps (including interval modifiers, when enabled).
* Running with `--full-refresh` adds the `--full-refresh` flag to Ingestr.

## Examples

The examples below show how to use the `ingestr` asset type in your pipeline. Feel free to change them as you wish according to your needs.

### Copy a table from MySQL to BigQuery

```yaml
name: raw.transactions
type: ingestr
parameters:
  source_connection: mysql_prod
  source_table: public.transactions
  destination: bigquery
```

### Copy a table from Microsoft SQL Server to Snowflake incrementally

This example shows how to use `updated_at` column to incrementally load the data from Microsoft SQL Server to Snowflake.

```yaml
name: raw.transactions
type: ingestr
parameters:
  source_connection: mysql_prod
  source_table: dbo.transactions
  destination: snowflake
materialization:
  type: table
  strategy: append
  incremental_key: updated_at
```

### Copy data from Google Sheets to Snowflake

This example shows how to copy data from Google Sheets into your Snowflake database

```yaml
name: raw.manual_orders
type: ingestr
parameters:
  source: gsheets
  source_connection: gcp-default
  source_table: <mysheetid>.<sheetname>
  destination: snowflake
```

### Enforce schema with column types

This example shows how to use `enforce_schema` to ensure the destination table has the correct column types. This is useful when the source system's type inference doesn't match your requirements.

```yaml
name: raw.users
type: ingestr
parameters:
  source_connection: mongodb_prod
  source_table: prod.users
  destination: bigquery
  enforce_schema: true
materialization:
  type: table
  strategy: merge
  incremental_key: updated_at

columns:
  - name: _id
    type: string
    primary_key: true
  - name: name
    type: string
  - name: email
    type: string
    mask: hash
  - name: age
    type: integer
  - name: created_at
    type: timestamp
  - name: updated_at
    type: timestamp
```

When `enforce_schema: true` is set, Bruin passes the column type hints to Ingestr via the `--columns` flag, ensuring the destination table schema matches your definition.
