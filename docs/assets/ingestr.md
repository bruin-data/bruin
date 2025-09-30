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
> You can read more about the capabilities of ingestr [in its documentation](https://bruin-data.github.io/ingestr/).



## Asset Structure
```yaml
name: string
type: ingestr
connection: string # optional, by default uses the default connection for destination platform in pipeline.yml
parameters:
  source: string # optional, used when inferring the source from connection is not enough, e.g. GCP connection + GSheets source
  source_connection: string
  source_table: string
  destination: bigquery | snowflake | redshift | synapse

  # optional
  incremental_strategy: replace | append | merge | delete+insert
  incremental_key: string
  sql_backend: pyarrow | sqlalchemy
  loader_file_format: jsonl | csv | parquet
```

## Parameter reference

The following parameters map directly to the flags that Bruin passes to the underlying `ingestr` CLI when it runs your asset. Unless stated otherwise, values are strings and left unset by default.

| Parameter | Required | Description |
| --- | --- | --- |
| `source_connection` | Yes | Name of the Bruin connection that should be used as the ingestion source. This connection must implement `GetIngestrURI()` so that Bruin can translate it into an `ingestr` source URI. 【F:pkg/ingestr/operator.go†L57-L73】 |
| `source` | No | Overrides the inferred source connector when the connection alone is not sufficient (for example, GCP credentials that need the `gsheets://` prefix). 【F:pkg/ingestr/operator.go†L70-L76】 |
| `source_table` | Yes | Identifier of the table, sheet, or file to read from. Bruin forwards this value to `--source-table`. 【F:pkg/ingestr/operator.go†L78-L88】 |
| `file_type` | No | Appends a `#<type>` suffix to the source table name for file-based connectors that need to declare a format. 【F:pkg/ingestr/operator.go†L83-L88】 |
| `destination` | Yes | Logical destination type (`bigquery`, `snowflake`, `redshift`, `synapse`, `postgres`, etc.). Bruin uses it to pick the correct destination connection for the asset. 【F:pkg/helpers/helpers.go†L20-L32】 |
| `incremental_strategy` | No | Controls how `ingestr` loads data into the destination (`replace`, `append`, `merge`, or `delete+insert`). 【F:pkg/python/helper.go†L23-L25】 |
| `incremental_key` | No | Column used to track new or updated rows during incremental loads. When the column is typed as `date`, Bruin also forwards the column definition to `ingestr`. 【F:pkg/python/helper.go†L12-L19】 |
| `primary_key` | No | Derived automatically from column metadata declared on the asset so that Bruin forwards `--primary-key` flags to `ingestr`. 【F:pkg/python/helper.go†L71-L76】 |
| `loader_file_format` | No | Chooses the staging file format (`jsonl`, `csv`, or `parquet`). 【F:pkg/python/helper.go†L27-L29】 |
| `loader_file_size` | No | Overrides the size (in megabytes) of intermediate files that `ingestr` writes before loading. 【F:pkg/python/helper.go†L43-L45】 |
| `sql_backend` | No | Selects the SQL execution backend (`pyarrow` or `sqlalchemy`). 【F:pkg/python/helper.go†L39-L41】 |
| `sql_reflection_level` | No | Adjusts how aggressively `ingestr` inspects schemas before loading. 【F:pkg/python/helper.go†L55-L57】 |
| `sql_limit` | No | Applies a limit clause when discovering metadata from the source, useful for very large tables. 【F:pkg/python/helper.go†L59-L61】 |
| `sql_exclude_columns` | No | Comma-separated list of source columns to skip during ingestion. 【F:pkg/python/helper.go†L63-L65】 |
| `partition_by` | No | Sets the partition key that `ingestr` should use when writing to destinations that support partitioning. 【F:pkg/python/helper.go†L31-L33】 |
| `cluster_by` | No | Provides clustering columns for destinations that support clustering. 【F:pkg/python/helper.go†L35-L37】 |
| `schema_naming` | No | Controls how schemas are named when `ingestr` creates intermediate objects. 【F:pkg/python/helper.go†L47-L49】 |
| `extract_parallelism` | No | Configures the number of parallel workers `ingestr` uses while extracting data. 【F:pkg/python/helper.go†L51-L53】 |
| `staging_bucket` | No | Explicitly sets the cloud storage bucket for intermediate files. 【F:pkg/python/helper.go†L67-L69】 |
| `interval_start` / `interval_end` | No | Populated automatically from pipeline run intervals and forwarded as `--interval-start` / `--interval-end`. Useful for time-bounded re-runs. 【F:pkg/python/helper.go†L79-L99】 |
| `full_refresh` | No | Automatically provided when the run is triggered with full refresh; sends the `--full-refresh` flag to `ingestr`. 【F:pkg/python/helper.go†L103-L105】 |
| `path` | No | (Seed operator only) Points to a CSV file within the repository for seeding data through `ingestr`. 【F:pkg/ingestr/operator.go†L162-L175】 |

> [!NOTE]
> Parameters marked as “Automatically provided” are typically set by Bruin based on the pipeline run context. You usually don’t need to add them manually unless you want to override the derived value.

##  Examples
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
  incremental_strategy: append
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
