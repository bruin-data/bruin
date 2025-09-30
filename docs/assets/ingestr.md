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

| Parameter | Required | Ingestr flag | Description |
| --- | --- | --- | --- |
| `connection` | No | `--dest-uri` | Destination connection; defaults to the pipeline's connection for the asset. |
| `source_connection` | Yes | `--source-uri` | Name of the configured source connection. Bruin resolves it to the URI passed to Ingestr. |
| `source` | No | _n/a_ | Overrides the inferred source type. For example, set `gsheets` when reusing a BigQuery connection for Google Sheets. |
| `source_table` | Yes | `--source-table` | Table, sheet, or resource identifier to pull from the source. |
| `file_type` | No | `--source-table` suffix | Appended to the `source_table` as `table#type` for connectors that need a file format hint. |
| `destination` | Yes | `--dest-uri` | Logical destination used to select the target connection; Bruin converts it into the URI supplied to Ingestr. |
| `incremental_strategy` | No | `--incremental-strategy` | Passes the incremental loading strategy (`replace`, `append`, `merge`, `delete+insert`) to Ingestr. |
| `incremental_key` | No | `--incremental-key` | Column that determines incremental progress. When the column is defined with type `date`, Bruin also forwards it through the `--columns` option so Ingestr treats it as a date field. |
| `partition_by` | No | `--partition-by` | Comma-separated list of destination columns to partition by. |
| `cluster_by` | No | `--cluster-by` | Comma-separated list of destination clustering keys. |
| `loader_file_format` | No | `--loader-file-format` | Overrides the loader file format (`jsonl`, `csv`, `parquet`). |
| `loader_file_size` | No | `--loader-file-size` | Sets the maximum loader file size accepted by Ingestr. |
| `sql_backend` | No | `--sql-backend` | Selects the SQL backend Ingestr should use (`pyarrow` or `sqlalchemy`). |
| `schema_naming` | No | `--schema-naming` | Controls how Ingestr normalizes schema names. Accepted values match the Ingestr CLI. |
| `extract_parallelism` | No | `--extract-parallelism` | Limits the number of concurrent extraction workers. |
| `sql_reflection_level` | No | `--sql-reflection-level` | Tunes the amount of schema reflection performed against the source. |
| `sql_limit` | No | `--sql-limit` | Applies a `LIMIT` clause when extracting from the source. |
| `sql_exclude_columns` | No | `--sql-exclude-columns` | List of columns to skip during extraction. |
| `staging_bucket` | No | `--staging-bucket` | Overrides the staging bucket that Ingestr uses for intermediate files. |

### Column metadata

Define columns on the asset to enrich the metadata passed to Ingestr. Columns flagged as `primary_key: true` are translated into repeated `--primary-key` flags, and date-typed incremental keys automatically surface through `--columns`. See [Column metadata](./columns.md) for the syntax.

### Run configuration

Pipeline run options propagate to Ingestr automatically:

* When a run defines an interval start or end date, Bruin appends `--interval-start` and `--interval-end` with the resolved timestamps (including interval modifiers, when enabled).
* Running with `--full-refresh` adds the `--full-refresh` flag to Ingestr.

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
