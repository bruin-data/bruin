# Ingestr Assets
[Ingestr](https://github.com/bruin-data/ingestr) is a Python package that allows you to easily move data between platforms. Bruin supports `ingestr` natively as an asset type.

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



## Template
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


> [!INFO]
> Ingestr assets require Docker to be installed in your machine. If you are using [Bruin Cloud](https://getbruin.com), you don't need to worry about this.


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
