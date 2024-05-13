# Ingestr Assets
## ingestr
Ingestr moves is a Python package that allows you to easily move data between platforms.

With Ingestr you can move data from:
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

### Template
```yaml
name: string
type: ingestr
connection: string # optional, by default uses the default connection for destination platform in pipeline.yml
parameters:
  source_connection: string
  source_table: string
  destination: bigquery | snowflake | redshift | synapse
  
  # optional
  incremental_strategy: replace | append | merge | delete+insert
  incremental_key: string
  sql_backend: pyarrow | sqlalchemy
  loader_file_format: jsonl | csv | parquet
```

###  Examples

#### Move MySQL table to BigQuery
```yaml
name: raw.transactions
type: ingestr
parameters:
  source_connection: mysql_prod
  source_table: public.transactions
  destination: bigquery
```

#### Move MSSQL table to Snowflake incrementally using `updated_at` column
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
