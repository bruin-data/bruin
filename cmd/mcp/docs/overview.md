



It's a bruin repo.

Bruin allows you to ingest data, run SQL / Python codes. It's a mix of dbt and airflow. It's local first.

Some CLI commands:

# Command: Run
bruin run [FLAGS] [path to the pipeline/asset]
Flags:
Flag	Type	Default	Description
--downstream	bool	false	Run all downstream tasks as well.
--workers	int	16	Number of workers to run tasks in parallel.
--start-date	str	Beginning of yesterday	The start date of the range the pipeline will run for. Format: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, or YYYY-MM-DD HH:MM:SS.ffffff
--end-date	str	End of yesterday	The end date of the range the pipeline will run for. Format: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, or YYYY-MM-DD HH:MM:SS.ffffff
--environment	str	-	The environment to use.
--push-metadata	bool	false	Push metadata to the destination database if supported (currently BigQuery).
--force	bool	false	Do not ask for confirmation in a production environment.
--full-refresh	bool	false	Truncate the table before running.
--apply-interval-modifiers	bool	false	Apply interval modifiers.
--continue	bool	false	Continue from the last failed asset.
--tag	str	-	Pick assets with the given tag.

# Command: Query
bruin query --connection my_connection --query "SELECT * FROM table"

# Command: Init (Create Project)
bruin init  [template name to be used: athena|chess|clickhouse|default|duckdb|duckdb-example|duckdb-lineage|firebase|frankfurter|gorgias|gsheet-bigquery|gsheet-duckdb|notion|python|redshift|shopify-bigquery|shopify-duckdb] [name of the folder where the pipeline will be created]

# Command: Connections
bruin connections list

# Command: Validate
bruin validate [path to pipelines/pipeline/asset] [flags]

Flags:
Flag	Alias	Description
--environment	-e, --env	Specifies the environment to use for validation.
--force	-f	Forces validation even if the environment is a production environment.
--output [format]	-o	Specifies the output type, possible values: plain, json.
--fast		Runs only fast validation rules, excludes some important rules such as query validation.

# Command: Data Diff
bruin data-diff [command options]

Compares data between two environments or sources. Table names can be provided as 'connection:table' or just 'table' if a default connection is set via --connection flag.

Flags:
Flag	Alias	Description
--connection value	-c value	Name of the default connection to use, if not specified in the table argument (e.g. conn:table)
--config-file value		The path to the .bruin.yml file [$BRUIN_CONFIG_FILE]
--tolerance value	-t value	Tolerance percentage for considering values equal (default: 0.001%). Values with percentage difference below this threshold are considered equal. (default: 0.001)
--schema-only		Compare only table schemas without analyzing row counts or column distributions (default: false)
--help	-h	Show help

------

A bruin pipeline looks like:
```yaml
pipeline-folder\
    pipeline.yml -> defines a pipeline
    assets\ -> contains all the assets of this pipeline
        asset1.sql
        folder1\
            asset2.sql
            asset3.py
        asset4.asset.yml
```

Example pipeline.yml file:
```yaml
id: pipeline-name
schedule: hourly # cron statement or daily, hourly, weekly, monthly
start_date: "2024-01-01" # 
notifications:
    slack:
        - channel: internal-pipelines
          success: false
default_connections:
    google_cloud_platform: bq-connection-name # if asset type is a gcp type, it uses this connection if not defined in the asset
    snowflake: sf-connection-name # snowflake
    databricks: ... # and others
```


An example Bruin Asset YAML:
```yaml
name: schema.table
type: bq.sql
description: here's some description
owner: sabri.karagonen@getbruin.com
tags:
  - whatever
  - hello
  - attr:val1

domains:
  - domain1
  - domain2

owner: John Doe

meta:
  random_key: random_value
  random_key2: random_value2

columns:
  - name: BookingId
    type: STRING
    description: Unique identifier for the booking
    primary_key: true
  - name: UserId
    type: STRING
    description: Unique identifier for the user
    meta: # it's free form, you can add anything you want here
      is_sensitive: true
      is_pii: true

    tags:
      - hello
      - whatever

  - name: StartDateDt
    type: TIMESTAMP
    description: Date the booking starts
```


* A Bruın pipeline always contains a pipeline.yml file and a group of assets in the assets folder next to it. If you are creating a new pipeline you must adhere this rule.
* If you need credentials always check the available connections using `bruin connections list` command.
* You can and you should run `bruin validate` often when you change something in the assets or add new assets or pipelines.
* If the connection is not defined for an asset, you can find the default connection name for the pipeline in pipeline.yml file in the respective parent folder.
* Avoid running a pipeline unless it's asked for. Run almost always an asset, not a pipeline!
* Use full paths to run / validate assets.
* Feel free to always run `bruin --help` for any command or flag. Every subcommand has its own help.
