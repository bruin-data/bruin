Bruin is a CLI tool that allows users to ingest data from many different sources, transform data using SQL and Python, run data quality checks, compare table outputs, and more. If you are reading this documentation it means Bruin CLI is already installed, therefore you can run all the relevant commands.

## Core Commands

### Command: Run
bruin run [FLAGS] [path to the pipeline/asset]
**Flags:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--downstream` | bool | false | Run all downstream tasks as well |
| `--workers` | int | 16 | Number of workers to run tasks in parallel |
| `--start-date` | str | Beginning of yesterday | Start date range (YYYY-MM-DD format) |
| `--end-date` | str | End of yesterday | End date range (YYYY-MM-DD format) |
| `--environment` | str | - | The environment to use |
| `--push-metadata` | bool | false | Push metadata to destination database (BigQuery) |
| `--force` | bool | false | Skip confirmation in production |
| `--full-refresh` | bool | false | Truncate table before running |
| `--apply-interval-modifiers` | bool | false | Apply interval modifiers |
| `--continue` | bool | false | Continue from last failed asset |
| `--tag` | str | - | Pick assets with given tag |

### Command: Query
bruin query --connection my_connection --query "SELECT * FROM table"

### Command: Init (Create Project)
bruin init [template] [folder-name]

**Available templates:**
- **Database platforms**: athena, clickhouse, duckdb, redshift
- **Cloud platforms**: firebase, gsheet-bigquery, gsheet-duckdb, shopify-bigquery, shopify-duckdb
- **Examples**: chess, duckdb-example, duckdb-lineage, frankfurter, gorgias, notion, python
- **Default**: default (basic template)

### Command: Connections
bruin connections list
bruin connections add
bruin connections ping [connection-name]

### Command: Validate
bruin validate [path to pipelines/pipeline/asset] [flags]

**Flags:**
| Flag | Alias | Description |
|------|-------|-------------|
| `--environment` | `-e, --env` | Environment to use for validation |
| `--force` | `-f` | Force validation in production environment |
| `--output [format]` | `-o` | Output format: plain, json |
| `--fast` | | Fast validation only (excludes query validation) |

### Command: Lint
bruin lint [path to pipeline/asset]

### Command: Format
bruin format [path to pipeline/asset]

### Command: Lineage
bruin lineage [path to pipeline/asset]

### Command: Data Diff
bruin data-diff [command options]

Compares data between two environments or sources. Table names can be provided as 'connection:table' or just 'table' if a default connection is set via --connection flag.

By default, the command exits with code 0 even when differences are detected. Use --fail-if-diff to exit with a non-zero code when differences are found.

**Flags:**
| Flag | Alias | Description |
|------|-------|-------------|
| `--connection value` | `-c value` | Default connection name (e.g. conn:table) |
| `--config-file value` | | Path to .bruin.yml file |
| `--tolerance value` | `-t value` | Tolerance percentage for equality (default: 0.001%) |
| `--schema-only` | | Compare only schemas, not data |
| `--fail-if-diff` | | Exit with non-zero code if differences found |
| `--help` | `-h` | Show help |

------

A bruin pipeline looks like:
```
pipeline-folder/
├── pipeline.yml          # Pipeline configuration
└── assets/               # Asset definitions
    ├── asset1.sql        # SQL asset
    ├── folder1/
    │   ├── asset2.sql    # Nested SQL asset
    │   └── asset3.py     # Python asset
    └── asset4.asset.yml  # YAML-defined asset
```

Example pipeline.yml file:
```yaml
id: pipeline-name
schedule: hourly # cron statement or daily, hourly, weekly, monthly
start_date: "2024-01-01" # Pipeline start date 
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
# name: schema.table (not required if same as assets/schema/table.sql)
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


## Key Concepts

**Assets** are the building blocks of Bruin pipelines. They can be:
- **SQL files** (`.sql`) - Database queries and transformations
- **Python files** (`.py`) - Custom Python logic and data processing
- **YAML files** (`.asset.yml`) - Asset definitions with metadata

**Pipelines** group related assets together and define how they should be executed, scheduled, and connected to databases.

## Best Practices

### **Project Structure**
* A Bruin pipeline always contains a `pipeline.yml` file and a group of assets in the `assets/` folder
* **YAML assets must end with `.asset.yml`** (not just `.yml`) to distinguish from other config files
* **Keep only assets in `/assets` folder** - create separate folders like `/analyses` or `/queries` for other files
* **For standalone SQL files**: Add `-- connection: connection_name` at the beginning of the file to specify database connection

### **Development Workflow**
* **Run individual assets, not entire pipelines** unless specifically requested
* **Validate frequently** with `bruin validate` when making changes
* **Format your code** with `bruin format` for consistency
* **Check connections** with `bruin connections list` when you need credentials
* **Get help** with `bruin help`, `bruin run help`, or `bruin query help` for any command or flag

### **Connections**
* If connection is not defined for an asset, check the `default_connections` in `pipeline.yml`
* Use `bruin connections ping [name]` to test if a connection is working

### **Common Workflows**
1. **Create new project**: `bruin init [template] [folder-name]`
2. **Add connection**: `bruin connections add`
3. **Validate setup**: `bruin validate [path]`
4. **Run asset**: `bruin run [path/to/asset.sql]`
5. **Check lineage**: `bruin lineage [path]`
