# `format` Command

The `format` command is used to process and format asset definition files in a project. It can handle a single asset file or process all asset files in a given path. 
The command supports two output types: plain text and JSON.

## Usage

```bash
bruin format [path-to-asset-or-project-root] [flags]
```

### Arguments

**path-to-asset-or-project-root** (optional):
- If the argument is a path to an asset definition file, the command processes and formats that single asset.
- If the argument is a path to a project root, it finds and formats all asset files within that path.
- Defaults to the current directory (`.`) if no argument is provided.

### Flags

**--output / -o** (optional):  
Specifies the output format for the command.  
Possible values:
- `plain` (default): Prints human-readable messages.
- `json`: Prints errors (if any) in JSON format.  

**--fail-if-changed** (optional):  
Fail the command if any of the assets need reformatting.

**--sqlfluff** (optional):  
Run SQLFluff to format SQL files in addition to formatting Bruin asset definitions. SQLFluff is a SQL linting and formatting tool that ensures consistent SQL code style across your project.

## SQLFluff Integration

When the `--sqlfluff` flag is used, Bruin automatically:

1. **Detects SQL dialects** based on asset types (e.g., `sf.sql` → Snowflake, `bq.sql` → BigQuery)
2. **Formats SQL files** using the appropriate dialect-specific rules
3. **Processes files in parallel** for improved performance (up to 30 concurrent operations)

### Supported Database Dialects

| Asset Type Prefix | SQLFluff Dialect | Database |
|-------------------|------------------|----------|
| `sf.` | snowflake | Snowflake |
| `bq.` | bigquery | BigQuery |
| `pg.` | postgres | PostgreSQL |
| `rs.` | redshift | Amazon Redshift |
| `athena.` | athena | Amazon Athena |
| `ms.` | tsql | Microsoft SQL Server |
| `databricks.` | sparksql | Databricks |
| `synapse.` | tsql | Azure Synapse |
| `duckdb.` | duckdb | DuckDB |
| `clickhouse.` | clickhouse | ClickHouse |

### Examples

Format all assets including SQL files:
```bash
bruin format --sqlfluff
```

Format a single SQL asset:
```bash
bruin format assets/sf.my_table.sql --sqlfluff
```

Format with JSON output:
```bash
bruin format --sqlfluff --output json
```