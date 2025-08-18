# `import` Command

The `import` commands allows you to automatically import existing tables in your data warehouse as Bruin assets. This command connects to your database, retrieves table metadata, and creates corresponding asset definition files in your pipeline.

```bash
bruin import database [FLAGS] [pipeline path]
```

## Overview

The import command streamlines the process of migrating existing database tables into a Bruin pipeline by:

- Connecting to your database using existing connection configurations
- Scanning database schemas and tables
- Creating asset definition files 
- Optionally filling column metadata from the database schema
- Organizing assets in the pipeline's `assets/` directory

## Arguments

| Argument | Description |
|----------|-------------|
| `pipeline path` | **Required.** Path to the directory where the pipeline and assets will be created. |

## Flags

<style>
table {
  width: 100%;
}
table th:first-child,
table td:first-child {
  white-space: nowrap;
}
</style>

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--connection`, `-c` | string | - | **Required.** Name of the connection to use as defined in `.bruin.yml` |
| `--schema`, `-s` | string | - | Filter by specific schema name |
| `--no-columns`, `-n` | bool | `false` | Skip filling column metadata from database schema |
| `--environment`, `--env` | string | - | Target environment name as defined in `.bruin.yml` |
| `--config-file` | string | - | Path to the `.bruin.yml` file. Can also be set via `BRUIN_CONFIG_FILE` environment variable |

## Supported Database Types

- **Snowflake** → `snowflake` 
- **BigQuery** → `bigquery` 
- **PostgreSQL** → `postgres` 
- **Redshift** → `redshift` 
- **Athena** → `athena` 
- **Databricks** → `databricks`
- **DuckDB** → `duckdb` 
- **ClickHouse** → `clickhouse`
- **Azure Synapse** → `synapse`
- **MS SQL Server** → `mssql` 

## How It Works

1. **Connection Setup**: Uses your existing connection configuration from `.bruin.yml`
2. **Database Scanning**: Retrieves database summary including schemas and tables
3. **Filtering**: Applies database and schema filters if specified
4. **Asset Creation**: Creates YAML asset files with naming pattern `{schema}.{table}.asset.yml`
5. **Directory Structure**: Places assets in `{pipeline_path}/assets/` directory
6. **Column Metadata**: Optionally queries table schema to populate column information

## Examples

### Basic Import

Import all tables from a Snowflake connection:

```bash
bruin import database --connection snowflake-prod ./my-pipeline
```

### Schema-Specific Import

Import only tables from a specific schema:

```bash
bruin import database --connection bigquery-dev --schema analytics ./my-pipeline
```

### Import with Column Metadata

Import tables and automatically fill column information (default behavior):

```bash
bruin import database --connection postgres-local ./my-pipeline
```

### Import without Column Metadata

Skip filling column information:

```bash
bruin import database --connection postgres-local --no-columns ./my-pipeline
```

### Environment-Specific Import

Import using a specific environment configuration:

```bash
bruin import database --connection snowflake-prod --environment production ./my-pipeline
```

## Generated Asset Structure

Each imported table creates a YAML asset file with the following structure:

```yaml
type: postgres  # or snowflake, bigquery, etc.
name: schema.table
description: "Imported table schema.table"
```

The asset file includes:
- **File Name**: `{schema}.{table}.asset.yml` (lowercase)
- **Asset Name**: `{schema}.{table}` (lowercase)
- **Description**: `"Imported table {schema}.{table}"`
- **Asset Type**: Automatically determined from connection type

### With Column Metadata (Default)

By default, the asset will include column metadata:

```yaml
type: postgres
name: schema.table
description: "Imported table schema.table"
columns:
  - name: column_name
    type: column_type
    checks: []
    upstreams: []
```

## Prerequisites

1. **Pipeline Directory**: The target pipeline path must exist
2. **Connection Configuration**: The specified connection must be defined in `.bruin.yml`
3. **Database Access**: The connection must have read permissions on the target database/schemas
4. **Assets Directory**: Will be created automatically if it doesn't exist

## Output

The command provides feedback on the import process:

```bash
Imported 25 tables and Merged 3 from data warehouse 'analytics' (schema: public) into pipeline './my-pipeline'
```

If column filling encounters issues, warnings are displayed but don't stop the import:

```bash
Warning: Could not fill columns for public.table_name: connection does not support schema introspection
```

## Error Handling

Common errors and solutions:

- **Connection not found**: Verify the connection name exists in your `.bruin.yml`
- **Database access denied**: Check connection credentials and permissions
- **Schema not found**: Verify the schema name exists in the target database
- **Pipeline path invalid**: Ensure the target directory exists and is writable

## Best Practices

1. **Start Small**: Use schema filtering for large databases to avoid importing too many tables
2. **Column Metadata**: Column metadata is filled by default for richer asset definitions
3. **Review Generated Assets**: Check and customize the generated assets  as needed


## Related Commands
- [`bruin run`](./run.md) - Execute the imported assets
- [`bruin validate`](./validate.md) - Validate the imported pipeline structure 