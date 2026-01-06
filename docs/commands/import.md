# `import` Command

The `import` commands allow you to automatically import existing resources from your data warehouse as Bruin assets. This includes database tables, BigQuery scheduled queries, and Tableau dashboards.

## Available Subcommands

- `bruin import database` - Import database tables as Bruin assets
- `bruin import bq-scheduled-queries` - Import BigQuery scheduled queries as Bruin assets
- `bruin import tableau` - Import Tableau dashboards, workbooks, and data sources as Bruin assets
---

## `import database`

Import existing database tables as Bruin assets.

```bash
bruin import database [FLAGS] [pipeline path]
```

### Overview

The database import command streamlines the process of migrating existing database tables into a Bruin pipeline by:

- Connecting to your database using existing connection configurations
- Scanning database schemas and tables
- Creating asset definition files 
- Optionally filling column metadata from the database schema
- Organizing assets in the pipeline's `assets/` directory

### Arguments

| Argument | Description |
|----------|-------------|
| `pipeline path` | **Required.** Path to the directory where the pipeline and assets will be created. |

### Flags

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
| `--connection`, `-c` | string | - | **Required.** Name of the connection to use as defined in `.bruin.yml` (or other [secrets backend](../secrets/overview.md)) |
| `--schema`, `-s` | string | - | Filter by specific schema name |
| `--no-columns`, `-n` | bool | `false` | Skip filling column metadata from database schema |
| `--environment`, `--env` | string | - | Target environment name as defined in `.bruin.yml` |
| `--config-file` | string | - | Path to the `.bruin.yml` file. Can also be set via `BRUIN_CONFIG_FILE` environment variable |

### Supported Database Types

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

### How It Works

1. **Connection Setup**: Uses your existing connection configuration from `.bruin.yml` (or any other [secrets backend](../secrets/overview.md))
2. **Database Scanning**: Retrieves database summary including schemas and tables
3. **Filtering**: Applies database and schema filters if specified
4. **Asset Creation**: Creates YAML source asset files named `<table>.asset.yml` under `assets/<schema>/`
5. **Directory Structure**: Places assets in `{pipeline_path}/assets/<schema>/<table>.asset.yml` (lowercase)
6. **Column Metadata**: Optionally queries table schema to populate column information

### Examples

#### Basic Import

Import all tables from a Snowflake connection:

```bash
bruin import database --connection snowflake-prod ./my-pipeline
```

#### Schema-Specific Import

Import only tables from a specific schema:

```bash
bruin import database --connection bigquery-dev --schema analytics ./my-pipeline
```

#### Import with Column Metadata

Import tables and automatically fill column information (default behavior):

```bash
bruin import database --connection postgres-local ./my-pipeline
```

#### Import without Column Metadata

Skip filling column information:

```bash
bruin import database --connection postgres-local --no-columns ./my-pipeline
```

#### Environment-Specific Import

Import using a specific environment configuration:

```bash
bruin import database --connection snowflake-prod --environment production ./my-pipeline
```

### Generated Asset Structure

Each imported table creates a YAML **source** asset file with the following structure:

```yaml
type: postgres  # or snowflake, bigquery, etc.
name: schema.table
description: "Imported table schema.table"
```

These are metadata-only source assets. SQL transformation assets live in `.sql` files, and `import database` does not generate SQL templates.

The asset file includes:
- **File Name**: `{schema}.{table}.asset.yml` (lowercase)
- **Asset Name**: `{schema}.{table}` (lowercase)
- **Description**: `"Imported table {schema}.{table}"`
- **Asset Type**: Automatically determined from connection type

#### With Column Metadata (Default)

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

### Prerequisites

1. **Pipeline Directory**: The target pipeline path must exist
2. **Connection Configuration**: The specified connection must be defined in `.bruin.yml` (or any other [secrets backend](../secrets/overview.md))
3. **Database Access**: The connection must have read permissions on the target database/schemas
4. **Assets Directory**: Will be created automatically if it doesn't exist

### Output

The command provides feedback on the import process:

```bash
Imported 25 tables and Merged 3 from data warehouse 'analytics' (schema: public) into pipeline './my-pipeline'
```

If column filling encounters issues, warnings are displayed but don't stop the import:

```bash
Warning: Could not fill columns for public.table_name: connection does not support schema introspection
```

### Error Handling

Common errors and solutions:

- **Connection not found**: Verify the connection name exists in your `.bruin.yml` (or any other [secrets backend](../secrets/overview.md))
- **Database access denied**: Check connection credentials and permissions
- **Schema not found**: Verify the schema name exists in the target database
- **Pipeline path invalid**: Ensure the target directory exists and is writable

### Best Practices

1. **Start Small**: Use schema filtering for large databases to avoid importing too many tables
2. **Column Metadata**: Column metadata is filled by default for richer asset definitions
3. **Review Generated Assets**: Check and customize the generated assets  as needed


### Related Commands
- [`bruin run`](./run.md) - Execute the imported assets
- [`bruin validate`](./validate.md) - Validate the imported pipeline structure

---

## `import bq-scheduled-queries`

Import BigQuery scheduled queries from the Data Transfer Service as individual Bruin assets.

```bash
bruin import bq-scheduled-queries [FLAGS] [pipeline path]
```

### Overview

The BigQuery scheduled queries import command allows you to:

- Connect to BigQuery Data Transfer Service
- Automatically scan across all BigQuery regions to find scheduled queries
- Present queries in an interactive terminal UI for selection
- Import selected queries as SQL assets in your Bruin pipeline

### Interactive UI Features

The command presents an interactive dual-pane interface where you can:

- **Navigate** with arrow keys or `j`/`k`
- **Select/deselect** queries with space bar
- **Select all** with `a`, **deselect all** with `n`
- **Switch panes** with Tab to scroll query details
- **Import selected** queries with Enter
- **Quit** without importing with `q` or Esc

### Arguments

| Argument | Description |
|----------|-------------|
| `pipeline path` | **Required.** Path to the directory where the pipeline and imported query assets will be created. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--connection`, `-c` | string | - | **Required.** Name of the BigQuery connection to use as defined in `.bruin.yml` (or any other [secrets backend](../secrets/overview.md)) |
| `--environment`, `--env` | string | - | Target environment name as defined in `.bruin.yml` |
| `--config-file` | string | - | Path to the `.bruin.yml` file. Can also be set via `BRUIN_CONFIG_FILE` environment variable |
| `--project-id`, `-p` | string | - | BigQuery project ID (uses connection config if not specified) |
| `--location`, `-l` | string | - | BigQuery location/region (searches all regions if not specified) |

### How It Works

1. **Authentication**: Uses your BigQuery connection credentials from `.bruin.yml` (or any other [secrets backend](../secrets/overview.md))
2. **Regional Scanning**: Searches across all BigQuery regions in parallel for scheduled queries (unless specific location provided)
3. **Interactive Selection**: Displays found queries in a user-friendly TUI with preview
4. **Asset Generation**: Creates `.sql` files with the query content and appropriate metadata
5. **Pipeline Integration**: Places assets in the pipeline's `assets/` directory

### Examples

#### Basic Import

Import scheduled queries using default connection settings:

```bash
bruin import bq-scheduled-queries ./my-pipeline --connection my-bq-conn
```

#### Environment-Specific Import

Import using a specific environment configuration:

```bash
bruin import bq-scheduled-queries ./my-pipeline --connection bq-prod --env production
```

#### Region-Specific Import

Import queries from a specific BigQuery region only:

```bash
bruin import bq-scheduled-queries ./my-pipeline --connection my-bq --location us-central1
```

#### Custom Project Import

Import from a specific GCP project:

```bash
bruin import bq-scheduled-queries ./my-pipeline --connection my-bq --project-id my-gcp-project
```

### Generated Asset Structure

Each imported scheduled query creates a SQL file with:

- **File Name**: Sanitized version of the query display name with `.sql` extension
- **Asset Type**: `bq.query` (BigQuery query asset)
- **Content**: The original SQL query from the scheduled query
- **Description**: References the original scheduled query name
- **Materialization**: Table materialization if the query has a target dataset

Example generated asset:

```sql
/* @bruin
name: sales_daily_summary
type: bq.query
description: "Imported from scheduled query: Sales Daily Summary"

materialization:
  type: table
@bruin */

SELECT 
  date,
  SUM(revenue) as total_revenue,
  COUNT(DISTINCT customer_id) as unique_customers
FROM sales_data
WHERE date = CURRENT_DATE()
GROUP BY date
```

### Prerequisites

1. **BigQuery Connection**: A BigQuery connection must be configured in `.bruin.yml` (or other [secrets backend](../secrets/overview.md))
2. **Data Transfer API**: The BigQuery Data Transfer API must be enabled in your GCP project
3. **Permissions**: Your service account needs:
   - `bigquery.transfers.get` permission
   - `bigquery.transfers.list` permission
4. **Pipeline Directory**: The target pipeline path must exist

### Output

The command provides real-time feedback during the import process:

```
🔍 Searching for scheduled queries across all BigQuery regions...
✨ Found 5 queries in us-central1
✨ Found 3 queries in europe-west1
🎉 Search complete! Found 8 queries across 2 regions

[Interactive UI displays here for selection]

Imported scheduled query 'Sales Daily Summary' as asset 'sales_daily_summary'
Imported scheduled query 'Customer Analytics' as asset 'customer_analytics'

Successfully imported 2 scheduled queries into pipeline './my-pipeline'
```

### Error Handling

Common issues and solutions:

- **API not enabled**: Enable the BigQuery Data Transfer API in your GCP project
- **Permission denied**: Ensure your service account has the required permissions
- **No queries found**: Verify scheduled queries exist in the specified project/location
- **Connection not BigQuery**: The connection must be a BigQuery type connection

### Best Practices

1. **Review Queries**: Use the preview pane to review query content before importing
2. **Selective Import**: Only import queries that fit your pipeline's purpose
3. **Post-Import Review**: Review and customize the generated SQL files as needed
4. **Naming Conflicts**: The command will skip queries if an asset with the same name already exists

### Notes

- The command searches all common BigQuery regions by default for comprehensive discovery
- Query search is performed in parallel for faster results across regions
- The interactive UI provides a smooth experience for reviewing and selecting queries
- Imported queries maintain their original SQL without modification

### Related Commands
- [`bruin run`](./run.md) - Execute the imported query assets
- [`bruin validate`](./validate.md) - Validate the imported pipeline structure
- [`bruin import database`](#import-database) - Import database tables as assets
---

## `import tableau`

Import Tableau dashboards, workbooks, and data sources as Bruin assets with automatic dependency detection and project hierarchy replication.

```bash
bruin import tableau [FLAGS] [pipeline path]
```

### Overview

The Tableau import command enables you to:

- Connect to Tableau Cloud/Server using Personal Access Tokens
- Automatically discover and import dashboards, workbooks, and data sources
- Replicate Tableau's project folder structure in your Bruin pipeline
- Create dependency relationships between dashboards and data sources
- Preserve metadata including project hierarchy and workbook associations

### Arguments

| Argument | Description |
|----------|-------------|
| `pipeline path` | **Required.** Path to the directory where the pipeline and imported Tableau assets will be created. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--connection`, `-c` | string | - | **Required.** Name of the Tableau connection to use as defined in `.bruin.yml` |
| `--environment`, `--env` | string | - | Target environment name as defined in `.bruin.yml` |
| `--config-file` | string | - | Path to the `.bruin.yml` file. Can also be set via `BRUIN_CONFIG_FILE` environment variable |

### How It Works

1. **Authentication**: Uses your Tableau Personal Access Token from `.bruin.yml`
2. **Discovery Phase**: 
   - Fetches all projects to understand the hierarchy
   - Retrieves workbooks and their associated views (dashboards/worksheets)
   - Identifies data source connections
3. **Parallel Processing**: Uses up to 10 concurrent workers to fetch workbook details efficiently
4. **Asset Generation**: 
   - Creates folder structure matching Tableau projects
   - Generates YAML assets for dashboards, workbooks, and data sources
   - Establishes dependency relationships
5. **Name Sanitization**: Folder and file names are automatically sanitized (spaces replaced with underscores, special characters removed)

### Examples

#### Basic Import

Import all Tableau assets:

```bash
bruin import tableau ./my-pipeline --connection tableau-prod
```

#### Environment-Specific Import

Import using a specific environment configuration:

```bash
bruin import tableau ./my-pipeline --connection tableau-cloud --env production
```

### Generated Asset Structure

The import command creates a hierarchical folder structure that mirrors your Tableau project organization:

```
assets/
└── tableau/
    ├── data_sources/
    │   ├── sales_datasource.asset.yml
    │   └── customer_datasource.asset.yml
    └── dashboards/
        ├── marketing_analytics/           # Project folder
        │   ├── campaign_dashboard.asset.yml
        │   └── roi_tracker.asset.yml
        └── sales_reporting/               # Project folder
            ├── daily_sales.asset.yml
            └── quarterly_review.asset.yml
```

#### Dashboard Asset Example

```yaml
type: tableau.dashboard
description: 'Tableau dashboard: Sales Overview [Project: Sales Reporting]'
meta:
  project_hierarchy: Sales Reporting
  workbook_id: 57e851bb-c413-4f24-8125-e14ad9d8c07b
  workbook_url: https://tableau.company.com/#/site/analytics/workbooks/SalesWorkbook

depends:
  - tableau.data_sources.sales_datasource
  - tableau.data_sources.customer_datasource

owner: analyst@company.com

parameters:
  dashboard_id: 2447c61b-8426-4767-a6dd-88292425551b
  dashboard_name: Sales Overview
  refresh: "false"
  url: https://tableau.company.com/#/site/analytics/views/SalesWorkbook/SalesOverview
```

#### Data Source Asset Example

```yaml
type: tableau.datasource
description: 'Tableau data source: Sales Database'

owner: data-team@company.com

parameters:
  datasource_id: 8a9b10c2-3d4e-5f67-8901-234567890abc
  datasource_name: Sales Database
  refresh: "false"
```

### Key Features

#### Project Hierarchy Preservation

The importer maintains your Tableau project structure, creating nested folders that match your Tableau organization. Project names are sanitized to be filesystem-friendly while maintaining recognizability.

#### Automatic Dependency Detection

The importer automatically:
- Identifies which data sources each dashboard depends on
- Creates proper dependency chains using full asset paths
- Ensures correct execution order in your pipeline

#### Metadata Preservation

Each asset includes metadata about:
- Parent workbook name and URL
- Project hierarchy
- Original Tableau IDs for programmatic access
- Owner information from Tableau

#### Name Extraction

Asset names are derived from file paths rather than explicitly defined, allowing Bruin to handle naming automatically. This prevents naming conflicts and ensures consistency.

### Prerequisites

1. **Tableau Connection**: A Tableau connection must be configured in `.bruin.yml` with:
   - Personal Access Token (PAT) for authentication
   - Site ID for your Tableau instance
   - Base URL for Tableau Cloud/Server
2. **Permissions**: Your PAT must have permissions to:
   - View workbooks and views
   - Access data source metadata
   - List projects
3. **Pipeline Directory**: The target pipeline path must exist

### Configuration Example

In your `.bruin.yml`:

```yaml
connections:
  tableau-prod:
    type: tableau
    base_url: https://prod-useast-b.online.tableau.com
    site_id: internetsociety
    personal_access_token: ${TABLEAU_PAT_TOKEN}
    personal_access_token_name: ${TABLEAU_PAT_NAME}
```

### Output

The command provides progress updates during import:

```
Fetching Tableau workbooks and dashboards...
Found 15 workbooks with 47 dashboards
Fetching details for all workbooks (using 10 parallel workers)...
Processing workbook: Marketing Analytics (3/15)
Creating assets in: ./my-pipeline/assets/tableau/
Successfully imported:
- 47 dashboards
- 12 data sources
- Created 5 project folders
```

### Error Handling

The import process is resilient to partial failures:

- **Missing Views**: If views can't be fetched for a workbook, it continues with other workbooks
- **API Errors**: Individual API failures are logged but don't stop the entire import
- **Name Conflicts**: Sanitization ensures valid filesystem names; extremely similar names may require manual adjustment

Common issues and solutions:

- **Authentication Failed**: Verify your PAT is valid and not expired
- **Site ID Missing**: Ensure site_id is configured in your connection
- **No Workbooks Found**: Check that your PAT has appropriate permissions
- **API Version Issues**: The importer uses Tableau API v3.21 by default

### Best Practices

1. **Review Generated Assets**: After import, review the generated structure and customize as needed
2. **Folder Organization**: The automatic folder structure can be reorganized if needed
3. **Dependency Management**: Review dependencies to ensure they match your expectations
4. **Incremental Updates**: Re-running import will overwrite existing assets; consider version control
5. **Name Validation**: Run `bruin validate` after import to ensure all asset names are valid

### Notes

- Dashboard and worksheet assets are created with `refresh: "false"` by default (no-op assets)
- Data source assets can be modified to enable refresh by setting `refresh: "true"`
- The import process fetches data in parallel for improved performance
- Extremely long or complex project hierarchies may be truncated for filesystem compatibility

### Related Commands
- [`bruin run`](./run.md) - Execute the imported Tableau assets
- [`bruin validate`](./validate.md) - Validate the imported pipeline structure
- [Tableau Asset Documentation](../assets/tableau-refresh.md) - Learn about Tableau asset types and refresh capabilities
