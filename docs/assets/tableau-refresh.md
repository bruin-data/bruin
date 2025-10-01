# Tableau Assets

Bruin supports integrating Tableau assets into your data pipelines. You can represent Tableau datasources, workbooks, worksheets, and dashboards as assets, and trigger refreshes for datasources and workbooks directly from your pipeline.

## Connection

In order to set up a Tableau connection, you need to add a configuration item to `connections` in the `.bruin.yml` file.

Tableau supports two authentication methods:

### Personal Access Token (Recommended)

```yaml
connections:
  tableau:
    - name: "connection_name"
      host: "your-tableau-server.com"
      site_id: "your-site-id"
      personal_access_token_name: "your-token-name"
      personal_access_token_secret: "your-token-secret"
      api_version: "3.4" # optional, defaults to 3.4
```

### Username and Password

```yaml
connections:
  tableau:
    - name: "connection_name"
      host: "your-tableau-server.com"
      site_id: "your-site-id"
      username: "your-username"
      password: "your-password"
      api_version: "3.4" # optional, defaults to 3.4
```

**Parameters:**
- `name`: A unique name for this connection
- `host`: Your Tableau Server hostname (without https://)
- `site_id`: The site identifier (content URL) for your Tableau site
- `personal_access_token_name`: Personal Access Token name (PAT authentication)
- `personal_access_token_secret`: Personal Access Token secret (PAT authentication)
- `username`: Your Tableau username (username/password authentication)
- `password`: Your Tableau password (username/password authentication)
- `api_version`: Tableau REST API version (optional, defaults to "3.4")

> **Note:** Either Personal Access Token credentials (name and secret) or username/password credentials are required. Personal Access Token authentication is recommended for production environments.

## Supported Tableau Asset Types

- `tableau.datasource` — Represents a Tableau data source (can be refreshed)
- `tableau.workbook`  — Represents a Tableau workbook (can be refreshed)
- `tableau`           — Alias for workbook (can be refreshed)
- `tableau.worksheet` — Represents a Tableau worksheet (no-op, for lineage/clarity)
- `tableau.dashboard` — Represents a Tableau dashboard (no-op, for lineage/clarity)

## Refreshing Tableau Assets

To trigger a refresh, set the `refresh` parameter to `true` on a supported asset type. You must provide either the asset's ID or its name for lookup:

- For data sources: provide `datasource_id` **or** `datasource_name`
- For workbooks: provide `workbook_id` **or** `workbook_name`

> **Note:** If both the ID and the name are provided in parameters, the ID will be prioritized for lookup.

If both ID and name are missing, or the name cannot be resolved, the pipeline will error.

### Example: Refreshing a Data Source

```yaml
name: refresh_sales_datasource
connection: tableau-prod
type: tableau.datasource
parameters:
  refresh: true
  datasource_id: "12345678-1234-1234-1234-123456789012"
```

Or, using a name lookup:

```yaml
name: refresh_sales_datasource_by_name
connection: tableau-prod
type: tableau.datasource
parameters:
  refresh: true
  datasource_name: "my_datasource"
```

> **Note:** If the `refresh` parameter is not set or is `false`, the `tableau.datasource` asset is a no-op and can be used for documentation or lineage only.

### Example: Refreshing a Workbook

```yaml
name: refresh_analytics_workbook
connection: tableau-prod
type: tableau.workbook
parameters:
  refresh: true
  workbook_id: "7741f406-e190-4941-83e0-7be218fb1952"
```

Or, using a name lookup:

```yaml
name: refresh_analytics_workbook_by_name
connection: tableau-prod
type: tableau.workbook
parameters:
  refresh: true
  workbook_name: "my_workbook"
```

> **Note:** If the `refresh` parameter is not set or is `false`, the `tableau.workbook` asset is a no-op and can be used for documentation or lineage only.

### Example: Using the `tableau` Asset Type

The `tableau` asset type is an alias for workbook refresh:

```yaml
name: refresh_tableau_workbook
connection: tableau-prod
type: tableau
parameters:
  refresh: true
  workbook_id: "7741f406-e190-4941-83e0-7be218fb1952"
```

## No-Op Asset Types: worksheet and dashboard

The `tableau.worksheet` and `tableau.dashboard` asset types are **no-ops** in Bruin. They do not trigger any refresh, but can be used to clarify lineage or document Tableau workloads in your pipeline:

```yaml
name: document_tableau_dashboard
connection: tableau-prod
type: tableau.dashboard
parameters:
  # No refresh, just for lineage/clarity
```

## Error Handling

- If `refresh: true` is set but neither ID nor name is provided, the pipeline will error.
- If a name is provided but no matching asset is found, the pipeline will error.
- If `refresh` is not set or is false, no refresh is triggered.
- Worksheet and dashboard asset types are always no-ops.

## API Details

- Refreshes are performed using the Tableau REST API:
  - Data source: `POST /api/{version}/sites/{site-id}/datasources/{datasource-id}/refresh`
  - Workbook:    `POST /api/{version}/sites/{site-id}/workbooks/{workbook-id}/refresh`
- Authentication is handled via the connection config (Personal Access Token recommended).

## Importing Tableau Dashboards

Bruin provides a powerful import command that automatically discovers and imports your Tableau dashboards, workbooks, and data sources as Bruin assets. The import command:

- Connects to your Tableau Cloud/Server using Personal Access Tokens
- Discovers all dashboards, workbooks, and data sources
- Replicates your Tableau project folder structure
- Automatically detects and creates dependency relationships
- Preserves metadata including workbook associations and project hierarchy

To import your Tableau assets:

```bash
bruin import tableau ./my-pipeline --connection tableau-prod
```

This command will create a structured folder hierarchy matching your Tableau projects, with all dashboards and data sources properly organized and linked.

For detailed information about the import process, configuration options, and generated asset structure, see the [Tableau Import Documentation](../commands/import.md#import-tableau).
