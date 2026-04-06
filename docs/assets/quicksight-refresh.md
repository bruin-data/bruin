# QuickSight Assets

Bruin supports integrating Amazon QuickSight assets into your data pipelines. You can represent QuickSight datasets and dashboards as assets, and trigger SPICE dataset refreshes directly from your pipeline.

## Connection

In order to set up a QuickSight connection, you need to add a configuration item to `connections` in the `.bruin.yml` file.

### AWS Credentials

```yaml
connections:
  quicksight:
    - name: "connection_name"
      aws_access_key_id: "your-access-key-id"
      aws_secret_access_key: "your-secret-access-key"
      aws_region: "us-east-1"
      aws_account_id: "123456789012"
```

You can also provide a session token for temporary credentials:

```yaml
connections:
  quicksight:
    - name: "connection_name"
      aws_access_key_id: "your-access-key-id"
      aws_secret_access_key: "your-secret-access-key"
      aws_session_token: "your-session-token"
      aws_region: "us-east-1"
      aws_account_id: "123456789012"
```

**Parameters:**

- `name`: A unique name for this connection
- `aws_access_key_id`: Your AWS access key ID
- `aws_secret_access_key`: Your AWS secret access key
- `aws_session_token`: AWS session token for temporary credentials (optional)
- `aws_region`: The AWS region where your QuickSight resources are located
- `aws_account_id`: Your AWS account ID

## Supported QuickSight Asset Types

- `quicksight.dataset`   — Represents a QuickSight SPICE dataset (can be refreshed)
- `quicksight.dashboard` — Represents a QuickSight dashboard (no-op, for lineage/clarity)
- `quicksight`           — Generic QuickSight asset (no-op, for lineage/clarity)

## Refreshing QuickSight Datasets

To trigger a SPICE dataset refresh, set the `refresh` parameter to `"true"` on a `quicksight.dataset` asset. You must provide the `dataset_id` parameter for the refresh to work.

If `refresh` is not set or is `"false"`, the asset is a no-op and can be used for documentation or lineage only.

### Refresh Mode: Incremental vs Full

For `quicksight.dataset` assets, Bruin supports controlling the refresh mode:

- `incremental` (optional): defaults to `"true"`. When `"true"`, Bruin requests an incremental SPICE refresh.
- `refresh_timeout_minutes` (optional): defaults to `"60"`. Controls how long Bruin waits for the SPICE ingestion to complete before timing out.
- Pipeline run flag `--full-refresh`: when enabled, Bruin forces a full refresh for QuickSight datasets.

If an incremental refresh is requested but the dataset is not configured for incremental updates, Bruin automatically retries with a full refresh.

### Example: Refreshing a Dataset

```yaml
name: quicksight.datasets.dataset_issues_custom_sql
type: quicksight.dataset
description: 'QuickSight dataset: issues_custom_sql'

parameters:
  dataset_id: 23e4f645-9837-4e73-ad15-04ccd4baa400
  dataset_name: issues_custom_sql
  import_mode: SPICE
  refresh: "true"
  custom_sql: "select * from issues where true limit 50"

columns:
  - name: id
    type: STRING
  - name: title
    type: STRING
  - name: description
    type: STRING
```

Or, with explicit incremental and timeout configuration:

```yaml
name: quicksight.datasets.dataset_issues_custom_sql
type: quicksight.dataset
description: 'QuickSight dataset: issues_custom_sql'

parameters:
  dataset_id: 23e4f645-9837-4e73-ad15-04ccd4baa400
  dataset_name: issues_custom_sql
  import_mode: SPICE
  refresh: "true"
  incremental: "true"
  refresh_timeout_minutes: "90"
  custom_sql: "select * from issues where true limit 50"

columns:
  - name: id
    type: STRING
  - name: title
    type: STRING
  - name: description
    type: STRING
```

> **Note:** If the `refresh` parameter is not set or is `"false"`, the `quicksight.dataset` asset is a no-op and can be used for documentation or lineage only.

### Parameters Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `refresh` | string | `"false"` | Set to `"true"` to trigger a SPICE refresh |
| `dataset_id` | string | - | **Required when refresh is true.** The QuickSight dataset ID |
| `dataset_name` | string | - | The dataset name (for documentation/lookup) |
| `incremental` | string | `"true"` | Set to `"false"` for full refresh |
| `refresh_timeout_minutes` | string | `"60"` | Maximum minutes to wait for refresh completion |
| `import_mode` | string | - | The dataset import mode (`SPICE` or `DIRECT_QUERY`) |
| `custom_sql` | string | - | Custom SQL query if the dataset uses a custom SQL physical table |

## QuickSight Dashboards

The `quicksight.dashboard` asset type is a **no-op** in Bruin. It does not trigger any action, but is used to represent dashboards in your pipeline for lineage tracking and documentation purposes.

Dashboard assets can declare dependencies on dataset assets, and include chart-level metadata such as dimensions and metrics for full lineage visibility.

### Example: Dashboard Asset

```yaml
name: quicksight.dashboards.dashboard_test
type: quicksight.dashboard
description: 'QuickSight dashboard: test'

depends:
  - quicksight.datasets.dataset_issues

parameters:
  chart_count: "2"
  charts[0].dimensions: id
  charts[0].metrics: labels
  charts[0].name: BarChart_0
  charts[0].type: BarChart
  charts[1].dimensions: assignee_id
  charts[1].metrics: branch_name
  charts[1].name: BarChart_1
  charts[1].type: BarChart
  dashboard_id: 77f8aa6a-de1c-4cb8-8323-856275b35096
  dashboard_name: test

columns:
  - name: id
    type: STRING
  - name: labels
    type: FLOAT
  - name: assignee_id
    type: STRING
  - name: branch_name
    type: FLOAT
```

### Dashboard Parameters Reference

| Parameter | Type | Description |
|-----------|------|-------------|
| `dashboard_id` | string | The QuickSight dashboard ID |
| `dashboard_name` | string | The dashboard name |
| `chart_count` | string | Total number of charts in the dashboard |
| `charts[N].name` | string | Name of the Nth chart |
| `charts[N].type` | string | Chart type (e.g., `BarChart`, `LineChart`, `PieChart`, `Table`, `PivotTable`, `KPI`) |
| `charts[N].dimensions` | string | Comma-separated list of dimension fields used in the chart |
| `charts[N].metrics` | string | Comma-separated list of metric fields used in the chart |
| `charts[N].dataset` | string | The dataset asset name referenced by the chart |

## Error Handling

- If `refresh: "true"` is set but `dataset_id` is missing, the pipeline will error.
- If an incremental refresh fails because the dataset does not support it, Bruin retries with a full refresh automatically.
- If the refresh times out (default: 60 minutes), the pipeline will error.
- If the SPICE refresh fails or is cancelled, the pipeline will error with the relevant error message.
- Dashboard and generic `quicksight` asset types are always no-ops.

## Importing QuickSight Assets

Bruin provides a powerful import command that automatically discovers and imports your QuickSight datasets and dashboards as Bruin assets. The import command:

- Connects to your AWS QuickSight account
- Discovers all datasets and dashboards
- Presents an interactive terminal UI for asset selection
- Automatically detects upstream warehouse table dependencies for datasets
- Creates chart-level metadata for dashboards including dimensions and metrics

To import your QuickSight assets:

```bash
bruin import quicksight ./my-pipeline --connection quicksight-prod
```

To import all assets without interactive selection:

```bash
bruin import quicksight ./my-pipeline --connection quicksight-prod --all
```

For detailed information about the import process, configuration options, and generated asset structure, see the [QuickSight Import Documentation](../commands/import.md#import-quicksight).
