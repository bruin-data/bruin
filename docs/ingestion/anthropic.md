# Anthropic

[Anthropic](https://www.anthropic.com/) is an AI safety company that builds Claude, a family of large language models. This source enables you to extract comprehensive data from the Anthropic Admin API, including Claude Code usage metrics, API usage reports, cost data, and organization management information.

To set up an Anthropic connection, you need to have an Admin API key that has the necessary permissions for the resources you want to access.

:::warning Admin API Key Required
This source requires an **Admin API key** which is different from standard API keys. Only organization members with the admin role can provision Admin API keys through the [Anthropic Console](https://console.anthropic.com/settings/admin-keys).

The Admin API is unavailable for individual accounts. To use this source, you must have an organization set up in Console → Settings → Organization.
:::

## Set up a connection

Anthropic connections are defined using the following properties:

- `name`: The name to identify this connection
- `api_key`: Your Anthropic Admin API key (must start with `sk-ant-admin...`) (required)

:::code-group
```yaml [connections.yml]
connections:
  anthropic:
    - name: "my_anthropic"
      api_key: "sk-ant-admin-..."
```
:::

You can also use environment variables in your connections.yml by using the `&#123;&#123; env_var("ENV_VAR_NAME") &#125;&#125;` syntax.

For example:
```yaml
connections:
  anthropic:
    - name: "my_anthropic"
      api_key: "&#123;&#123; env_var('ANTHROPIC_API_KEY') &#125;&#125;"
```

## Supported Data Assets

Anthropic assets will be ingested to your data warehouse as defined in the `destination` table.

| Asset                  | Table Name             | Incremental Key | Description                                                                                       |
|------------------------|------------------------|-----------------|---------------------------------------------------------------------------------------------------|
| Claude Code Usage      | `claude_code_usage`    | date            | Daily aggregated usage metrics for Claude Code users in your organization                        |
| Usage Report           | `usage_report`         | replace         | Detailed token usage metrics from the Messages API, aggregated by time bucket                   |
| Cost Report            | `cost_report`          | replace         | Aggregated cost data broken down by workspace and cost description                               |
| Organization           | `organization`         | replace         | Information about your Anthropic organization                                                     |
| Workspaces             | `workspaces`           | replace         | All workspaces in your organization                                                               |
| API Keys               | `api_keys`             | replace         | All API keys in your organization                                                                 |
| Invites                | `invites`              | replace         | All pending organization invites                                                                  |
| Users                  | `users`                | replace         | All users in your organization                                                                    |
| Workspace Members      | `workspace_members`    | replace         | Workspace membership information                                                                  |

## Asset-Specific Configuration

### Claude Code Usage
The `claude_code_usage` table supports incremental loading based on the `date` field. This data helps you analyze developer productivity and monitor Claude Code adoption.

### Usage Report
The `usage_report` table contains detailed token usage metrics from the Messages API, aggregated by time bucket, workspace, API key, model, and service tier. Supports date range filtering.

### Cost Report
The `cost_report` table contains aggregated cost data broken down by workspace and cost description. Supports date range filtering.

### Organization Data
Tables like `organization`, `workspaces`, `api_keys`, `invites`, `users`, and `workspace_members` use full refresh mode as they represent current state data.

## Notes

- **Authentication**: The Anthropic Admin API uses Bearer token authentication. Make sure your API key has the necessary permissions for the resources you want to access.
- **Incremental Loading**: Only `claude_code_usage` supports incremental loading. Other tables use full refresh (replace strategy).
- **Date Filtering**: `usage_report` and `cost_report` support date range filtering with `--interval-start` and `--interval-end`.
- **Permissions**: Some endpoints may return a 403 Forbidden error if your API key doesn't have the required permissions.
- **Data Freshness**: Claude Code analytics data typically appears within 1 hour of user activity completion. The API provides daily aggregated metrics only.
- **Rate Limits**: The Anthropic Admin API has rate limits in place. The source handles pagination automatically and respects these limits.
- **Scope**: This source only tracks Claude Code usage on the Anthropic API (1st party). Usage on Amazon Bedrock, Google Vertex AI, or other third-party platforms is not included.
- **Timezone**: All dates and timestamps are in UTC.
- **Organization Access**: The source requires organization-level access (not available for individual accounts).

## Example pipeline

Here's an example of an asset ingesting data from Anthropic to a Snowflake table:

```sql
/* @bruin

name: anthropic.claude_code_usage
type: ingestr

@ingestr
source_connection: anthropic
source_table: claude_code_usage

destination: snowflake

@end
*/

select * from {{ source() }}
```

The ingestr operator will automatically pull data from the `claude_code_usage` endpoint of your Anthropic Admin API and load it into your target data warehouse.