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

You can also use environment variables in your connections.yml by using the `${VAR_NAME}` syntax.

For example:

```yaml
connections:
  anthropic:
    - name: "my_anthropic"
      api_key: ${ANTHROPIC_API_KEY}
```

## Available Source Tables

Anthropic assets will be ingested to your data warehouse as defined in the `destination` table.


| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| `claude_code_usage` | date, actor_type, actor_id, terminal_type | date | append | Daily aggregated usage metrics for Claude Code users in your organization |
| `usage_report` | bucket, api_key_id, workspace_id, model, service_tier | bucket | replace | API usage and latency metrics from the Messages API |
| `cost_report` | bucket, workspace_id, description | bucket | replace | Cost breakdown by workspace and cost description |
| `organization` | - | - | replace | Organization information |
| `workspaces` | id | - | replace | Workspace list |
| `api_keys` | id | - | replace | API key management |
| `invites` | id | - | replace | Pending invitations |
| `users` | id | - | replace | User list |
| `workspace_members` | workspace_id, user_id | - | replace | Workspace memberships |

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
