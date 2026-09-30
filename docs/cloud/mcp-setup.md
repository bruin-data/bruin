# Cloud MCP

This guide shows how to connect **Claude** (Desktop & Web), **Cursor**, **Claude Code** and **Codex** to the **Bruin Cloud MCP** so your AI assistant can securely call Bruin Cloud tools (for example: listing pipelines, inspecting runs, or triggering actions) directly from chat.

Bruin Cloud MCP is optional. The Bruin CLI already supports Cloud operations through [`bruin cloud`](/commands/cloud), which is usually the right interface when an assistant has shell access and a configured API key or `.bruin.yml`. Use Bruin Cloud MCP when your assistant is set up for MCP tool calls, when you want structured Cloud tools available directly in chat, or when the assistant should not shell out to the local CLI.

## Setup

The Bruin Cloud MCP is exposed at:

 `https://cloud.getbruin.com/mcp`

For an individual connection, create a personal access token with only the permissions required by the MCP tools you plan to use. Personal tokens are also limited by your current role on the selected team. Use a team token only for a shared integration that should act as the team.

1. Log in to Bruin Cloud.
2. Open the user menu and select **Access Tokens**. For a team token, use **Team Settings → API Tokens** instead.
3. Create a token, choose **Custom**, and select the permissions required by the tools in the [table below](#available-tools).
4. Copy the **plain-text token** once; it is not shown again.

See [API Tokens](/cloud/api-tokens) for the full token-management walkthrough.

## Cursor

Go to Settings > Cursor Settings > Tools & MCP > New MCP Server.

Edit the **`.cursor/mcp.json`** file and add your token.

```json
{
  "mcpServers": {
    "bruin_cloud": {
      "type": "streamable-http",
      "url": "https://cloud.getbruin.com/mcp",
      "headers": {
        "Authorization": "Bearer YOUR_TOKEN_HERE"
      }
    }
  }
}
```

Restart Cursor (or reload the window) so it picks up the MCP config.

## Claude Code

From a terminal (any directory):

```bash
claude mcp add --transport http bruin_cloud https://cloud.getbruin.com/mcp --header "Authorization: Bearer YOUR_TOKEN_HERE"
```

```bash
# List configured MCP servers
claude mcp list

# Details for one server
claude mcp get bruin_cloud

# Remove a server
claude mcp remove bruin_cloud
```

Inside Claude Code, type **`/mcp`** to see MCP status and connected servers.

## Claude (Desktop & Web)

The Claude Desktop and Web apps connect to Bruin Cloud as a **custom connector** over OAuth — you sign in to Bruin Cloud and approve access, so there is **no API token to create or paste** (you can skip the token step above).

1. In Claude, open **Settings → Connectors → Add custom connector**.
2. For **Remote MCP server URL**, enter:

   `https://cloud.getbruin.com/mcp`

3. Leave **Advanced settings** (OAuth Client ID / Secret) empty — Claude registers itself automatically.
4. Click **Add**. Claude opens a Bruin Cloud sign-in page.
5. Sign in, choose which **team** to connect, and approve.

Claude returns to the connectors list showing **Connected**, and the Bruin Cloud tools become available in chat. The connection is scoped to the team you selected and limited by your current permissions on that team; access tokens are short-lived and refresh automatically.

## Codex CLI

Edit your Codex configuration file at `~/.codex/config.toml`:

```toml
[mcp_servers.bruin_cloud]
url = "https://cloud.getbruin.com/mcp"
http_headers = { Authorization = "Bearer YOUR_TOKEN_HERE" }
enabled = true
```

Restart Codex CLI to load the new configuration.

---

### Using the tools

Once the Bruin Cloud MCP server is connected, you can ask in natural language, for example:

- “List all pipelines for my team.”
- “Show pipeline runs status failed.”
- “Get asset instances for pipeline Y, run_id Z.”
- “Mark pipeline X run Y as success.”
- "Trigger a new run for pipeline X with start/end dates."
- "Show me the latest runs for pipeline X, sorted by start time."
- "List all assets for pipeline X and show their current status."
- "For pipeline X, show asset instances that failed in the last 24 hours."
- "Get the logs for asset Y from run Z."
- "Show me validation errors."
- "Cancel the currently running instance of pipeline X."
- "Mark external dependencies in run id X as success."
- "List all connections."
- "Create a new generic connection called my-secret with value X."
- "Delete the connection named my-secret."
- "What was my BigQuery warehouse spend last month?"
- "Which pipelines cost the most this month? Break it down by pipeline."
- "Break down warehouse cost by user for pipeline X."
- "Show the daily cost trend for pipeline X over the last 2 weeks."
- "What are the most expensive assets in pipeline X?"

### Available tools

Each tool checks its concrete permission. A personal token must contain that permission, and your current team role must grant it. Read-only tools only query data; write tools change state and compatible assistants may require confirmation before calling them.

| Tool | Access | Required permission | Purpose |
| --- | --- | --- | --- |
| `pipeline-list-tool` | read | `pipeline:list` | List pipelines, or fetch one pipeline's details. |
| `pipeline-run-list-tool` | read | `pipeline:run:list` | List pipeline runs, or fetch one run's details. |
| `asset-list-tool` | read | `pipeline:asset:show` | List assets, or fetch one asset's details and dependencies. |
| `asset-health-tool` | read | `pipeline:asset:show` | Get asset health information. |
| `asset-instance-list-tool` | read | `pipeline:run:asset-instance:show` | List asset instances within a run. |
| `asset-instance-logs-tool` | read | `pipeline:run:asset-instance:show` | Fetch execution logs for an asset instance. |
| `asset-runs-tool` | read | `pipeline:run:list` | Show run history for an asset. |
| `backfill-list-tool` | read | `pipeline:run:list` | List backfills and their runs. |
| `run-tags-tool` | read | `pipeline:run:show` | List tags for a pipeline run. |
| `validation-error-list-tool` | read | `pipeline:show` | List pipeline validation errors. |
| `connection-list-tool` | read | `connection:list` | List connection metadata, never secret values. |
| `connection-types-tool` | read | `connection:list` | List supported connection types and their fields. |
| `cost-explorer-schema-tool` | read | `pipeline:cost:show` | Describe Cost Explorer dimensions and metrics. |
| `cost-explorer-tool` | read | `pipeline:cost:show` | Query warehouse cost data. |
| `notification-rule-list-tool` | read | `notification-rule:list` | List notification rules. |
| `notification-rule-schema-tool` | read | `notification-rule:list` | Describe the notification-rule schema. |
| `pipeline-trigger-tool` | write | `pipeline:run:trigger` | Trigger a pipeline run. |
| `pipeline-rerun-tool` | write | `pipeline:run:re-run` | Rerun an existing pipeline run. |
| `pipeline-toggle-tool` | write | `pipeline:update` | Enable or disable a pipeline. |
| `asset-rerun-tool` | write | `pipeline:run:asset-instance:re-run` | Rerun an asset. |
| `backfill-trigger-tool` | write | `pipeline:run:trigger` | Trigger a backfill over a date range. |
| `backfill-delete-tool` | write | `pipeline:run:delete` | Delete a backfill. |
| `backfill-mark-tool` | write | `pipeline:run:mark-as` | Mark backfill runs with a status. |
| `backfill-skip-assets-tool` | write | `pipeline:run:mark-as` | Skip selected assets in a backfill. |
| `mark-pipeline-run-tool` | write | `pipeline:run:mark-as` | Mark a pipeline run's status. |
| `mark-asset-run-tool` | write | `pipeline:run:asset-instance:mark-as` | Mark an asset run's status. |
| `mark-external-dependency-tool` | write | `pipeline:run:asset-instance:mark-as` | Mark an external dependency's status. |
| `asset-health-manual-entry-create-tool` | write | `pipeline:run:asset-instance:mark-as` | Create a manual asset-health entry. |
| `asset-health-manual-entry-delete-tool` | write | `pipeline:run:asset-instance:mark-as` | Delete a manual asset-health entry. |
| `run-note-tool` | write | `pipeline:run:update` | Set or clear a pipeline-run note. |
| `run-delete-tool` | write | `pipeline:run:delete` | Delete a pipeline run. |
| `connection-create-tool` | write | `connection:create` | Create a connection. |
| `connection-delete-tool` | write | `connection:delete` | Delete a connection. |
| `notification-rule-create-tool` | write | `notification-rule:manage` | Create a notification rule. |
| `notification-rule-update-tool` | write | `notification-rule:manage` | Update a notification rule. |
| `notification-rule-delete-tool` | write | `notification-rule:delete` | Delete a notification rule. |

## Troubleshooting

- **401 Unauthorized:** Missing or invalid Bearer token. Check that the token is correct and not expired.
- **“Insufficient token permissions”:** The token is missing the concrete permission named in the error. Edit the token and grant that permission.
- **“Your role does not grant”:** A personal token cannot exceed your current role on the selected team. Ask a team admin to update your role or use a tool your role permits.
- **Cursor, tools not showing:** Ensure `.cursor/mcp.json` is valid JSON and restart Cursor.
- **Claude Code, server not found:** Run `claude mcp list` to confirm the server is configured; use `claude mcp get bruin_cloud` to check its URL and headers.
- **Codex CLI, tools not available:** Ensure `~/.codex/config.toml` is valid toml and restart Codex CLI.
- **Claude custom connector, stuck or "disconnected":** Re-run **Add custom connector**, sign in to Bruin Cloud, and make sure you approve access for a team you belong to. Leave the OAuth Client ID/Secret fields empty.

## Related

- [Pipelines](/cloud/pipelines) for the operations the MCP can drive (runs, backfills, status).
- [Connections](/cloud/connections) for the connections the MCP can list and create.
- [Insights](/cloud/insights#cost-explorer) for the Cost Explorer data the MCP can query (warehouse spend by pipeline, asset, or user).
- [Bruin MCP (local)](/getting-started/bruin-mcp) for the local-CLI MCP, separate from the cloud-hosted one.
- [`bruin cloud`](/commands/cloud) — the CLI command that talks to Bruin Cloud using the same kind of API token.
