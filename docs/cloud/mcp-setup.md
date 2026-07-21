# Cloud MCP

This guide shows how to connect **Claude** (Desktop & Web), **Cursor**, **Claude Code** and **Codex** to the **Bruin Cloud MCP** so your AI assistant can securely call Bruin Cloud tools (for example: listing pipelines, inspecting runs, or triggering actions) directly from chat.

Bruin Cloud MCP is optional. The Bruin CLI already supports Cloud operations through [`bruin cloud`](/commands/cloud), which is usually the right interface when an assistant has shell access and a configured API key or `.bruin.yml`. Use Bruin Cloud MCP when your assistant is set up for MCP tool calls, when you want structured Cloud tools available directly in chat, or when the assistant should not shell out to the local CLI.

## Setup

The Bruin Cloud MCP is exposed at:

 `https://cloud.getbruin.com/mcp`

Create an API token with the `mcp:token` ability:

1. Log in to Bruin Cloud.
2. Go to **Team Settings → Team Access** and find the **API Tokens** panel.
3. Create a new token and grant it the `mcp:token` ability.
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

Claude returns to the connectors list showing **Connected**, and the Bruin Cloud tools become available in chat. The connection is scoped to the team you selected and carries the same `mcp:token` ability as a static token; access tokens are short-lived and refresh automatically.


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

### Available tools

Read-only tools only query data; write tools change state (trigger runs, mark statuses, manage connections) and are annotated as destructive so your assistant asks before calling them.

| Tool | Access | Purpose |
| --- | --- | --- |
| `pipeline-list` | read | List pipelines, or fetch one pipeline's details. |
| `pipeline-run-list` | read | List pipeline runs, or fetch one run's details. |
| `asset-list` | read | List assets, or fetch one asset's details and dependencies. |
| `asset-instance-list` | read | List asset instances (per-asset status) within a run. |
| `asset-instance-logs` | read | Fetch execution logs for a step of an asset instance. |
| `asset-runs` | read | Show run history for an asset. |
| `connection-list` | read | List connections (metadata only — never secret values). |
| `validation-error-list` | read | List pipeline validation errors. |
| `pipeline-trigger` | write | Trigger a new pipeline run. |
| `pipeline-rerun` | write | Rerun an existing pipeline run. |
| `pipeline-toggle` | write | Enable or disable (pause/resume) a pipeline. |
| `asset-rerun` | write | Rerun a single asset. |
| `backfill-trigger` | write | Trigger a backfill over a date range. |
| `mark-pipeline-run` | write | Mark a pipeline run's status. |
| `mark-asset-run` | write | Mark an asset run's status. |
| `mark-external-dependency` | write | Mark an external dependency's status. |
| `connection-create` | write | Create a connection. |
| `connection-delete` | write | Delete a connection. |


## Troubleshooting

- **401 Unauthorized:** Missing or invalid Bearer token. Check that the token is correct and not expired.
- **403 Forbidden / “Insufficient token permissions”:** Token does not have the `mcp:token` ability. Create a new token with MCP permission.
- **Cursor, tools not showing:** Ensure `.cursor/mcp.json` is valid JSON and restart Cursor.
- **Claude Code, server not found:** Run `claude mcp list` to confirm the server is configured; use `claude mcp get bruin_cloud` to check its URL and headers.
- **Codex CLI, tools not available:** Ensure `~/.codex/config.toml` is valid toml and restart Codex CLI.
- **Claude custom connector, stuck or "disconnected":** Re-run **Add custom connector**, sign in to Bruin Cloud, and make sure you approve access for a team you belong to. Leave the OAuth Client ID/Secret fields empty.

## Related

- [Pipelines](/cloud/pipelines) for the operations the MCP can drive (runs, backfills, status).
- [Connections](/cloud/connections) for the connections the MCP can list and create.
- [Bruin MCP (local)](/getting-started/bruin-mcp) for the local-CLI MCP, separate from the cloud-hosted one.
- [`bruin cloud`](/commands/cloud) — the CLI command that talks to Bruin Cloud using the same kind of API token.
