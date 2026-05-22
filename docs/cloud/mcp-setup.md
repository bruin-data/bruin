# Cloud MCP

This guide shows how to connect **Cursor**, **Claude Code** and **Codex** to the **Bruin Cloud MCP** so your AI assistant can securely call Bruin Cloud tools (for example: listing pipelines, inspecting runs, or triggering actions) directly from chat.

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


## Troubleshooting

- **401 Unauthorized:** Missing or invalid Bearer token. Check that the token is correct and not expired.
- **403 Forbidden / “Insufficient token permissions”:** Token does not have the `mcp:token` ability. Create a new token with MCP permission.
- **Cursor, tools not showing:** Ensure `.cursor/mcp.json` is valid JSON and restart Cursor.
- **Claude Code, server not found:** Run `claude mcp list` to confirm the server is configured; use `claude mcp get bruin_cloud` to check its URL and headers.
- **Codex CLI, tools not available:** Ensure `~/.codex/config.toml` is valid toml and restart Codex CLI.

## Related

- [Pipelines](/cloud/pipelines) for the operations the MCP can drive (runs, backfills, status).
- [Connections](/cloud/connections) for the connections the MCP can list and create.
- [Bruin MCP (local)](/getting-started/bruin-mcp) for the local-CLI MCP, separate from the cloud-hosted one.
- [`bruin cloud`](/commands/cloud) — the CLI command that talks to Bruin Cloud using the same kind of API token.
