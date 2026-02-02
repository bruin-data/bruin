# Cloud MCP

This guide shows how to connect **Cursor**, **Claude Code** and **Codex** to the **Bruin Asset MCP server** so your AI assistant can securely call Bruin Cloud tools (for example: listing pipelines, inspecting runs, or triggering actions) directly from chat.

## Setup

The Asset MCP server is exposed at:

 `https://cloud.getbruin.com/mcp/api/asset`

Create API token with MCP permission:

1. Log in to the Bruin Cloud.
2. Go to **Team settings** → **API Tokens**.
3. Create a new token and grant it (`mcp:token`) permission.
4. Copy the **plain-text token** once; it is not shown again.

## Cursor

Go to Settings > Cursor Settings > Tools & MCP > New MCP Server.

Edit the **`.cursor/mcp.json`** file and add your token.

```json
{
  "mcpServers": {
    "asset": {
      "type": "streamable-http",
      "url": "https://cloud.getbruin.com/mcp/api/asset",
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
claude mcp add --transport http asset_server https://cloud.getbruin.com/mcp/api/asset --header "Authorization: Bearer YOUR_TOKEN_HERE"
```


```bash
# List configured MCP servers
claude mcp list

# Details for one server
claude mcp get asset_server

# Remove a server
claude mcp remove asset_server
```

Inside Claude Code, type **`/mcp`** to see MCP status and connected servers.


## Codex CLI

Edit your Codex configuration file at `~/.codex/config.toml`:
```toml
[mcp_servers.asset_server]
url = "https://cloud.getbruin.com/mcp/api/asset"
http_headers = { Authorization = "Bearer YOUR_TOKEN_HERE" }
enabled = true
```

 Restart Codex CLI to load the new configuration.


---
### Using the tools

Once the Asset server is connected, you can ask in natural language, for example:

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


## Troubleshooting

- **401 Unauthorized:** Missing or invalid Bearer token. Check that the token is correct and not expired.
- **403 Forbidden / “Insufficient token permissions”:** Token does not have the `mcp:token` ability. Create a new token with MCP permission.
- **Cursor, tools not showing:** Ensure `.cursor/mcp.json` is valid JSON and restart Cursor.
- **Claude Code, server not found:** Run `claude mcp list` to confirm the server is configured; use `claude mcp get asset_server` to check its URL and headers.
- **Codex CLI, tools not available:** Ensure `~/.codex/config.toml` is valid toml and restart Codex CLI.
