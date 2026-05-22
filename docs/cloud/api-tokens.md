# API Tokens

API tokens let you talk to Bruin Cloud programmatically — from CI pipelines, custom scripts, the [Cloud MCP](/cloud/mcp-setup), or any external system that needs to read or trigger things in your team. Each token carries a set of scoped abilities, so you can hand a CI job a token that can only trigger runs without giving it permission to delete pipelines.

## Where to manage tokens

Tokens are managed in **Team Settings → Team Access → API Tokens** (or directly at `/user/api-tokens`).

The panel lists every active token with its name, abilities, and last-used timestamp. Three actions per row: **Permissions** (edit the ability set), **Delete** (revoke), and the value itself (only shown once at creation — see below).

## Create a token

1. Click **Create Token**.
2. Give it a **name** (e.g. `ci-trigger`, `mcp-cursor`, `prod-monitoring`).
3. Pick the **abilities** the token should have. The defaults are read-only; tick boxes for write actions as needed.
4. Click **Create**.

The plain-text token appears once, in a modal. Copy it now — Bruin Cloud doesn't store the plaintext and won't show it again. If you lose it, delete the token and create a new one.

The token is used as a bearer token:

```text
Authorization: Bearer <token>
```

## Available abilities

Abilities are grouped by what they unlock. Pick the smallest set the consumer actually needs.

**Pipelines**

- `pipeline:list` — list pipelines.
- `pipeline:show` — read a pipeline's definition.
- `pipeline:update` — change pipeline settings (enable/disable, schedule overrides).
- `pipeline:delete` — delete a pipeline.
- `pipeline:health:show` — read pipeline health metrics (enterprise).
- `pipeline:cost:show` — read cost data (enterprise).
- `pipeline:run:list` — list runs.
- `pipeline:run:trigger` — trigger new runs and backfills.
- `pipeline:asset:list` — list assets within a pipeline.
- `pipeline:asset:show` — read an asset.

**Connections**

- `connection:list`, `connection:create`, `connection:delete`.

**Team**

- `team:show`, `team:update`, `team:delete`, `team:create`.

**Other**

- `audit-log:list` — read audit logs.
- `glossary:list`, `glossary:entity:list`, `glossary:entity:show`.
- `agent:list`, `agent:thread:list`, `agent:message:send`, `agent:message:status`, `agent:manage`.
- `dashboard:manage`.
- `notification:send`.
- `mcp:token` — required for the [Cloud MCP](/cloud/mcp-setup).

## Edit abilities

Click **Permissions** on any token. Tick or untick abilities in the modal, then **Save**. The token value itself doesn't change — only what it can do.

## Revoke a token

Click **Delete** on the token row, then confirm. The token stops working immediately. Bruin clears `last_used_at` and removes it from the list.

If you suspect a token has leaked, revoke first and ask questions later. A new token takes seconds to create.

## Use cases

- **Cloud MCP** — `mcp:token` is the only required scope. See [Cloud MCP](/cloud/mcp-setup) for the connection setup in Cursor, Claude Code, and Codex.
- **CI / CD** — give your pipeline a token with `pipeline:run:trigger` (and `pipeline:run:list` if you poll for status). Don't grant `pipeline:delete`.
- **External monitoring** — `pipeline:run:list`, `pipeline:asset:show`, and `audit-log:list` are usually enough.
- **Read-only dashboards** — `pipeline:list`, `pipeline:show`, `pipeline:asset:list` cover most warehouse-side reporting.

## Troubleshooting

**`401 Unauthorized`** — the token is missing, malformed, or revoked. Double-check the `Authorization: Bearer …` header and that the token still exists.

**`403 Forbidden` / *Insufficient token permissions*** — the token doesn't have the ability the endpoint needs. Edit the token's abilities and retry.

**Token doesn't appear in `last_used_at`** — `last_used_at` updates asynchronously. Give it a minute, then refresh.

## Related

- [Cloud MCP](/cloud/mcp-setup) — wire a token into Cursor, Claude Code, or Codex.
- [Team Settings](/cloud/team-settings) — where the API Tokens panel lives.
- [Audit Logs](/cloud/audit-logs) — token creation, updates, and deletion are all logged.
