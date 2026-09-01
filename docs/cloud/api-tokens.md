# API Tokens

API tokens let you talk to Bruin Cloud programmatically — from the [Bruin CLI](/commands/cloud), CI pipelines, custom scripts, the [Cloud MCP](/cloud/mcp-setup), or any external system that needs to read or trigger things in your team. Each token carries a scoped set of abilities, a set of teams it can act on, and an expiry date, so you can hand a CI job a token that can only trigger runs on one team without giving it permission to delete pipelines everywhere.

A personal access token **acts as you**: its abilities are always clamped to your live role on each team, so a token can never do more than you can. If your role is reduced, the token loses those abilities immediately — no need to revoke and recreate it.

## Where to manage tokens

Personal tokens live on your account page: **Account → Personal Access Tokens**, at `/account/api-tokens`. Any team member can create one — no team-admin permission required. They also appear in **Team Settings → API Tokens** alongside your team's shared tokens.

The panel lists each token with its name, status (active / expiring soon / expired), teams, abilities, and last-used timestamp.

## Create a token

1. Click **Create Token**.
2. Give it a **name** (e.g. `ci-trigger`, `mcp-cursor`, `prod-monitoring`).
3. Choose the **teams** the token may act on (see [Team scope](#team-scope)).
4. Pick an **expiration** (see [Expiration](#expiration)).
5. Select the **abilities** the token should have. Tick the smallest set the consumer actually needs (see [Permissions](#permissions)).
6. Click **Create**.

The plain-text token appears once, in a modal. It's prefixed with `bruin_pat_` so secret scanners can spot it if it leaks. Copy it now — Bruin Cloud doesn't store the plaintext and won't show it again. If you lose it, delete the token and create a new one.

The token is used as a bearer token:

```text
Authorization: Bearer bruin_pat_xxxxxxxx…
```

## Team scope

A personal token is scoped to one or more of the teams you belong to. You choose the scope at creation, and you can change it later without rotating the secret.

- **Specific teams** — tick the teams the token may act on. Its abilities are still clamped to your role *on each team* individually, so a permission you hold on team A but not team B is inert on B.
- **All my teams (current & future)** — a dynamic scope that automatically covers any team you join later. Useful for a personal CLI token you don't want to keep re-editing.

Because a token can span several teams, requests that target a team need to say which one:

- A **single-team** token needs nothing extra — the API infers the team.
- A **multi-team** token must name the team on each request (the `X-Bruin-Team` header, or `--team` in the CLI). Without it, the API returns `409 team_required`.
- Naming a team the token isn't scoped to returns `403 team_not_in_scope`.

See [Using a token with the CLI](#using-a-token-with-the-cli) for how the CLI handles this with `--team` and a stored default team.

## Expiration

Every token has an expiry. When creating one, pick **30 / 60 / 90 days**, **1 year**, a **custom date**, or **No expiration** (defaults to 90 days). Your team may cap the maximum lifetime, in which case longer presets and "No expiration" are hidden and a custom date is limited to the cap.

Once a token is within **7 days** of expiring it's flagged **Expiring soon**, and its owner gets a one-time reminder email. After it expires the token stops authenticating and shows as **Expired** — create a fresh one to replace it.

## Permissions

Abilities are grouped in the create/edit form by what they unlock. Only abilities that actually gate an API endpoint are offered, and each is clamped to your role, so the exact checkboxes you see depend on your permissions. The groups:

- **Pipelines & Runs** — list/show/update/delete pipelines and assets; list, trigger, re-run, and mark the status of runs (`pipeline:list`, `pipeline:run:trigger`, `pipeline:run:re-run`, `pipeline:run:mark-as`, …).
- **Connections** — `connection:list`, `connection:create`, `connection:delete`.
- **AI Agents** — talk to and manage agents, threads, and connection sets (`agent:list`, `agent:message:send`, `agent:thread:export`, `connection-set:list`, …).
- **Dashboards** — `dashboard:list`, `dashboard:create`, `dashboard:update`, `dashboard:publish`, `dashboard:delete`, …
- **Scheduled Agents** — `scheduled-agent:list`, `scheduled-agent:manage`, …
- **Glossary** — `glossary:entity:list`, `glossary:entity:show`.
- **Team & Admin** — `team:update`, `audit-log:list`, and `mcp:token` (required for the [Cloud MCP](/cloud/mcp-setup)).

Destructive abilities (anything with `delete`) are shown in red as a reminder. If your role no longer grants an ability the token was created with, the token keeps working for everything else and the UI marks the ability as **Restricted**.

Pick the smallest set the consumer needs. See [Use cases](#use-cases) below for common combinations.

## Edit abilities and scope

Click **Edit** on any token to change its abilities or team scope. Changes apply immediately and the token value itself doesn't change — the same secret keeps working, only with the new permissions. (Full-access tokens are the exception: they can't be edited in place, recreate them instead.)

## Using a token

### As a bearer token

For HTTP calls, CI/CD, the [Cloud MCP](/cloud/mcp-setup), and any external system, send the token in the `Authorization` header:

```text
Authorization: Bearer bruin_pat_xxxxxxxx…
```

For a multi-team token, add `X-Bruin-Team: <company_prefix>` to target a specific team.

### Using a token with the CLI

The [`bruin cloud`](/commands/cloud) commands read the same token. Point them at it with the `--api-key` flag, the `BRUIN_CLOUD_API_KEY` environment variable, or a `bruin` connection in `.bruin.yml`:

```yaml
# .bruin.yml
environments:
  default:
    connections:
      bruin:
        - name: "cloud"
          api_token: "bruin_pat_xxxxxxxx…"
```

If your token is scoped to more than one team, tell the CLI which team to act on with `--team <company_prefix>` (run `bruin cloud teams list` to see the prefixes), or set a default once so you can skip it:

```bash
bruin cloud config set-team acme
```

See the [`cloud` command reference](/commands/cloud#config) for the full team-resolution rules.

## Revoke a token

Click **Delete** on the token row, then confirm. The token stops working immediately and drops off the list. Team admins can also revoke a member's token from **Team Settings → API Tokens**.

If you suspect a token has leaked, revoke first and ask questions later. A new token takes seconds to create.

## Limits

You can hold up to **25** personal tokens at a time (per-run agent tokens don't count). The create/edit/delete actions are rate-limited, so bulk automation should reuse a token rather than churn new ones.

## Use cases

- **Bruin CLI** — a token scoped to your team with the abilities for the commands you run (e.g. `pipeline:run:trigger` to trigger runs, `agent:message:send` to chat with agents). See [`bruin cloud`](/commands/cloud).
- **Cloud MCP** — `mcp:token` is the only required scope. See [Cloud MCP](/cloud/mcp-setup) for the connection setup in Cursor, Claude Code, and Codex.
- **CI / CD** — give your pipeline a token with `pipeline:run:trigger` (and `pipeline:run:list` if you poll for status). Don't grant `pipeline:delete`.
- **External monitoring** — `pipeline:run:list`, `pipeline:asset:show`, and `audit-log:list` are usually enough.

## Troubleshooting

**`401 Unauthorized`** — the token is missing, malformed, revoked, or expired. Double-check the `Authorization: Bearer …` header and that the token still exists and hasn't passed its expiry.

**`403 forbidden` / *Insufficient token permissions*** — the token doesn't have the ability the endpoint needs, or your current role no longer grants it. Edit the token's abilities (and check the **Restricted** markers) and retry.

**`409 team_required`** — the token is scoped to more than one team and the request didn't say which. Add `X-Bruin-Team` (or `--team` in the CLI).

**`403 team_not_in_scope`** — the team you targeted isn't in the token's scope. Edit the token's teams, or target one it's scoped to.

**Token doesn't appear in `last_used_at`** — `last_used_at` updates asynchronously. Give it a minute, then refresh.

## Related

- [`cloud` command](/commands/cloud) — drive Bruin Cloud from the CLI with a token.
- [Cloud MCP](/cloud/mcp-setup) — wire a token into Cursor, Claude Code, or Codex.
- [Team Settings](/cloud/team-settings) — where the team-wide API Tokens panel lives.
- [Audit Logs](/cloud/audit-logs) — token creation, updates, and deletion are all logged.
