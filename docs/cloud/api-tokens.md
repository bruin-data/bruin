# API Tokens

API tokens let you talk to Bruin Cloud programmatically — from the [Bruin CLI](/commands/cloud), CI pipelines, custom scripts, the [Cloud MCP](/cloud/mcp-setup), or any external system that needs to read or trigger things in your team. Each token carries a scoped set of abilities and an expiry date, so you can hand a CI job a token that can only trigger runs without giving it permission to delete pipelines.

Bruin Cloud has two kinds of token:

- **Team tokens** — owned by the team, act *as the team*. They carry **exactly** the abilities you grant them (they're not tied to any one person's role), which makes them the right choice for shared automation, CI/CD, and service integrations. Created and managed by team admins.
- **Personal access tokens (PATs)** — owned by you, act *as you*. Their abilities are always clamped to your live role on each team, so a PAT can never do more than you can, and if your role is reduced the token loses those abilities immediately. Any team member can create one for their own CLI use and scripts.

Both are bearer tokens and both expire.

|  | Team token | Personal access token |
|--|-----------|-----------------------|
| Acts as | the team | you |
| Abilities | exactly what you grant | clamped to your role |
| Teams | the team it's created in | one or more of your teams |
| Who can create | team admins | any member (self-service) |
| Managed in | Team Settings → API Tokens | User menu → Access Tokens |

## Where to manage tokens

- **Team tokens** — **Team Settings → API Tokens**. Creating and revoking them requires team-admin permission.
- **Personal access tokens** — open the **user menu** (your avatar, top-right) and choose **Access Tokens** under *Manage Account*. This opens the **Personal Access Tokens** page (`/account/api-tokens`). Self-service — no admin permission required. Your PATs also appear in the Team Settings panel alongside the team's shared tokens.

Each panel lists a token's name, status (active / expiring soon / expired), abilities, and last-used timestamp.

## Create a token

1. Click **Create Token**.
2. Give it a **name** (e.g. `ci-trigger`, `mcp-cursor`, `prod-monitoring`).
3. For a personal token, choose the **teams** it may act on (see [Team scope](#team-scope-personal-tokens)).
4. Pick an **expiration** (see [Expiration](#expiration)).
5. Select the **abilities** the token should have — the smallest set the consumer actually needs (see [Permissions](#permissions)).
6. Click **Create**.

The plain-text token appears once, in a modal. Copy it now — Bruin Cloud doesn't store the plaintext and won't show it again. If you lose it, delete the token and create a new one.

The token is used as a bearer token:

```text
Authorization: Bearer <token>
```

## Team scope (personal tokens)

A personal token is scoped to one or more of the teams you belong to. You choose the scope at creation, and you can change it later without rotating the secret. (Team tokens don't have this — they always act on the one team they belong to.)

- **Specific teams** — tick the teams the token may act on. Its abilities are still clamped to your role *on each team* individually, so a permission you hold on team A but not team B is inert on B.
- **All my teams (current & future)** — a dynamic scope that automatically covers any team you join later. Useful for a personal CLI token you don't want to keep re-editing.

Because a personal token can span several teams, requests that target a team need to say which one:

- A **single-team** token needs nothing extra — the API infers the team.
- A **multi-team** token must name the team on each request (the `X-Bruin-Team` header, or `--team` in the CLI). Without it, the API returns `409 team_required`.
- Naming a team the token isn't scoped to returns `403 team_not_in_scope`.

See [Using a token with the CLI](#using-a-token-with-the-cli) for how the CLI handles this with `--team` and a stored default team.

## Expiration

Every token has an expiry. When creating one, pick **30 / 60 / 90 days**, **1 year**, a **custom date**, or **No expiration** (defaults to 90 days).

Once a token is within **7 days** of expiring it's flagged **Expiring soon**, and (for personal tokens) its owner gets a one-time reminder email. After it expires the token stops authenticating and shows as **Expired** — create a fresh one to replace it.

## Permissions

Abilities are grouped in the create/edit form by what they unlock. Only abilities that actually gate an API endpoint are offered, so there are no dead checkboxes. The groups:

- **Pipelines & Runs** — list/show/update/delete pipelines and assets; list, trigger, re-run, and mark the status of runs (`pipeline:list`, `pipeline:run:trigger`, `pipeline:run:re-run`, `pipeline:run:mark-as`, …).
- **Connections** — `connection:list`, `connection:create`, `connection:delete`.
- **AI Agents** — talk to and manage agents, threads, and connection sets (`agent:list`, `agent:message:send`, `agent:thread:export`, `connection-set:list`, …).
- **Dashboards** — `dashboard:list`, `dashboard:create`, `dashboard:update`, `dashboard:publish`, `dashboard:delete`, …
- **Scheduled Agents** — `scheduled-agent:list`, `scheduled-agent:manage`, …
- **Glossary** — `glossary:entity:list`, `glossary:entity:show`.
- **Team & Admin** — `team:update`, `audit-log:list`, and `mcp:token` (required for the [Cloud MCP](/cloud/mcp-setup)).

A team token carries exactly the abilities you tick. A personal token carries the same set clamped to your role, so the checkboxes you see depend on your permissions; if your role later drops an ability the token was created with, the token keeps working for everything else and the UI marks that ability as **Restricted**. Destructive abilities (anything with `delete`) are shown in red as a reminder.

Pick the smallest set the consumer needs. See [Use cases](#use-cases) below for common combinations.

## Edit abilities and scope

Click **Edit** on a token to change its abilities (and, for a personal token, its team scope). Changes apply immediately and the token value itself doesn't change — the same secret keeps working, only with the new permissions. (Full-access tokens are the exception: they can't be edited in place, recreate them instead.)

## Using a token

### As a bearer token

For HTTP calls, CI/CD, the [Cloud MCP](/cloud/mcp-setup), and any external system, send the token in the `Authorization` header:

```text
Authorization: Bearer <token>
```

For a multi-team personal token, add `X-Bruin-Team: <company_prefix>` to target a specific team.

### Using a token with the CLI

The [`bruin cloud`](/commands/cloud) commands work with either token type. Point them at your token with the `--api-key` flag, the `BRUIN_CLOUD_API_KEY` environment variable, or a `bruin` connection in `.bruin.yml`:

```yaml
# .bruin.yml
environments:
  default:
    connections:
      bruin:
        - name: "cloud"
          api_token: "your-token-here"
```

The CLI reads `.bruin.yml` from the Git repository root, so the `.bruin.yml` approach only works inside a repo — outside one (or to override it), use `--api-key` or `BRUIN_CLOUD_API_KEY` instead.

If you're using a personal token scoped to more than one team, tell the CLI which team to act on with `--team <company_prefix>` (run `bruin cloud teams list` to see the prefixes), or set a default once so you can skip it:

```bash
bruin cloud config set-team acme
```

See the [`cloud` command reference](/commands/cloud#config) for the full team-resolution rules.

## Revoke a token

Click **Delete** on the token row, then confirm. The token stops working immediately and drops off the list. Team admins can revoke any team token, and a member's personal token, from **Team Settings → API Tokens**.

If you suspect a token has leaked, revoke first and ask questions later. A new token takes seconds to create.

## Limits

You can hold up to **25** personal tokens at a time (per-run agent tokens don't count). The personal create/edit/delete actions are rate-limited, so bulk automation should reuse a token rather than churn new ones.

## Use cases

- **Cloud MCP** — `mcp:token` is the only required scope. See [Cloud MCP](/cloud/mcp-setup) for the connection setup in Cursor, Claude Code, and Codex.
- **CI / CD** — a team token with `pipeline:run:trigger` (and `pipeline:run:list` if you poll for status). Don't grant `pipeline:delete`.
- **Bruin CLI** — a personal token scoped to your team with the abilities for the commands you run (e.g. `pipeline:run:trigger`, `agent:message:send`). See [`bruin cloud`](/commands/cloud).
- **External monitoring** — `pipeline:run:list`, `pipeline:asset:show`, and `audit-log:list` are usually enough.

## Troubleshooting

**`401 Unauthorized`** — the token is missing, malformed, revoked, or expired. Double-check the `Authorization: Bearer …` header and that the token still exists and hasn't passed its expiry.

**`403 forbidden` / *Insufficient token permissions*** — the token doesn't have the ability the endpoint needs. For a personal token, your current role may no longer grant it — check the **Restricted** markers. Edit the token's abilities and retry.

**`409 team_required`** — a personal token scoped to more than one team, and the request didn't say which. Add `X-Bruin-Team` (or `--team` in the CLI).

**`403 team_not_in_scope`** — the team you targeted isn't in the personal token's scope. Edit the token's teams, or target one it's scoped to.

**Token doesn't appear in `last_used_at`** — `last_used_at` updates asynchronously. Give it a minute, then refresh.

## Related

- [`cloud` command](/commands/cloud) — drive Bruin Cloud from the CLI with a token.
- [Cloud MCP](/cloud/mcp-setup) — wire a token into Cursor, Claude Code, or Codex.
- [Team Settings](/cloud/team-settings) — where the team-wide API Tokens panel lives.
- [Audit Logs](/cloud/audit-logs) — token creation, updates, and deletion are all logged.
