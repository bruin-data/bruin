# API Tokens

For the CLI, [sign in with `bruin cloud login`](/commands/login). You do not need to create or copy a token manually.

For CI, scripts, or other integrations, create a token in Cloud. Choose the type for your use:

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
5. Choose an [access level](#permissions). Select **Custom** to pick individual permissions.
6. Click **Create**.

Copy the token when it appears and save it somewhere secure. You cannot view it again; if you lose it, delete it and create another.

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

Choose an access level when creating or editing a token:

| Access level | Choose it to |
|--------------|--------------|
| **Read only** | View pipelines, runs, dashboards, and agents. |
| **Read & write (no delete)** | Run pipelines, chat with agents, and create or edit resources without deletion access. |
| **Read & write** | Include deletion access for actions your role permits. |
| **Custom** | Select individual permissions. |

For **Custom**, select the permissions needed by your commands or integration. See [Use cases](#use-cases) for examples.

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

To sign in through your browser:

```bash
bruin cloud login
bruin cloud projects list
```

See the [login instructions](/commands/login) for repository and global login options.

To use a token you already created, pass `--api-key` or set `BRUIN_CLOUD_API_KEY`:

```bash
export BRUIN_CLOUD_API_KEY="your-token-here"
bruin cloud projects list
```

To save it for a repository, add this connection to `.bruin.yml` at the Git root:

```yaml
# .bruin.yml
environments:
  default:
    connections:
      bruin:
        - name: "cloud"
          api_token: "your-token-here"
```

Run `bruin auth status` to check which login you are using. For command options, see the [Cloud command reference](/commands/cloud#authentication).

If you're using a personal token scoped to more than one team, tell the CLI which team to act on with `--team <company_prefix>` (run `bruin cloud teams list` to see the prefixes), or set a default once so you can skip it:

```bash
bruin cloud config set-team acme
```

See the [`cloud` command reference](/commands/cloud#config) for the full team-resolution rules.

## Revoke a token

Click **Delete** on the token row, then confirm. The token stops working immediately and drops off the list. Team admins can revoke any team token, and a member's personal token, from **Team Settings → API Tokens**.

If you suspect a token has leaked, revoke first and ask questions later. A new token takes seconds to create.

## Limits

You can hold a limited number of personal tokens at a time (per-run agent tokens don't count). The personal create/edit/delete actions are rate-limited, so bulk automation should reuse a token rather than churn new ones.

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
