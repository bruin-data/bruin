# Team Settings

The **Team Settings** page is where you manage your team — name, projects, members, billing, audit logs, and more. Open it from the team menu in the top bar (click your team name) and choose **Team Settings**.

The page is split into sections in the left sidebar. What you see depends on your role and your team's tier.

## General

Basic team info.

- **Team Name** — display name across Bruin Cloud.
- **Team Owner** — read-only display of the owner's profile and email.
- **Enable AI Features** — toggle to enable OpenAI-backed features (asset descriptions, suggestions, the AI agent runtime).

Requires `team:update` to edit. Saved via the team update endpoint.

## Projects

Every Git project connected to your team.

- **Project list** — each row shows the project name, type (hosted or GitHub App), Git token status, and per-project actions: **Edit**, **Update Secret** (if PAT-authenticated), **Migrate to GitHub App** (if still PAT), **Delete**.
- **Add Project** — kick off the [project creation flow](/cloud/projects#create-a-project). Choose between the Bruin GitHub App, a personal access token, or a hosted project (no Git).
- **Column Metadata Keys** — define custom metadata keys that appear on every asset's [Columns tab](/cloud/assets#columns). Use these for internal tags like "PII", "regulated", "deprecated" that your team agrees on.

Requires `team:update` to add, edit, or delete.

## Team Access

Member and invitation management — labeled **Team Members** in the UI sidebar.

- **Add Team Member** — invite by email and pick a role. The invitation goes out by email; pending invitations show in a separate list until accepted.
- **Pending Invitations** — list of unaccepted invitations, with **Resend** and **Cancel** actions.
- **Team Members** — list of active members with their last login, role badge, and per-row actions:
  - **Permissions** — change the member's role.
  - **Make Owner** — transfer team ownership (owner-only action, requires confirmation).
  - **Leave** — only on your own row.
  - **Remove** — only on other members' rows.
- **API Tokens** — a separate panel for managing API tokens (see [API Tokens](/cloud/api-tokens)).

Bruin employees (`@getbruin.com` addresses) are grouped separately at the bottom of the member list for clarity.

Most actions require `team:update`.

## Usage & Billing

- **Billing Details** — Stripe customer info, current tier, payment method status. The **Create Stripe Customer** button (admin-only) provisions Stripe billing for the team.
- **Usage Details** — pipeline runs, asset instances, and compute hours consumed in the current billing cycle.

For richer breakdowns by asset, pipeline, or project, use [Insights → Usage](/cloud/insights#usage). Billing thresholds are stored per team and can be adjusted with help from your account manager.

## Audit Logs

The full event history for your team. See [Audit Logs](/cloud/audit-logs) for what's tracked, filtering, and export. The Audit Logs section here is the same view, embedded in settings.

Filters: User, Event Type, Date Range. Each row expands to show the full metadata. Export to CSV from the kebab menu in the top right.

Requires `audit-log:list`.

## Danger Zone

Owner-only section, visible if you own the team and it's not a personal team.

- **Delete Team** — permanently deletes the team and everything in it. Two-step confirmation required.

This is irreversible. Pipelines, runs, connections, agents, dashboards, and audit logs all go away.

## Related

- [Projects](/cloud/projects) — add a Git repo and migrate to the GitHub App.
- [API Tokens](/cloud/api-tokens) — create, scope, and revoke tokens.
- [Audit Logs](/cloud/audit-logs) — full event reference.
- [Insights → Usage](/cloud/insights#usage) — finer-grained usage and cost breakdown.
