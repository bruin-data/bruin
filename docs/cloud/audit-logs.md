# Audit Logs

Bruin Cloud records every consequential action — who triggered which pipeline run, who created or revoked an API token, who changed a connection, who hit which API endpoint. The audit log is the timeline you reach for when something went sideways at 3am.

Find it in **Team Settings → Audit Logs**.

> [!INFO]
> Audit log access requires the `audit-log:list` permission. By default this is granted to team admins.

## What's tracked

Events are grouped roughly into the following families. The page shows the human-readable event type next to each row.

### Pipeline operations

- **Pipeline run triggered** — manual or API trigger.
- **Pipeline run re-run** — rerun all or rerun failed.
- **Asset instance re-run** — single asset retried.
- **Pipeline status change** — pipeline enabled, disabled, or deleted.
- **Run note updated** — note added or edited.
- **Asset marked as instance** / **Asset instance marked as run** — manual status changes from the run detail page.
- **Enable / Disable multiple pipelines** — bulk operations.
- **Mark multiple pipeline runs status** — bulk mark via the runs page.
- **Mark multiple asset instances** — bulk mark within a run.
- **Trigger multiple pipeline runs** — bulk trigger (e.g. backfill setup).

### Connections

- **Connection created** — with type and name in the metadata.
- **Connection deleted**.

### Team and users

- **Team created**, **Team updated**, **Team deleted** — team updates record old vs new values.
- **User login** — login source and IP.

### API tokens

- **API token created** — name, abilities, ID.
- **API token updated** — old vs new abilities.
- **API token deleted**.

### AI agents

- **Agent created**, **Agent updated**, **Agent deleted**.
- **Agent send message** — agent invocation.
- **Agent get message status**.
- **Agent list threads**, **Agent list messages**.
- **List agents**.

### API requests (28 distinct request types)

Every cross-cutting read endpoint logs its hit, with the calling token name and request parameters. The full list includes:

- Get pipelines / projects / single pipeline / pipeline runs
- Get glossary entities and entity details
- Get pipeline assets / single asset / asset instances (for runs, details, logs)
- Get pipeline validation errors
- Rerun, mark, trigger, enable / disable endpoints

These are useful for understanding what an integration is hitting and when. They're identified in the metadata with the endpoint, method, and any pipeline/asset/run identifiers in scope.

### Billing

- **Billing action** — Stripe-related state changes (customer created, plan changed, threshold crossed).

## Reading the feed

Each row in the table shows:

- **User** — who did it (their email, with a colour-coded source badge below).
- **Event** — the human-readable event type.
- **Source** — where the action came from: **web** (blue), **api** (purple), **mcp** (amber).
- **IP** — IP address of the request.
- **Timestamp** — UTC.

Click any row to expand it. The expanded view shows the full event metadata. For state changes, that includes side-by-side **old / new** values:

- Team updates show old and new values for every field that changed.
- API token updates show old and new ability lists, side by side.
- API requests show the endpoint, method, token name, and any project / pipeline / asset / run identifiers in scope.

## Filters

Above the table:

- **User** — multi-select of team members.
- **Event Type** — multi-select of event types.
- **Date Range** — Start Date and End Date. Inclusive on both ends, treated as 00:00:00 / 23:59:59 UTC.

Filters apply live — the feed re-fetches when you change one.

## Export to CSV

Use the export icon in the top-right menu of the table. Export respects your current filters, so apply them first to narrow the dump. The CSV contains User, Event Type, Source, IP, Timestamp, and the expanded metadata for each row.

Exports are useful for compliance reviews, post-incident timelines, and slicing the data in a spreadsheet.

## Retention

Bruin Cloud retains audit logs indefinitely for the duration of your subscription. Contact your account manager if you have specific retention or destruction requirements (e.g. GDPR right-to-erasure on a deactivated user).

## Related

- [Team Settings](/cloud/team-settings#audit-logs) — the audit log panel lives here.
- [API Tokens](/cloud/api-tokens) — token creation, updates, and deletion are all audit-logged.
- [Pipelines](/cloud/pipelines) and [Runs](/cloud/runs) — every manual action recorded.
