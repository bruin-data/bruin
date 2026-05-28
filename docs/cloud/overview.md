# Bruin Cloud

[Bruin Cloud](https://cloud.getbruin.com/register) is a managed platform built on top of the open-source [Bruin CLI](/getting-started/introduction/installation). It runs your pipelines on a schedule, stores your connections securely, gives you a UI for monitoring runs and lineage, and ships an AI layer that can chat with your data, build dashboards, and answer questions in tools like Slack and Teams.

> [!INFO]
> This section of the documentation covers Bruin Cloud. If you are looking for the open-source CLI, start at the [Quickstart](/getting-started/introduction/quickstart).

## What you get

- Managed scheduling: pipelines defined in your Git repo run on their configured schedule without you running a server.
- Connections and secrets: BigQuery, Snowflake, Postgres, Databricks, S3, and dozens of other platforms configured through the UI. Credentials are encrypted at rest with [HashiCorp Vault](/secrets/vault).
- Run monitoring: [runs](/cloud/runs), logs, [lineage](/cloud/catalog#global-lineage), [backfills](/cloud/backfills), manual runs, and per-asset history for every pipeline.
- AI agents and [dashboards](/cloud/dashboards): configurable agents scoped to projects and [connection sets](/cloud/connections#connection-sets-for-ai-agents). Use them in the Bruin Cloud chat, embed them in [Slack, Teams, Google Chat, Discord, WhatsApp, or Telegram](/cloud/integrations/overview), schedule them, or build dashboards with them.
- Cross-pipeline dependencies: depend on assets that live in a different pipeline or repo using URIs.
- [Insights](/cloud/insights): cost explorer, pipeline health, risk report, and usage tracking.
- [Catalog](/cloud/catalog) and [governance](/cloud/governance): a glossary, owners, and built-in quality rules that score every asset.
- Team administration: [team settings](/cloud/team-settings), [API tokens](/cloud/api-tokens), and a full [audit log](/cloud/audit-logs).

## How to read these docs

If you are new to Bruin Cloud, start with [Getting Started](/cloud/getting-started). It walks through wiring up a Git repo, adding connections, and enabling your first pipeline.

If you haven't installed the open-source CLI yet, the cloud docs assume you'll be defining pipelines in code — [Quickstart](/getting-started/introduction/quickstart), [Pipeline definition](/pipelines/definition), and [Asset definition schema](/assets/definition-schema) are the starting points there.

From there:

- [Projects](/cloud/projects): connect a Git repo, choose between the GitHub App and a personal access token, migrate existing projects.
- [Connections](/cloud/connections): configure the connections your pipelines and agents use.
- [Pipelines](/cloud/pipelines): enable pipelines, trigger runs, manage backfills, view lineage.
- [Runs](/cloud/runs): cross-pipeline run history, rerun, mark success/failure, drill into per-asset logs.
- [Backfills](/cloud/backfills): multi-interval re-processing across historical date ranges.
- [Assets](/cloud/assets): asset catalog, per-asset detail, profile, columns, custom checks, AI suggestions.
- [Catalog](/cloud/catalog): glossary, owners, and global lineage across pipelines.
- [Insights](/cloud/insights): cost explorer, pipeline health, risk report, usage.
- [Dashboards](/cloud/dashboards): AI-built dashboards your team can re-open without re-asking.
- [AI Agents](/cloud/ai-agents/overview): create agents, chat with them, schedule them, deploy them to chat platforms.
- [Integrations](/cloud/integrations/overview): connect agents to Slack, Microsoft Teams, Google Chat, Discord, WhatsApp, or Telegram.
- [Notifications](/cloud/notifications): pipeline-level Slack, Teams, Discord, and webhook notifications.
- [Cross-pipeline dependencies](/cloud/cross-pipeline): depend on assets that live in other pipelines.
- [Governance](/cloud/governance): the rules that drive quality scores and the risk report.
- [Instance Types](/cloud/instance-types): sizing assets at run time.
- [Security](/cloud/security): network access and dedicated egress IPs for allowlisting.
- [Team Settings](/cloud/team-settings), [API Tokens](/cloud/api-tokens), [Audit Logs](/cloud/audit-logs): team administration.
- [Cloud MCP](/cloud/mcp-setup): talk to Bruin Cloud from Cursor, Claude Code, or Codex.
- [FAQ](/cloud/faq): short answers to common questions, including patterns that look plausible but are not real features.

---

[Sign up for Bruin Cloud →](https://cloud.getbruin.com/register)
