# Bruin Cloud

[Bruin Cloud](https://cloud.getbruin.com/register) is a managed platform built on top of the open-source Bruin CLI. It runs your pipelines on a schedule, stores your connections securely, gives you a UI for monitoring runs and lineage, and ships an AI layer that can chat with your data, build dashboards, and answer questions in Slack.

> [!INFO]
> This section of the documentation covers Bruin Cloud. If you are looking for the open-source CLI, start at the [Quickstart](/getting-started/introduction/quickstart).

## What you get

- **Managed scheduling** — pipelines defined in your Git repo run on their configured schedule without you running a server.
- **Connections and secrets** — connect to BigQuery, Snowflake, Postgres, Databricks, S3, and dozens of other platforms through the UI. Credentials are encrypted at rest with [HashiCorp Vault](/secrets/vault).
- **Run monitoring** — runs, logs, lineage, backfills, manual runs, and per-asset history for every pipeline.
- **AI agents** — configurable agents scoped to projects and connection sets. Use them in chat, embed them in Slack/Teams/Discord/WhatsApp, schedule them, or build dashboards with them.
- **Cross-pipeline dependencies** — depend on assets that live in a different pipeline or repo using URIs.
- **Developer environments** — browser-based IDEs preconfigured with your repo and credentials.
- **Insights** — cost explorer, pipeline health, and risk reports.

## How to read these docs

If you are new to Bruin Cloud, start with [Getting Started](/cloud/getting-started). It walks through wiring up a Git repo, adding connections, and enabling your first pipeline.

From there:

- **[Projects](/cloud/projects)** — connect a Git repo, manage Git authentication (GitHub App vs personal access token), migrate existing projects.
- **[Connections](/cloud/connections)** — configure the connections your pipelines and agents use.
- **[Pipelines](/cloud/pipelines)** — enable pipelines, trigger runs, manage backfills, view lineage.
- **[AI Agents](/cloud/ai-agents/overview)** — create agents, chat with them, schedule them, deploy to Slack.
- **[Notifications](/cloud/notifications)** — Slack, Teams, Discord, and webhook notifications.
- **[Cross-pipeline dependencies](/cloud/cross-pipeline)** — depend on assets that live in other pipelines.
- **[Developer Environments](/cloud/developer-environments)** — browser-based IDEs.
- **[Instance Types](/cloud/instance-types)** — sizing assets at run time.
- **[dbt Projects](/cloud/dbt)** — running dbt projects on Bruin Cloud.
- **[Cloud MCP](/cloud/mcp-setup)** — talk to Bruin Cloud from Cursor, Claude Code, or Codex.

---

[Sign up for Bruin Cloud →](https://cloud.getbruin.com/register)
