# Getting Started

The shortest path from a Bruin Cloud sign-up to a pipeline running on a schedule.

If you would rather start with the AI side (connect a warehouse and ask questions in chat or Slack), jump to [AI Analyst onboarding](#ai-analyst-onboarding) at the bottom.

## 1. Sign up

Create an account at [cloud.getbruin.com/register](https://cloud.getbruin.com/register) with email and password, or sign in with Google.

## 2. Pick a track

After sign-up, Bruin Cloud asks which track you want to start with:

- **ETL/ELT Pipeline** — wire your Git repo, configure connections, enable pipelines on a schedule. Continue with this page.
- **AI Data Analyst** — connect a warehouse and chat with your data right away. Skip to [AI Analyst onboarding](#ai-analyst-onboarding).

You can switch tracks at any time from **Getting Started** on the home page (the toggle is labelled **AI Analyst** / **Data Engineer**).

## 3. Create a project

A [project](/core-concepts/project) in Bruin Cloud maps one-to-one with a Git repository. Creating a project syncs the pipelines in that repo, and gives you a place to manage the connections those pipelines need.

See [Projects](/cloud/projects) for the full walkthrough: picking GitHub authentication, selecting a repo, naming the project, waiting for the initial sync.

## 4. Add connections

While the project is syncing, open **Connections** and add the data sources, destinations, and secrets your pipelines reference. These are the same connections you would define locally in `.bruin.yml`, but stored encrypted in Bruin Cloud instead of in your repo.

See [Connections](/cloud/connections) for connection types, naming rules, and the validation flow.

## 5. Enable a pipeline

Pipelines synced from a repo start disabled. Open **Catalog → Pipelines**, pick one, and click **Enable selected pipelines**. If any referenced connections are missing, Bruin Cloud lists them and lets you add each one inline.

The first run triggers automatically when you enable a new pipeline.

See [Pipelines](/cloud/pipelines) for the full walkthrough, including runs, backfills, lineage, and manual runs.

## 6. Monitor and operate

Once a pipeline is active, the pipeline page is where you operate it:

- Watch the **runs** panel for status and history.
- Open **Assets** to see every asset with its type, owner, schedule, and last run.
- Use **Backfills** for historical reprocessing.
- Use **Lineage** to see how assets connect.
- Use **New run** for ad-hoc runs, full refreshes, and backfills.

For pipeline-level Slack, Teams, Discord, or webhook notifications on success/failure, see [Notifications](/cloud/notifications). For dependencies across pipelines, see [Cross-pipeline dependencies](/cloud/cross-pipeline).

## AI Analyst onboarding

Want to start by asking questions in chat or Slack instead of wiring pipelines? The AI track is:

1. **Connect a warehouse.** Add a [connection](/cloud/connections) for BigQuery, Postgres, MySQL, SQL Server, Snowflake, Databricks, or Redshift. We recommend creating with validation so Bruin can confirm credentials work. If you do not have warehouse access yourself, invite a teammate from your data team to the workspace and have them set it up.
2. **Open AI → Chats.** Pick the default agent and ask a question. A good first prompt is "What data do you have access to?" The agent will inspect the schema and tell you what is available. See [Chat with Agents](/cloud/ai-agents/chat).
3. **Add a chat integration.** From the **Agents** page, open the agent and connect [Slack](/cloud/integrations/slack), [Microsoft Teams](/cloud/integrations/teams), [Discord](/cloud/integrations/discord), [WhatsApp](/cloud/integrations/whatsapp), or [Telegram](/cloud/integrations/telegram). See [Configure Agents](/cloud/ai-agents/configure) and the [Integrations overview](/cloud/integrations/overview).
4. **Build a dashboard.** Use the agent to assemble charts and filters from natural language, then publish.
5. **Schedule a report.** Have the agent run on a cadence and post results to Slack. See [Scheduled Agents](/cloud/ai-agents/scheduled).
6. **Optional: add a context layer.** Connect a Git repo (your dbt or Bruin semantic layer), or describe tables manually, so the agent gets accuracy on your team's metrics.

## Next

- [Projects](/cloud/projects) and [Connections](/cloud/connections) for the pipeline track.
- [AI Agents](/cloud/ai-agents/overview) for the AI track.
- [FAQ](/cloud/faq) for common questions and patterns that aren't real features.
