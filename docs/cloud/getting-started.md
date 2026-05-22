---
outline: deep
---

# Getting Started

Bruin Cloud has two tracks. Pick the one that matches what you want to do first — you can set up the other later from the same workspace.

<div class="track-grid">
  <a href="#ai-data-analyst-track" class="track-card">
    <div class="track-badge ai">AI</div>
    <h3>AI Data Analyst</h3>
    <p>Connect a warehouse, ask questions in plain English, get charts and reports back. Push answers into Slack, Teams, or WhatsApp.</p>
    <ul>
      <li>Best for analysts and PMs</li>
      <li>Connect once, ask anything</li>
      <li>No code required</li>
    </ul>
    <span class="track-cta">Start with AI Data Analyst →</span>
  </a>
  <a href="#etl-elt-pipeline-track" class="track-card">
    <div class="track-badge etl">ETL</div>
    <h3>ETL/ELT Pipeline</h3>
    <p>Define SQL and Python transforms in Git, then schedule them. Bruin parses, validates, and runs your pipelines automatically.</p>
    <ul>
      <li>Best for data engineers</li>
      <li>Git-based, code-first workflow</li>
      <li>Scheduled SQL + Python transforms</li>
    </ul>
    <span class="track-cta">Start with ETL/ELT Pipeline →</span>
  </a>
</div>

<style scoped>
.track-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 16px;
  margin: 24px 0 32px;
}
.track-card {
  display: flex;
  flex-direction: column;
  padding: 20px 22px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  background: var(--vp-c-bg-soft);
  text-decoration: none !important;
  color: inherit;
  transition: border-color 0.2s, transform 0.2s;
}
.track-card:hover {
  border-color: var(--vp-c-brand-1);
  transform: translateY(-2px);
}
.track-card h3 {
  margin: 8px 0 6px;
  border-top: none !important;
  padding-top: 0 !important;
  font-size: 18px;
}
.track-card p {
  margin: 0 0 12px;
  font-size: 14px;
  color: var(--vp-c-text-2);
  line-height: 1.5;
}
.track-card ul {
  list-style: none;
  padding: 0;
  margin: 0 0 16px;
  font-size: 13px;
  color: var(--vp-c-text-2);
}
.track-card ul li {
  padding: 2px 0;
}
.track-card ul li::before {
  content: "•";
  color: var(--vp-c-brand-1);
  margin-right: 8px;
}
.track-badge {
  align-self: flex-start;
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.5px;
  padding: 3px 8px;
  border-radius: 4px;
  text-transform: uppercase;
}
.track-badge.ai {
  background: rgba(99, 102, 241, 0.12);
  color: rgb(99, 102, 241);
}
.track-badge.etl {
  background: rgba(16, 185, 129, 0.12);
  color: rgb(16, 185, 129);
}
.track-cta {
  margin-top: auto;
  font-size: 13px;
  font-weight: 500;
  color: var(--vp-c-brand-1);
}
</style>

## Before you start

**Sign up** at [cloud.getbruin.com/register](https://cloud.getbruin.com/register) with email and password, or sign in with Google. Right after sign-up, Bruin Cloud asks which track you want — pick **AI Data Analyst** or **ETL/ELT Pipeline**.

If you'd rather start in code, the open-source [Bruin CLI](/getting-started/introduction/installation) defines the same pipelines, assets, and connections Bruin Cloud runs. Start with the [Quickstart](/getting-started/introduction/quickstart), then connect your repo to Cloud.

> [!TIP]
> You can switch tracks at any time from **Getting Started** on the home page. The toggle is labelled **AI Analyst** / **Data Engineer**.

## AI Data Analyst track

Connect a warehouse, ask the agent questions, then push answers into your team's chat tools.

**1. Connect your data.** Add a [connection](/cloud/connections) to a warehouse the agent should read from — BigQuery, Postgres, MySQL, SQL Server, Snowflake, Databricks, or Redshift. Create with validation so Bruin can confirm the credentials work.

> [!TIP]
> No warehouse credentials yourself? Invite a teammate from your data team and have them set the connection up. The agent then becomes available to the whole workspace.

**2. Ask your first question.** Open **AI → Chats**, pick the default agent, and try:
> What data do you have access to?

The agent inspects the schema and tells you what's available. From there, ask the questions that matter — revenue, retention, top SKUs, whatever your team needs. See [Chat with Agents](/cloud/ai-agents/chat).

**3. Connect a chat platform.** Open **AI → Agents**, pick the agent, and wire it to where your team already works:

- [Slack](/cloud/integrations/slack) — OAuth install, then a Channel ID per agent
- [Microsoft Teams](/cloud/integrations/teams) — `connect BRN-XXXX` in a channel, group chat, or 1:1
- [Google Chat](/cloud/integrations/google-chat) — `connect BRN-XXXX` in a DM or space
- [Discord](/cloud/integrations/discord) — bot install, then `/bruin` in any wired channel
- [WhatsApp](/cloud/integrations/whatsapp) — message Bruin's number with `connect BRN-XXXX`
- [Telegram](/cloud/integrations/telegram) — DM [@BruinDataBot](https://t.me/BruinDataBot) with `connect BRN-XXXX`

**4. Build a dashboard.** Open **AI → Dashboards** and describe what you want. The agent assembles charts, tables, and metrics from your prompt; iterate until it looks right, then click **Publish**. See [Dashboards](/cloud/dashboards).

**5. Schedule a report.** Have the agent run on a cadence and post results into your chat tool. Daily revenue summaries, weekly retention alerts, threshold-based notifications. See [Scheduled Agents](/cloud/ai-agents/scheduled).

**6. (Optional) Add a context layer.** Connect a Git repo with your dbt or Bruin semantic layer so the agent learns your team's vocabulary and metric definitions. Or describe tables manually for the metrics that matter most. Both options are in [Team Settings → Projects](/cloud/team-settings#projects).

## ETL/ELT Pipeline track

Wire a Git repo, configure the connections your pipelines need, enable them, then monitor.

**1. Create a project.** A [project](/core-concepts/project) in Bruin Cloud maps one-to-one with a Git repository. Creating one syncs the pipelines in that repo and gives you a place to manage their connections.

> [!TIP]
> Use the **Bruin GitHub App** instead of a personal access token where possible — fine-grained access, no expiring tokens, repo-level installs. See [Projects](/cloud/projects#github-authentication).

**2. Add connections.** While the project syncs, open **Connections** and add the data sources, destinations, and secrets your pipelines reference. These are the same connections you'd define locally in `.bruin.yml`, but encrypted in Bruin Cloud's [HashiCorp Vault](/secrets/vault) instead of your repo.

See [Connections](/cloud/connections) for connection types, naming rules, and the validation flow.

**3. Enable a pipeline.** Pipelines synced from a repo start disabled. Open **Catalog → Pipelines**, pick one, and click **Enable selected pipelines**. If any referenced connections are missing, Bruin Cloud lists them and lets you add each one inline.

> [!INFO]
> The first run triggers automatically when you enable a new pipeline. You don't need to click **New run** yourself.

**4. Monitor and operate.** Once a pipeline is active, the pipeline page is your operations console:

- **Runs panel** for status and history. See [Runs](/cloud/runs).
- **Assets** for every asset with its type, owner, schedule, last run. See [Assets](/cloud/assets).
- **Backfills** for historical reprocessing. See [Backfills](/cloud/backfills).
- **Lineage** for how assets connect. See [Catalog → Lineage](/cloud/catalog#global-lineage).
- **New run** for ad-hoc runs, full refreshes, and backfills.

**5. Set up notifications.** Configure pipeline-level alerts on success or failure in your `pipeline.yml`. Bruin Cloud supports Slack, Microsoft Teams, Discord, and generic webhooks. See [Notifications](/cloud/notifications).

**6. (Optional) Cross-pipeline dependencies.** If pipelines in different repos depend on each other's assets, wire them up with URIs instead of duplicating definitions. See [Cross-pipeline dependencies](/cloud/cross-pipeline).

## After the first run

These apply regardless of which track you started with:

- [Team Settings](/cloud/team-settings) — members, projects, billing, audit logs
- [API Tokens](/cloud/api-tokens) — programmatic access (CI, MCP, external monitoring)
- [Insights](/cloud/insights) — cost explorer, pipeline health, risk report, usage
- [Cloud MCP](/cloud/mcp-setup) — talk to Bruin Cloud from Cursor, Claude Code, or Codex
- [FAQ](/cloud/faq) — common questions and patterns that aren't real features
