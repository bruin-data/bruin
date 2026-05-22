# Configure Agents

Agents in Bruin Cloud are configurable AI assistants. Each agent can be scoped to a project, connected to messaging platforms, and given a [connection set](/cloud/connections#connection-sets-for-ai-agents) that controls exactly which data it can read. You can also let it run Bruin Cloud CLI commands and give it a custom system prompt.

## Create an agent

### 1. Open the agents page

From the **AI** menu, go to **Agents**. You will see a list of existing agents and a button to create a new one.

### 2. Pick a project

Select the [project](/cloud/projects) the agent should connect to.

- **Pick a project**: the agent gets access to that project's repo and pipelines, including any `AGENTS.md` or instruction files in the repo.
- **No project**: the agent has no access to your repos or projects. It behaves like a regular ChatGPT or Claude chat.

### 3. Name the agent

Give the agent a clear name. This is how it shows up in the agents list and in any messaging integrations.

### 4. Add messaging integrations

You can deploy the agent into any of the supported chat platforms. Each platform has its own page covering install, configuration, and usage:

- [Slack](/cloud/integrations/slack): OAuth install per workspace, then enter Channel IDs on the agent.
- [Microsoft Teams](/cloud/integrations/teams): install the Bruin app, then send a `connect BRN-XXXX` code in a channel, group chat, or 1:1.
- [Discord](/cloud/integrations/discord): OAuth install per server, then enter Channel IDs and use the `/bruin` slash command.
- [WhatsApp](/cloud/integrations/whatsapp): message Bruin's WhatsApp number with a `connect BRN-XXXX` code (DMs and groups).
- [Telegram](/cloud/integrations/telegram): DM [@BruinDataBot](https://t.me/BruinDataBot) with a `connect BRN-XXXX` code.

For an end-to-end view of how connect codes, channel IDs, and webhook routing work, see the [Integrations overview](/cloud/integrations/overview).

If you do not pick an integration, the agent is still usable in the Bruin Cloud web chat, the dashboard builder, and as a [scheduled agent](/cloud/ai-agents/scheduled).

### 5. Attach a connection set

Under **Connection Set (optional)**, pick the connection set the agent should use.

A [connection set](/cloud/connections#connection-sets-for-ai-agents) is a named bundle of connections to data platforms, kept separate from the connections your pipelines use. This separation lets you:

- Restrict agents to only the data they need.
- Give agents read-only access where pipelines have read/write.
- Apply granular, agent-specific permissions without touching pipeline credentials.

An agent with **no connection set** can still answer general questions and help with non-data tasks, but it cannot read your data. It behaves more like a regular ChatGPT or Claude.

### 6. Optional: Cloud CLI access

Give the agent access to the Bruin Cloud CLI. With this enabled, the agent can:

- Run pipelines and assets.
- Read logs and run history.
- Query the data catalog and glossary.

### 7. Optional: System prompt

Add a system prompt to give the agent specific instructions, a role, or constraints. Useful when you want the agent to focus on a particular workflow or follow a specific tone.

### 8. Create the agent

Click **Create Agent**. The agent is now available in the agents list and ready to use in chat, dashboards, scheduled runs, or any connected messaging platform.

## Reconfigure an agent

Open an agent from the agents list to change its settings at any time:

- Add or remove integrations.
- Swap the connection set.
- Edit the system prompt.
- **Manage access**: control which teams and members in your organization can use the agent.

## Next

- [Chat with Agents](/cloud/ai-agents/chat) to put a new agent to work in the Bruin Cloud web chat.
- [Scheduled Agents](/cloud/ai-agents/scheduled) to run the agent on a cron.
- [Integrations](/cloud/integrations/overview) to deploy the agent into Slack, Teams, Google Chat, Discord, WhatsApp, or Telegram.
- [Project structure](/core-concepts/project) and [Connections overview](/connections/overview) — what the agent sees through its connection set.
