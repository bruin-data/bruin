# Chat with Agents

Chat is the main way to talk to a configured AI agent in Bruin Cloud. Depending on how the agent is set up, you can use it as a data analyst that queries your warehouse, or as an assistant for data engineering tasks like checking pipeline status and run history.

If you have not created an agent yet, see [Configure Agents](/cloud/ai-agents/configure) first.

## Open a chat

### 1. Navigate to AI → Chats

From the **AI** menu, open **Chats**.

### 2. Pick an agent

Use the dropdown to select the agent you want to talk to. Each agent comes with the project, [connection set](/cloud/connections#connection-sets-for-ai-agents), integrations, and system prompt configured for it.

### 3. Send a prompt

Type your question and send.

> [!TIP]
> A useful first prompt is to ask the agent what it has access to. This makes it inspect its environment and tell you what data it can query before you dig in.

## Example: generate a financial report

Suppose the agent has access to a warehouse with stock-market data.

> Create a financial report for Microsoft and Apple as a PDF.

From the prompt and the warehouse metadata, the agent:

- Identifies the datasets and tables to query.
- Runs the queries it needs.
- Generates charts.
- Attaches both a Python file and a PDF for download.

This kind of task typically completes in around 20 steps and a handful of queries.

## Example: operate Bruin Cloud via CLI

If the agent has Cloud CLI access enabled, you can also use it for data engineering tasks. It can:

- Read pipeline run history.
- Inspect assets and the catalog.
- Run pipelines and check their status.

> How many times has pipeline X run in the last 10 days, and how many of those failed?

The agent runs the CLI commands it needs and reports back with the numbers and the failing run IDs.

## How chats are scoped

- The agent's project, connection set, integrations, and CLI access are set on the agent itself. Pick the agent in the chat dropdown to switch context.
- Each chat runs in a sandbox that clones the connected repo and builds context from your `AGENTS.md` files. See the [Slack AI Analyst tutorial](/cloud/ai-agents/slack-ai-analyst#6-add-agent-instructions) for an example `AGENTS.md`.
- For data access and retention details, see [Does the agent see my actual data?](/cloud/faq#does-the-agent-see-my-actual-data) in the FAQ.

## Next

- Want the agent to turn answers into something visual? Open **AI → Dashboards** and use the same agent there.
- Want it to run on a schedule? See [Scheduled Agents](/cloud/ai-agents/scheduled).
- Want it in Slack, Teams, Discord, WhatsApp, or Telegram? See the [Integrations overview](/cloud/integrations/overview).
