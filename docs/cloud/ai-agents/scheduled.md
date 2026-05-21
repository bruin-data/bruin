# Scheduled Agents

A **scheduled agent** is an existing AI agent given a specific task to run on a recurring schedule: daily reports, threshold alerts, custom SQL runs. Each scheduled run uses the agent's [connection set](/cloud/connections#connection-sets-for-ai-agents), integrations, and CLI access, and posts results to its messaging integrations or the Bruin Cloud chat.

If you have not created an agent yet, see [Configure Agents](/cloud/ai-agents/configure) first.

## Create a scheduled agent

### 1. Open Scheduled Agents

From the **AI** menu, open **Scheduled agents**.

### 2. New scheduled agent

Click **New scheduled agent**, give it a name, and pick the underlying agent it should use. The schedule inherits whatever connections, integrations, and CLI access that agent already has.

### 3. Configure the task

The configuration page has two halves:

- **Right panel:** the actual settings (name, schedule, instructions, notifications, custom SQL, output format).
- **Left panel:** a chat where you can describe the task in plain English and let an AI build the configuration for you.

You can set things up manually, or describe the task and let the agent fill it in.

### 4. Set the schedule

Pick from built-in presets, or write a custom cron expression for full control.

### 5. Set up notifications

Pick a notification channel: Slack, Teams, WhatsApp, or any [integration](/cloud/integrations/overview) that is already wired up to the underlying agent. Only integrations the agent has access to appear here.

### 6. Optional: custom SQL or output format

You can manually provide:

- A SQL query the agent should run.
- An output format. For example, a PDF report, a formatted Slack message, or a threshold-based alert.

## Example: daily stock report

A natural-language prompt to the configuration chat:

> Send a daily PDF report to our Slack channel summarizing the previous day's Apple and Microsoft stock. If a stock moved more than 5%, format the message as an alert.

After a minute or so, the configuration agent builds the full setup:

- The instructions for the scheduled run.
- A daily schedule.
- The notification target Slack channel.
- The SQL query to run each day.
- The output format: a PDF report plus a Slack alert when the threshold is hit.

## Activate and run

Once the configuration looks right:

- **Enable** the scheduled agent. You will see the next run time (UTC).
- Use **New run** to trigger it manually any time.
- Open the **Runs** tab to see history and status, and jump into any specific run.

## Where the runs live

Scheduled agents execute inside the AI **Chats** view. That means:

- A scheduled agent with no messaging integration only shows up under Chats.
- A scheduled agent with an integration (Slack, WhatsApp, Teams, etc.) still appears under Chats. The integration is just the destination for the output.

For more detail on chat-vs-integration routing, see [Integrations overview](/cloud/integrations/overview) and the FAQ entry on [where scheduled runs show up](/cloud/faq#my-scheduled-agent-has-a-slack-integration-where-do-the-runs-show-up).

## Next

- [Integrations](/cloud/integrations/overview) to deliver results into Slack, Teams, Discord, WhatsApp, or Telegram.
- [WhatsApp integration](/cloud/integrations/whatsapp#the-24-hour-window-and-template-fallback) for the 24-hour-window template fallback when scheduled runs deliver to WhatsApp.
- [Chat with Agents](/cloud/ai-agents/chat) to iterate on the underlying agent before scheduling it.
