# Slack

Connect a Bruin Cloud AI agent to Slack so your team can ask data questions from any channel or in a direct message. Slack uses a workspace-level OAuth install, then per-agent channel configuration.

> [!WARNING]
> The Bruin Slack agent uses large language models to answer data questions. Outputs can contain mistakes, misinterpret your schema, or generate incorrect SQL. Review results before relying on them for business decisions.

## Connect Slack to your workspace

### 1. Open AI Agent Settings

From the **AI** menu, choose **Agents** to open AI Agent Settings.

### 2. Open Integrations

In the AI Agent Settings sidebar, click **Integrations**. You'll see every available messaging platform. Click **Connect** next to Slack to start the OAuth flow.

### 3. Authorize Bruin in Slack

Slack asks you to authorize the Bruin app in your workspace. Review the permissions, pick the right workspace from the dropdown, and click **Allow**.

Once authorized, the Slack integration is available to every agent in this workspace. You only do this once per Slack workspace.

## Configure an agent for a Slack channel

### 1. Open the agent

In AI Agent Settings, open the agent you want to deploy to Slack.

### 2. Expand Slack under Integrations

In the agent's Integrations section, expand **Slack** and enter the **Channel ID** of the Slack channel where Bruin should respond. You can add multiple channel IDs for the same agent.

> [!TIP]
> To find a Channel ID, right-click a channel name in Slack, select **View channel details**, and scroll to the bottom.

### 3. Invite the Bruin bot to the channel

In Slack, type `/invite @Bruin` (or whatever the bot is named in your workspace) in the channel. Bruin only listens in channels it's been invited to.

### 4. Save

Save the agent.

## Use the agent in Slack

**In a channel**, mention `@Bruin` and ask a question:

```text
@Bruin which marketing source drove the most signups last week?
```

**In a DM**, just message the Bruin bot directly — no mention needed. Direct messages are routed to your **default agent** (configured in your Bruin Cloud user settings). The DM uses your Slack profile email to match you to a Bruin Cloud account, so make sure your email is visible in your Slack profile.

The agent responds inside Slack with a data-driven answer — running queries, generating charts, and attaching reports when relevant. If you delete the question while Bruin is still working, the in-flight response is silenced.

## How it works

- **One install per workspace.** Slack uses an OAuth install; the Bruin app and bot token are scoped to the Slack workspace and shared across every agent in that workspace.
- **Channels are wired per agent.** An agent listens in the channel IDs you put on its Integrations panel. Different agents can serve different channels.
- **DMs go to your default agent.** Slack DMs are routed by your email's default agent setting.
- **Replay-safe.** Slack retries any event it didn't get a 2xx for within 3 seconds. Bruin deduplicates retries by event ID so you don't get double responses.
- **Signature-verified.** Every Slack webhook is verified against the Slack signing secret before it's processed.

## Troubleshooting

**"I couldn't verify your identity."** Your Slack profile email isn't visible to apps. Set it visible in your Slack profile preferences.

**"I couldn't find a Bruin Cloud account for ...".** Your Slack email doesn't match any Bruin Cloud user. Ask your admin to invite that email, or sign in to Bruin Cloud with that email.

**"You don't have a default agent configured."** Open your Bruin Cloud user settings and pick a default agent for Slack DMs.

**Bruin doesn't respond in a channel.** Check that (1) the channel ID is on the agent, (2) you've invited the bot to that channel with `/invite @Bruin`, and (3) you mentioned the bot in the message.

## Next

For an end-to-end example — build a pipeline, enhance metadata, deploy an analyst to Slack — see the [Slack AI Analyst tutorial](/cloud/ai-agents/slack-ai-analyst).
