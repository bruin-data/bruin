# Discord

Connect a Bruin Cloud AI agent to Discord so your community or team can query data with a slash command. Discord uses a server-level bot install, then per-agent channel configuration.

> [!WARNING]
> The Bruin Discord agent uses large language models to answer data questions. Outputs can contain mistakes, misinterpret your schema, or generate incorrect SQL. Review results before relying on them for business decisions.

## Install the Bruin bot in Discord

### 1. Open Integrations

In Bruin Cloud, go to **AI → Agents → Integrations** and click **Connect** next to Discord. This opens Discord's OAuth page.

### 2. Authorize the bot

Pick the Discord server, review the permissions, and click **Authorize**. The Bruin bot is added to your server with permission to read messages and post replies, and the `/bruin` slash command becomes available.

You only do this once per Discord server.

## Configure an agent for a Discord channel

### 1. Open the agent

In AI Agent Settings, edit the agent you want to deploy to Discord.

### 2. Add channel IDs

In the agent's Integrations section, expand **Discord** and enter the **Channel ID** of each channel where this agent should respond. Add as many as you need.

> [!TIP]
> Discord hides channel IDs behind Developer Mode. Enable it under **Settings → Advanced → Developer Mode**, then right-click a channel and choose **Copy Channel ID**. ([Discord help article](https://support.discord.com/hc/en-us/articles/206346498))

### 3. Save

Save the agent.

## Use the agent in Discord

Inside a connected channel, run the slash command:

```text
/bruin question: which marketing source drove the most signups last week?
```

You can also attach a file to your question — use the optional `file` parameter on the slash command and Bruin will pick it up alongside your text. The agent responds in the same channel with a data-driven answer.

If you run `/bruin` inside a **thread**, Bruin reuses the existing conversation context for that thread, so follow-up questions stay in scope.

## How it works

- **One bot install per server.** OAuth-installed; the bot serves every agent configured in that server.
- **Channels are wired per agent.** Each agent listens only in the channel IDs you put on it. If `/bruin` is run in a channel not linked to any agent, Discord tells the user the channel isn't connected.
- **Slash command, not mentions.** Discord works through the `/bruin` slash command, not bot mentions. Discord shows a "thinking…" indicator immediately while Bruin processes the question.
- **Threads inherit context.** If `/bruin` is run in a thread, Bruin looks up the parent channel for routing but keeps the conversation tied to that thread.
- **Signature-verified.** Every Discord interaction is verified against the Bruin app's public key before processing.
- **Replay-safe.** Discord interactions are deduplicated by interaction ID.

## Troubleshooting

**"This channel is not connected to any agent."** Add the channel ID to an agent's Discord integration in Bruin Cloud.

**`/bruin` doesn't appear.** Discord may take a few minutes to register slash commands after install. Reload Discord or wait a minute.

**The bot can't see channels.** Make sure the Bruin bot role has access to the channel — Discord respects channel-level role permissions even after the OAuth install.

## Next

- [Configure Agents](/cloud/ai-agents/configure) — set up the agent before wiring it to Discord channels.
- [Integrations overview](/cloud/integrations/overview) — how Bruin agents connect across platforms.
