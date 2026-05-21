# Microsoft Teams

Connect a Bruin Cloud AI agent to Microsoft Teams so your team can ask data questions in a channel, group chat, or 1:1 chat with the bot. Teams uses **connect codes** — short codes you generate on the agent and paste into the chat to link them.

> [!WARNING]
> The Bruin Teams agent uses large language models to answer data questions. Outputs can contain mistakes, misinterpret your schema, or generate incorrect SQL. Review results before relying on them for business decisions.

## Install the Bruin app in Teams

Ask your Teams admin to install the Bruin app from the Microsoft Teams admin center (or sideload the app package if you're piloting). Once installed, you can add **Bruin** to any channel, group chat, or as a personal app.

When Bruin is added to a chat, it sends a welcome card with the commands you can use.

## Connect an agent to a Teams chat

### 1. Generate a connect code

1. In Bruin Cloud, open **AI → Agents** and edit the agent you want to deploy.
2. Open the **Integrations** section and pick **Microsoft Teams**.
3. Click **Generate Code**. A code like `BRN-A1B2` is shown, valid for 10 minutes.

### 2. Send the code in Teams

In the Teams chat where you want the agent to respond, send the code. The exact format depends on the scope:

| Scope | What to send |
|---|---|
| Channel | `@Bruin connect BRN-A1B2` |
| Group chat | `@Bruin connect BRN-A1B2` |
| Personal (1:1) chat | `connect BRN-A1B2` |

Bruin replies confirming the chat is connected, e.g. *"Connected to agent 'Analytics Bot'. Mention me with @Bruin to ask a question."*

The code is single-use — it's consumed as soon as it's accepted. Generate a new one if it expires before you use it.

## Use the agent in Teams

**In a channel or group chat**, mention `@Bruin` and ask a question:

```text
@Bruin how many active customers signed up last week?
```

**In a 1:1 chat with the bot**, just type your question. No mention needed.

The agent responds inline in the same chat — running queries, generating charts, and attaching reports when relevant.

## Disconnect a chat

To unlink a chat from its agent:

| Scope | What to send |
|---|---|
| Channel | `@Bruin disconnect` |
| Group chat | `@Bruin disconnect` |
| Personal chat | `disconnect` |

Bruin confirms the chat is no longer connected. You can then send a new `connect BRN-XXXX` code to wire it up to a different agent.

## How it works

- **One bot, many scopes.** A single Microsoft Teams app serves channel, group, and personal chats. Each chat is identified separately and linked to one agent.
- **Connect codes expire fast.** A code is valid for 10 minutes and is single-use. This keeps stale codes from accidentally linking the wrong chat.
- **Channels are identified stably.** Bruin uses the Teams `teamsChannelId` (not the conversation ID) so message threads don't confuse routing.
- **Replay-safe.** Teams retries activities it didn't get a 200 for; Bruin deduplicates by activity ID.
- **Welcome card on install.** When the bot is added to a chat, Bruin sends a card explaining the commands and how to connect.

## Troubleshooting

**"Connect code not found or expired."** The code is older than 10 minutes or has already been used. Generate a new one.

**"This conversation is already connected to agent ..."** You're trying to link a chat that's already linked. Disconnect first, then send the new code.

**Bruin doesn't see my messages.** In channels and group chats, you must mention `@Bruin` for the bot to receive the message. In a 1:1 personal chat, no mention is needed.

## Next

- [Configure Agents](/cloud/ai-agents/configure) — set up the agent before you generate a connect code.
- [Integrations overview](/cloud/integrations/overview) — how connect codes and platform routing work end-to-end.
