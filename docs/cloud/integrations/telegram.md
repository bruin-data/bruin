# Telegram

Connect a Bruin Cloud AI agent to Telegram so you can ask data questions in a direct message with the Bruin bot. Telegram uses a **connect code** to link your chat to a specific agent.

> [!WARNING]
> The Bruin Telegram agent uses large language models to answer data questions. Outputs can contain mistakes, misinterpret your schema, or generate incorrect SQL. Review results before relying on them for business decisions.

> [!NOTE]
> Telegram integration currently supports **direct messages only**. Group chats are not supported — Bruin will tell users in a group to DM the bot instead.

## Connect an agent to Telegram

### 1. Open the Bruin bot

Open [@BruinDataBot](https://t.me/BruinDataBot) on Telegram and tap **Start**. The bot replies with a welcome message and the command list.

### 2. Generate a connect code

1. In Bruin Cloud, open **AI → Agents** and edit the agent you want to deploy.
2. Open the **Integrations** section and pick **Telegram**.
3. Click **Generate Code**. A code like `BRN-A1B2` is shown, valid for 10 minutes.

### 3. Send the code on Telegram

In your DM with @BruinDataBot, send:

```text
connect BRN-A1B2
```

Bruin replies *"Connected! Now you are linked to 'Analytics Bot'. You can ask questions about your data pipelines."* The code is single-use — generate a new one if it expires before you use it.

## Use the agent on Telegram

Send a question to @BruinDataBot in your DM. No mention or command needed:

```text
how many active customers signed up last week?
```

Bruin runs the question against your agent's connection set and replies inline.

### Commands

| Command | What it does |
|---|---|
| `/start` | Show the welcome message and command list. |
| `connect BRN-XXXX` | Link this DM to an agent (or switch to a different one). |
| `/reset` | Start a fresh session. Clears prior context. |

### Switch to a different agent

Send a new `connect BRN-XXXX` code. Bruin replies *"Switched from 'X' to 'Y'."* and all subsequent messages route to the new agent.

## How it works

- **DMs only.** Telegram group chats are explicitly rejected — Bruin replies *"Bruin AI is only available in direct messages. Please DM me instead!"* and ignores the message.
- **Webhook-secured.** Every Telegram webhook is verified against a secret token before processing. Replays are deduplicated by `chat_id` + `message_id`.
- **One chat → one agent.** Each Telegram DM (one per Telegram user) is linked to one agent. To change the agent, send a new connect code.
- **Threads are per-user.** Each Telegram user has their own thread with the agent. `/reset` creates a fresh thread without unlinking the agent.

## Troubleshooting

**"Welcome to Bruin AI! To get started, please send your connect code."** Your DM isn't linked to any agent yet. Generate a code in Bruin Cloud and send `connect BRN-XXXX`.

**"Connect code not found or expired."** The code is older than 10 minutes or has already been used. Generate a new one.

**"Bruin AI is only available in direct messages."** You tried to use the bot in a Telegram group. Telegram support is DM-only — open a 1:1 chat with @BruinDataBot instead.

**"No agent connected. Please connect first."** You ran `/reset` before linking an agent. Send `connect BRN-XXXX` first.

## Next

- [Configure Agents](/cloud/ai-agents/configure) — set up the agent before generating a connect code.
- [Integrations overview](/cloud/integrations/overview) — how connect codes work across platforms.
