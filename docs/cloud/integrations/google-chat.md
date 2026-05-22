# Google Chat

Connect a Bruin Cloud AI agent to Google Chat so your team can ask data questions in a DM with the bot or inside a shared space (room). Google Chat uses **connect codes** to link a chat to a specific agent.

> [!WARNING]
> The Bruin Google Chat agent uses large language models to answer data questions. Outputs can contain mistakes, misinterpret your schema, or generate incorrect SQL. Review results before relying on them for business decisions.

## Install the Bruin Chat app

The Bruin Chat app is installed at the Google Workspace level. Ask your Workspace admin to add Bruin from the Google Workspace Marketplace, or sideload the app package if you're piloting.

Once installed, anyone in the workspace can:

- Open a 1:1 chat with **Bruin** from Chat.
- Add **Bruin** to a space.

When Bruin is added to a chat, it sends a welcome card with the commands you can use.

## Connect an agent to a Google Chat space

### 1. Generate a connect code

1. In Bruin Cloud, open **AI → Agents** and edit the agent you want to deploy.
2. Open the **Integrations** section and pick **Google Chat**.
3. Click **Generate Code**. A code like `BRN-A1B2` is shown, valid for 10 minutes.

### 2. Send the code in Google Chat

In the chat where you want the agent to respond, send the code. The format is the same in DMs and spaces — the @mention is optional but harmless:

```text
connect BRN-A1B2
```

Bruin replies confirming the chat is connected, e.g. *"Connected to agent 'Analytics Bot'. You can now ask me questions about your data pipelines!"*

The code is single-use — it's consumed as soon as it's accepted. Generate a new one if it expires before you use it.

## Use the agent in Google Chat

**In a DM with Bruin**, just type your question. No mention needed:

```text
how many active customers signed up last week?
```

**In a space**, mention `@Bruin` first so the bot receives the message:

```text
@Bruin how many active customers signed up last week?
```

The agent replies inline with a data-driven answer — running queries, generating charts, and attaching reports when relevant. Replies stay in the same thread so follow-ups keep context.

## Commands

| Command | What it does |
|---|---|
| `connect BRN-XXXX` | Link this DM or space to an agent. |
| `help` (or `/help`, `bruin help`, `?`) | Show the welcome card with commands and example questions. |

There is no inline `disconnect` command in Google Chat. To unlink a space from its agent, remove the integration from the agent's **Integrations** panel in Bruin Cloud, or remove the Bruin bot from the space.

## How it works

- **Workspace-level install, space-level linking.** A single Bruin Chat app serves the workspace; each space (or DM) is linked to one agent via a connect code.
- **Connect codes expire fast.** A code is valid for 10 minutes and is single-use.
- **DMs vs spaces.** In a DM, every message is routed to the linked agent. In a space, only messages that mention `@Bruin` are processed.
- **Threads preserved.** Replies are posted in the same thread as the original question — follow-ups inherit context. If a thread doesn't exist, Bruin falls back to a new one.
- **Signature-verified.** Every webhook is verified against a Google-issued JWT (issuer `accounts.google.com`, audience matched against the Bruin project number).
- **Replay-safe.** Messages are deduplicated by message ID for five minutes to absorb Google Chat retries.
- **Welcome and help cards.** Adding Bruin to a space triggers a welcome card; `help` shows it again on demand.

## Troubleshooting

**"This space is already connected to agent '…'."** The space is linked to a different agent. Remove the existing integration from that agent's **Integrations** panel in Bruin Cloud, then send a new `connect BRN-XXXX` code.

**"Connect code not found or expired."** The code is older than 10 minutes or has been used. Generate a new one from the agent in Bruin Cloud.

**"Invalid connect code format. Expected format: BRN-XXXX."** Codes must be `BRN-` followed by four alphanumeric characters. Check for typos or autocorrect.

**Bruin doesn't see my messages in a space.** Spaces require an explicit `@Bruin` mention for the bot to receive the message. DMs don't need one.

## Next

- [Configure Agents](/cloud/ai-agents/configure) — set up the agent before generating a connect code.
- [Integrations overview](/cloud/integrations/overview) — how connect codes work across platforms.
