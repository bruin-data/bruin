# Integrations

Bruin Cloud AI agents can answer data questions wherever your team already works. Connect an agent to a messaging platform and people can ask data questions in plain English without ever opening Bruin Cloud.

> [!WARNING]
> Bruin's chat integrations are powered by large language models. Outputs can contain mistakes, misinterpret your schema, or generate incorrect SQL. Review results before relying on them for business decisions.

## Supported platforms

| Platform | Where it works | How an agent links to a chat |
|---|---|---|
| [Slack](/cloud/integrations/slack) | Channels and DMs | OAuth install per workspace, then enter a Channel ID on the agent |
| [Microsoft Teams](/cloud/integrations/teams) | Channels, group chats, and 1:1 chats | Install the Bruin app, then send a `connect BRN-XXXX` code |
| [Google Chat](/cloud/integrations/google-chat) | DMs and spaces (rooms) | Install the Bruin Chat app, then send a `connect BRN-XXXX` code |
| [Discord](/cloud/integrations/discord) | Server channels and threads | Install the Bruin bot, then enter a Channel ID on the agent |
| [WhatsApp](/cloud/integrations/whatsapp) | Direct messages and groups | Message Bruin's number with a `connect BRN-XXXX` code |
| [Telegram](/cloud/integrations/telegram) | Direct messages only | Message [@BruinDataBot](https://t.me/BruinDataBot) with a `connect BRN-XXXX` code |

## How integrations work

Every chat platform plugs into the same agent runtime. The difference is only in how a conversation gets linked to an agent.

**Workspace-level install, channel-level config (Slack, Discord).** You install the Bruin app once into the Slack workspace or Discord server. From then on, each agent can be wired to a specific Channel ID — that channel becomes the place where the agent listens and responds.

**Connect codes (Teams, Google Chat, WhatsApp, Telegram).** You generate a short code (format `BRN-XXXX`, valid for 10 minutes) on the agent's integrations panel, then paste it into the target chat. The platform sees the code, links that conversation to the agent, and the code is burned.

Either way, the link is stored as an `AgentIntegration` record — platform + external channel/chat ID → agent. When a message comes in, Bruin looks up the integration, finds the agent, and routes the conversation through the same processing pipeline used by the web chat.

## Generating a connect code

Connect codes are used by Teams, WhatsApp, and Telegram.

1. Open **AI → Agents** and edit the agent you want to deploy.
2. Open the **Integrations** section and pick the platform.
3. Click **Generate Code**. A code in the format `BRN-XXXX` is shown, valid for 10 minutes.
4. Send the code (with the `connect ` prefix) into the target chat. Format and channel mention rules differ by platform — see the per-platform pages.

If the code expires before you use it, just generate another one.

## What agents can do in chat

Once connected, an agent in a chat behaves the same way as it does in the Bruin Cloud web chat. It can:

- Query the data warehouse using the agent's [connection set](/cloud/connections#connection-sets-for-ai-agents).
- Generate and render charts inline.
- Attach files and reports.
- Run Bruin Cloud CLI commands (if enabled on the agent).
- Read your repo's `AGENTS.md`, glossary, catalog, and lineage to ground its answers.

The agent's system prompt, connection set, and project scope are configured once on the agent — they apply across every integration the agent is connected to.

## Next

- [Configure Agents](/cloud/ai-agents/configure) — set up the agent itself before connecting it to a chat platform.
- [Chat with Agents](/cloud/ai-agents/chat) — try the agent in the Bruin Cloud web chat first.
