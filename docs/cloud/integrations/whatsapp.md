# WhatsApp

Connect a Bruin Cloud AI agent to WhatsApp so people can ask data questions from their phone — in a direct message or a group chat. WhatsApp uses **connect codes** to link a chat to a specific agent.

> [!WARNING]
> The Bruin WhatsApp agent uses large language models to answer data questions. Outputs can contain mistakes, misinterpret your schema, or generate incorrect SQL. Review results before relying on them for business decisions.

## Connect an agent to WhatsApp

### 1. Generate a connect code

1. In Bruin Cloud, open **AI → Agents** and edit the agent you want to deploy.
2. Open the **Integrations** section and pick **WhatsApp**.
3. Click **Generate Code**. A code like `BRN-A1B2` is shown, valid for 10 minutes. The panel also shows the Bruin WhatsApp number and an "Open in WhatsApp" deep link.

### 2. Send the code on WhatsApp

Message Bruin's WhatsApp number (or click the deep link) and send:

```text
connect BRN-A1B2
```

You can do this from a **direct chat** with the Bruin bot or from a **group** that includes the bot. Bruin replies confirming the chat is connected to your agent.

The code is single-use — it's consumed as soon as it's accepted. If it expires, just generate another.

## Use the agent on WhatsApp

Send a question to the Bruin bot — in a DM or in a group that's connected to the agent. No mention or special command is needed:

```text
how many active customers signed up last week?
```

Bruin shows a ⏳ reaction while it's working, then replies with the answer in the same chat.

You can also send:

- **Voice notes** — Bruin transcribes them with Whisper and answers your spoken question.
- **Images** — attach a chart, screenshot, or photo and ask Bruin about it.
- **Documents** — PDFs, CSVs, spreadsheets, etc. Bruin stores them and uses them as context for the question.

### Reset the conversation

To start a fresh thread (clears prior context):

```text
/reset
```

Bruin confirms with *"Session reset! Starting fresh with [agent name]."*

### Switch to a different agent

Send a fresh `connect BRN-XXXX` code in the same chat. Bruin replies *"Switched from 'X' to 'Y'."* and routes all subsequent messages to the new agent.

## The 24-hour window and template fallback

WhatsApp restricts business-initiated messages to a 24-hour window after the user's last message. If Bruin tries to send you a [scheduled run](/cloud/ai-agents/scheduled) result outside that window, the message will fail.

When this happens Bruin automatically sends an approved WhatsApp template ("session followup") with a **Continue** button. Tap **Continue** and Bruin resends the scheduled result inline. The Continue button is valid for the most recent scheduled run completed in the last 24 hours.

## How it works

- **Delivered via Kapso.** Bruin uses [Kapso](https://kapso.ai) as the WhatsApp Business API provider. Webhooks are signature-verified, and replays are deduplicated by message ID.
- **One chat → one agent.** Each WhatsApp chat (DM or group) is linked to one agent via the `connect` code. To change agents, send a new code.
- **Voice → text.** Audio messages are transcribed with OpenAI Whisper before being passed to the agent.
- **Media in S3.** Images and documents are stored in S3 and referenced as message attachments. Presigned download URLs are generated fresh at processing time.
- **Group support.** When Bruin is in a group, the whole group shares one agent and one thread. The sender's phone number is recorded on each message for traceability.

## Troubleshooting

**"Welcome to Bruin AI! To get started, please send your connect code."** The chat isn't linked to any agent yet. Generate a code in Bruin Cloud and send `connect BRN-XXXX`.

**"Still working on your previous request. Please wait a moment."** Bruin is processing an earlier message in the same thread. Wait for the reply, then send the next question.

**"Invalid connect code format."** Codes must be `BRN-` followed by 4 alphanumeric characters. Check for typos or autocorrect changes.

**Bruin doesn't reply in a group.** Make sure the Bruin WhatsApp bot is a member of the group and has been linked with `connect BRN-XXXX` from inside the group.

## Next

- [Scheduled Agents](/cloud/ai-agents/scheduled) — push scheduled results into WhatsApp on a cron.
- [Integrations overview](/cloud/integrations/overview) — how connect codes work across platforms.
