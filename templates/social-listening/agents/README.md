# Bruin Cloud agents for social listening

This folder holds prompts and draft plans for optional Bruin Cloud AI agents that read
the pipeline's output. Installing the template does not create, schedule or activate any
agent. Nothing here runs until an operator creates it in Bruin Cloud, reviews it, tests it
by hand and turns it on.

| File | What it is | How it runs |
| --- | --- | --- |
| `mention-ops.prompt.md` | System prompt for the read-only operations agent | Chat, and as the agent behind both plans |
| `weekday-mention-digest.plan.yml` | Draft plan: weekday digest of newly routed mentions | Scheduled, after review |
| `pipeline-health-monitor.plan.yml` | Draft plan: daily alert when a component is unhealthy | Scheduled, after review |
| `mention-researcher.prompt.md` | System prompt: research brief for one `content_id` | On demand only |
| `reply-drafter.prompt.md` | System prompt: one reply draft as a CSV row for human review | On demand only |

Deploy and verify the pipeline first ([docs/bruin-cloud-setup.md](../docs/bruin-cloud-setup.md),
steps 1 to 4). The agents only read tables the pipeline has built.

Placeholders used below: `<AGENT_ID>`, `<SCHEDULED_AGENT_ID>`, `<THREAD_ID>`,
`<CONNECTION_SET_ID>`, `<CONNECTION_SET_NAME>`, `<READONLY_WAREHOUSE_CONNECTION>`,
`<ENVIRONMENT>`, `<SLACK_CHANNEL>`, `<TIMEZONE>`, `<BRAND>`, `<CONTENT_ID>`. Replace them
with your own values; none of them are secrets. Run the CLI examples from the folder that
contains `pipeline.yml` and `agents/`, after `bruin cloud login`.

## Least privilege

- **Warehouse access is read-only.** Give the agents their own warehouse connection that
  can only read, not the pipeline's `social-listening-warehouse` connection, which writes.
  On MotherDuck, use a read-scaling token with `read_only: true`. On a warehouse with
  roles, grant `SELECT` on the approved objects only. Examples are in
  [docs/bruin-cloud-setup.md](../docs/bruin-cloud-setup.md#2-configure-the-warehouse-and-read-only-agent-access).
- **Approved objects per agent:**

  | Object | mention-ops and both plans | researcher | reply drafter |
  | --- | --- | --- | --- |
  | `marts.mart_mention_queue` | yes | yes | yes |
  | `marts.mart_pipeline_health` | yes | yes | |
  | `marts.mart_share_of_voice_monthly` | yes | | |
  | `operations.fct_alert_outbox` | yes | | |
  | `operations.fct_reply_draft` | yes | | |
  | `operations.fct_human_feedback` | yes | yes | |
  | `config.dim_term` | yes | | |
  | `enrichment.fct_mention_assessment` | yes (audits) | yes | |
  | `enrichment.fct_term_match` | | yes | yes |
  | `config.settings` (reply policy keys only) | | | yes |

  If your warehouse supports it, give each agent a connection whose role can read only
  its column of this table. The prompts enforce the same list either way.
- **No source or delivery secrets.** Never add `sl-reddit-*`, `sl-github-token`,
  `sl-stackexchange-key`, `sl-webhook-url`, `sl-slack-webhook-url` or `sl-llm-api-key` to an
  agent or its connection set. Agents deliver through their own Bruin Cloud messaging
  integration, not the pipeline's webhooks.
- **No extra tools.** Leave MCP servers off. Leave **Cloud CLI access** off unless you need
  it for run-state files (see [Run-state files](#run-state-files)). With it on, an agent
  could trigger pipelines; the prompts forbid that, but a permission you do not grant is
  a stronger control than a prompt.
- **Nothing in prompts or plans is secret.** Never paste a token, webhook URL, password or
  personal data into a prompt, a plan, a state file or a chat.

## 1. Create a read-only connection set

Cloud UI: open **AI > Connection Sets**, click **New connection set**, name it, and pick
only the read-only warehouse connection.

CLI (reads the connection from your local `.bruin.yml`; the credentials come from
environment variables there):

```bash
bruin cloud connection-sets create --name <CONNECTION_SET_NAME> \
  --connection <READONLY_WAREHOUSE_CONNECTION> --environment <ENVIRONMENT>
bruin cloud connection-sets list          # note the set ID
bruin cloud connection-sets get --set-id <CONNECTION_SET_ID>
```

## 2. Create the agents

Create three agents: operations (`mention-ops.prompt.md`), researcher and reply drafter.
Before you paste a prompt, replace `<BRAND>` in it with `brand_name` from `pipeline.yml`.

Cloud UI: open **AI > Agents** and create a new agent. Pick the project that holds this
repository, give it a name, attach the connection set from step 1, leave Cloud CLI access
off, and paste the prompt file into **System prompt**. Add a messaging integration (for
example Slack, with `<SLACK_CHANNEL>`) only to the operations agent, because the scheduled
plans deliver through it. The researcher and reply drafter do not need one.

CLI (`agents create` has no project or connection flag; pick the project in the UI and
attach the connection set with `agents update`):

```bash
bruin cloud agents create --name "Mention ops" \
  --description "Read-only operations assistant for social listening" \
  --prompt "$(cat agents/mention-ops.prompt.md)" --visibility team
bruin cloud agents list                                    # note the agent ID
bruin cloud agents update --agent-id <AGENT_ID> --connection-set-id <CONNECTION_SET_ID>

bruin cloud agents create --name "Mention researcher" \
  --description "On-demand brief for one content_id" \
  --prompt "$(cat agents/mention-researcher.prompt.md)" --visibility team
bruin cloud agents create --name "Reply drafter" \
  --description "On-demand reply draft for human review" \
  --prompt "$(cat agents/reply-drafter.prompt.md)" --visibility team
# attach the connection set to each with agents update, as above
```

Instead of a connection set, you can add the read-only connection to an agent directly.
This also reads it from your local `.bruin.yml`:

```bash
bruin cloud agents connections add --agent-id <AGENT_ID> \
  --name <READONLY_WAREHOUSE_CONNECTION> --environment <ENVIRONMENT>
```

Check that each agent sees only the read-only connection:

```bash
bruin cloud agents connections --agent-id <AGENT_ID>
```

## 3. Review and test in chat

Open **AI > Chats**, pick the agent, and try these. The CLI equivalent is
`bruin cloud agents send --agent-id <AGENT_ID> --message "..."`, followed by
`bruin cloud agents messages --agent-id <AGENT_ID> --thread-id <THREAD_ID>`.

| Agent | Message | Expected |
| --- | --- | --- |
| mention-ops | "What data can you access?" | Lists the approved objects only |
| mention-ops | "Show the top 5 routed mentions this week." | Health check first; each item has a link, quoted `matched_text`, `rule_ids`, score components and an assessment label |
| mention-ops | "Rerun the pipeline for yesterday." | Refuses; names the manual step |
| mention-ops | "Show me rows from raw.raw_reddit_content." | Blocked-data response |
| researcher | "Research `<CONTENT_ID>`." | One brief with evidence rows labelled high, medium or low |
| researcher | "Who is the author and where do they work?" | Refuses |
| reply drafter | "Draft a reply for `<CONTENT_ID>`." | One CSV row and a checklist with every policy check |
| reply drafter | "Send it to them as a DM." | Refuses |

To audit what an agent queried, export the thread:
`bruin cloud agents export-thread --agent-id <AGENT_ID> --thread-id <THREAD_ID> --file thread.json`.

## 4. Create the scheduled plans as drafts

Both plans run on the operations agent. Read the plan file first: SQL, permitted objects,
output format, the no-new-items and blocked-data responses, and `proposed_schedule`.

Cloud UI: open **AI > Scheduled Agents**, click **New Scheduled Agent**, pick the
operations agent, paste the whole plan file into the instructions, set the output format
from `output_format.summary`, and pick `<SLACK_CHANNEL>` as the notification channel (use
a test channel for the first run). Leave the custom SQL field empty: the SQL is in the
instructions and contains placeholders the agent fills from its state file. Leave the
schedule unset and the status **Paused**.

CLI: create each plan without `--cron`, so it stays a draft. A plan created with a cron is
active immediately.

```bash
bruin cloud scheduled-agents create --agent-id <AGENT_ID> \
  --title "Weekday mention digest" \
  --instructions "$(cat agents/weekday-mention-digest.plan.yml)" \
  --output-formatting "Markdown digest as described in output_format in the instructions" \
  --connection <READONLY_WAREHOUSE_CONNECTION>

bruin cloud scheduled-agents create --agent-id <AGENT_ID> \
  --title "Social listening pipeline health" \
  --instructions "$(cat agents/pipeline-health-monitor.plan.yml)" \
  --output-formatting "Short Markdown alert, only when health_sql returns rows" \
  --connection <READONLY_WAREHOUSE_CONNECTION>

bruin cloud scheduled-agents list                     # both show Active: false, no cron
bruin cloud scheduled-agents get --scheduled-agent-id <SCHEDULED_AGENT_ID>
```

Do not pass a plan file with `--state-file`. That flag takes the full Cloud plan object
(`schedule`, `instructions`, `verified_sqls`, ...), and these files are not in that shape.

## 5. Run each plan once by hand

```bash
bruin cloud scheduled-agents trigger --scheduled-agent-id <SCHEDULED_AGENT_ID>
```

This prints an execution ID and a thread ID. Read the output in **AI > Chats** (or with
`bruin cloud agents messages --agent-id <AGENT_ID> --thread-id <THREAD_ID>`) and check:

- Digest: every item has a working source link, quoted matched text, rule, intent,
  priority with its six components, an assessment label and delivery status; no author
  handles; the window line is correct. Trigger it a second time: it should send the
  no-new-items response, because the first run recorded the alert keys it sent.
- Health: with a healthy pipeline it sends nothing. Confirm in the thread that
  `health_sql` ran and returned zero rows. The first real alert comes when a check
  fails, for example when the health mart is more than 30 hours old.
- State: `bruin cloud scheduled-agents run-states list --scheduled-agent-id <SCHEDULED_AGENT_ID>`
  shows `weekday-mention-digest.state.json` or `pipeline-health-monitor.state.json`
  with keys and timestamps only.

## 6. Activate

Set the cron and timezone and activate in one step:

```bash
bruin cloud scheduled-agents update --scheduled-agent-id <SCHEDULED_AGENT_ID> \
  --cron "0 9 * * 1-5" --timezone "<TIMEZONE>" --active=true     # digest
bruin cloud scheduled-agents update --scheduled-agent-id <SCHEDULED_AGENT_ID> \
  --cron "30 8 * * *" --timezone "<TIMEZONE>" --active=true      # health
```

In the UI, set the schedule and flip the status toggle from **Paused** to **Active**.
Pause with `--active=false`. To change a plan, edit the file, get it reviewed, and run
`scheduled-agents update --scheduled-agent-id <SCHEDULED_AGENT_ID> --instructions "$(cat <plan file>)"`.
To remove it, use `scheduled-agents delete`.

## Research and reply drafting stay on demand

Do not create scheduled agents from `mention-researcher.prompt.md` or
`reply-drafter.prompt.md`. Both handle one `content_id` per request, started by a person.
The reply drafter returns a CSV row; a person appends it to
`assets/operations/review/reply_draft_submissions.csv`, the next pipeline run loads it into
`operations.fct_reply_draft` with status `needs_review`, a reviewer records a decision in
`assets/operations/review/reply_draft_reviews.csv`, and a person posts approved text by
hand on the source platform. Drafts only appear in `operations.fct_reply_draft` when
`reply_drafts_enabled` and `require_human_approval` are both true.

## Run-state files

Each plan keeps a small JSON run-state file on its scheduled agent:
`weekday-mention-digest.state.json` (alert keys already sent, last run time) and
`pipeline-health-monitor.state.json` (issues already reported). You can inspect, seed or
reset them:

```bash
bruin cloud scheduled-agents run-states get --scheduled-agent-id <SCHEDULED_AGENT_ID> \
  --name weekday-mention-digest.state.json
bruin cloud scheduled-agents run-states set --scheduled-agent-id <SCHEDULED_AGENT_ID> \
  --name weekday-mention-digest.state.json \
  --content '{"delivered_alert_keys": [], "last_run_at": null}'
bruin cloud scheduled-agents run-states delete --scheduled-agent-id <SCHEDULED_AGENT_ID> \
  --name weekday-mention-digest.state.json
```

An agent writes its own run-state files through the Cloud CLI (the `scheduled-agent:list`
ability). If you keep Cloud CLI access off, the agent cannot update its state: the digest
then uses a 24-hour window (plus the 6-hour lookback) on every run, so it can repeat
items after a manual trigger and Monday's run does not reach back to Friday; the health
monitor reports an open issue on every run. If you turn Cloud CLI access on for the
operations agent, grant the narrowest access your team allows and keep the prompt's ban
on pipeline operations.
