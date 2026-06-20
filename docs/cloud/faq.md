---
outline: deep
---

# FAQ

Short answers to common Bruin Cloud questions, including patterns that look plausible but aren't real features. If you're using an AI assistant against the Bruin Cloud docs and it suggests one of the "what does not exist" items below, treat that as a hallucination.

## Pipelines and scheduling

### Can I skip a single asset from scheduled runs?

Yes. Set [`enabled: false`](/assets/definition-schema#enabled) in the asset definition. Bruin marks that asset as skipped instead of executing it, and downstream assets can continue running. You can also template this value, for example `enabled: "{{ var.asset_enabled }}"`, as long as it renders to `true` or `false`.

Other supported patterns are:

- **One-off:** mark the asset as successful before the run starts. Open the pipeline page, find the asset for the next interval, and mark it successful. Downstream assets treat it as completed.
- **Different cadence:** move the asset to its own pipeline. Give it a different schedule, or leave that pipeline disabled and trigger it manually with **New run**. See [Pipelines](/cloud/pipelines#enable-a-pipeline).

### Can I run different assets on different schedules within one pipeline?

No. A pipeline has one schedule, and every asset in it runs on that schedule. If you need multiple cadences, split the assets across pipelines, one per cadence. See [Pipelines](/cloud/pipelines) for how to enable each one separately.

### Can I pass CLI flags to a scheduled run?

No. Bruin Cloud's scheduled runs use the pipeline's own configuration. CLI flags like [`--tag`, `--exclude-tag`, `--only`, and `--full-refresh`](/commands/run#focused-runs-filtering-by-tags-and-execution-types) apply to ad-hoc `bruin run` invocations on your machine, not to scheduled runs. For full refreshes in cloud, use the **New run** dialog and toggle full refresh there.

## Connections

### Why is my pipeline showing a "missing connection"?

The pipeline references a connection name that is not configured in Bruin Cloud, or the configured name does not match exactly. Connection names are case-sensitive and must match the name in your pipeline's YAML. See [Connections](/cloud/connections).

## AI Agents

### Does the agent see my actual data?

Yes. To answer questions, build charts, or complete tasks against your warehouse, the agent has to query the actual data. Schemas alone aren't enough. Two things to know about access and retention:

- **Only members of your organization can access it.** Bruin employees cannot read the data your agent queries or the conversations it generates.
- **Data is retained for the duration of the agent session only.** Query results, intermediate context, and the sandbox itself are discarded when the session ends. Outputs the agent posted to a chat integration (Slack message, PDF report, etc.) live wherever you sent them.

You control the scope of what the agent can read through its attached [connection set](/cloud/connections#connection-sets-for-ai-agents). Use a read-only role, scoped schemas, or masked columns if you want to tighten access further.

### Can one agent talk to multiple chat platforms at the same time?

A single channel can only be connected to one agent. One Slack channel maps to exactly one agent, one Teams channel to one agent, and so on. If you want different behavior in different channels, create separate agents and wire each one to its own channel. See the [Integrations overview](/cloud/integrations/overview) for how this works on each platform.

### My scheduled agent has a Slack integration. Where do the runs show up?

In both places. The Slack channel receives the output, and the run is also recorded under **AI → Chats** where you can review the conversation, the queries it ran, and any generated files. See [Scheduled Agents](/cloud/ai-agents/scheduled).

### What's the difference between a scheduled pipeline and a scheduled agent?

A **scheduled pipeline** is configured in your repo (the `schedule:` field on `pipeline.yml`) and runs the assets defined in that pipeline on the cadence you set. See [Pipelines](/cloud/pipelines).

A **scheduled agent** is an existing AI agent set to run on a cron — it executes a natural-language task (and optional verified SQL) and posts the result to a chat integration or the Bruin Cloud chat. See [Scheduled Agents](/cloud/ai-agents/scheduled).

They're stored in different places (pipeline YAML vs. the AI Agents settings page) and they do different things. The **AI → Scheduled Agents** menu only contains scheduled *agents*; pipeline schedules live on the pipeline itself.

## See also

- [Pipelines](/cloud/pipelines) for the pipeline page and run controls.
- [Connections](/cloud/connections) for connection types, naming, and validation.
- [AI Agents](/cloud/ai-agents/overview) for agent setup, chat, and scheduling.
- [Integrations](/cloud/integrations/overview) for connecting agents to Slack, Teams, Google Chat, Discord, WhatsApp, and Telegram.

For the local CLI side of these features:

- [Project structure](/core-concepts/project), [Pipeline definition](/pipelines/definition), [Asset definition schema](/assets/definition-schema).
- [`bruin run`](/commands/run), [`bruin validate`](/commands/validate), [`bruin query`](/commands/query), [`bruin render`](/commands/render), [`bruin ai-enhance`](/commands/ai-enhance).
