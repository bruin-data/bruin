# Bruin Cloud setup

This runbook takes the social listening template from a repository to a running Bruin
Cloud deployment with optional read-only agents. Follow the steps in order. Each step
ends with something to check before you move on.

Installing the template changes nothing in Bruin Cloud. The pipeline has no schedule,
delivery starts in dry-run mode, and no agent is created or activated until you do it
yourself in steps 5 and 6.

Related: [template README](../README.md) and [agents/README.md](../agents/README.md).

Placeholders: `<TEAM>`, `<PROJECT_ID>`, `<AGENT_ID>`, `<SCHEDULED_AGENT_ID>`,
`<CONNECTION_SET_ID>`, `<CONNECTION_SET_NAME>`, `<READONLY_WAREHOUSE_CONNECTION>`,
`<AGENT_READONLY_ROLE>`, `<SLACK_CHANNEL>`, `<TIMEZONE>`, `<YYYY-MM-DD>`. None of them are
secrets. Run the CLI examples from the folder that contains `pipeline.yml`.

## 1. Create or select a Cloud team and connect the repository

1. Sign in: `bruin cloud login`. If your account belongs to several teams, list them with
   `bruin cloud teams list` and set a default with `bruin cloud config set-team <TEAM>`.
2. In Bruin Cloud, open **Team Settings > Projects > New project**, connect GitHub (the
   Bruin GitHub App is recommended), pick the repository that contains this template, and
   create the project.
3. Wait for the sync to finish.

Check: `bruin cloud projects list` shows the project, and **Catalog > Pipelines** lists
`social-listening` as disabled.

## 2. Configure the warehouse and read-only agent access

**Pipeline connection.** The documented cloud path is MotherDuck, under the connection
name `social-listening-warehouse` (see the `production` environment in
[`.bruin.yml.example`](../.bruin.yml.example)). MotherDuck runs the template's DuckDB SQL
unchanged. Another warehouse needs the macros in `macros/social_listening.sql` ported to
its dialect and `default.type` changed in `pipeline.yml` first.

Create the connection in Cloud with the same name, either in the UI (team menu >
**Connections > New connection**, type MotherDuck) or from your local `.bruin.yml`, where
the token comes from an environment variable:

```bash
bruin cloud connections add --name social-listening-warehouse --environment production
```

This connection can write. Only the pipeline uses it. Never attach it to an agent.

**Agent connection.** Give agents a separate connection that can only read:

- MotherDuck: a read-scaling token with `read_only: true`. Add it to your local
  `.bruin.yml` under the `production` environment:

  ```yaml
  motherduck:
    - name: <READONLY_WAREHOUSE_CONNECTION>
      token: "${MOTHERDUCK_READ_SCALING_TOKEN}"
      database: social_listening
      read_only: true
  ```

  A MotherDuck token cannot be limited to certain schemas: it can read everything the
  account can read, including `raw.*` and `staging.*`. With MotherDuck, the approved-object
  list in each prompt is the control on which tables the agent reads. If the agent must
  be technically unable to read raw content, use a warehouse with object-level grants,
  or a separate database that holds only copies of the approved objects.

- A warehouse with roles (after porting the macros): create a read-only role and grant
  `SELECT` on the approved objects only. For example, in PostgreSQL syntax:

  ```sql
  CREATE ROLE <AGENT_READONLY_ROLE> LOGIN;  -- set its password through your secrets process
  GRANT USAGE ON SCHEMA marts, operations, config, enrichment TO <AGENT_READONLY_ROLE>;
  GRANT SELECT ON
      marts.mart_mention_queue,
      marts.mart_pipeline_health,
      marts.mart_share_of_voice_monthly,
      operations.fct_alert_outbox,
      operations.fct_reply_draft,
      operations.fct_human_feedback,
      config.dim_term,
      config.settings,                    -- reply drafter only (reply policy keys)
      enrichment.fct_mention_assessment,
      enrichment.fct_term_match           -- researcher and reply drafter only
  TO <AGENT_READONLY_ROLE>;
  ```

  Grant nothing on `raw`, `staging`, `operations.alert_delivery_attempt`,
  `operations.review_*` or `config.redaction_request`, and grant no `INSERT`, `UPDATE`,
  `DELETE` or `CREATE`. The marts are rebuilt as tables on each run, which can drop object
  grants on some warehouses. Re-apply the grants after each run, or expose the approved
  objects as views in a schema that only this role can read. Schema-wide default or
  future grants would also cover the non-approved tables in the same schemas.

Check: `bruin query --connection social-listening-warehouse --environment production --query "SELECT 1"`
succeeds locally, and a write through the read-only connection fails.

## 3. Add source and delivery secrets

Create each secret the pipeline uses as a Cloud **generic secret** connection with exactly
these names. Assets look them up by name.

| Connection name | Needed when |
| --- | --- |
| `sl-reddit-client-id` | `reddit_enabled` is true |
| `sl-reddit-client-secret` | `reddit_enabled` is true |
| `sl-reddit-user-agent` | `reddit_enabled` is true |
| `sl-github-token` | `github_enabled` is true (public-repository read only) |
| `sl-stackexchange-key` | optional, raises the Stack Exchange quota |
| `sl-webhook-url` | `notification_destination` is `webhook` |
| `sl-slack-webhook-url` | `notification_destination` is `slack_webhook` |
| `sl-llm-api-key` | `llm_enabled` is true with a real provider |

Use the UI (team menu > **Connections > New connection > generic secret**), or push them
from your local `.bruin.yml`, where each value comes from an environment variable:

```bash
bruin cloud connections add --name sl-reddit-client-id --environment production
bruin cloud connections list    # names and types only
```

Secrets go only into Cloud connections (or your secrets backend). Never put one in a
prompt, a plan, a run-state file, `.bruin.yml.example`, `pipeline.yml` or a pipeline custom
variable, and never add one to an agent or connection set.

Check: the pipeline page in Cloud no longer flags missing connections for the sources you
enabled. The `config.settings` asset also stops a run early if an enabled source is
missing its secret.

## 4. Deploy, run a bounded dry-run backfill, verify, then enable delivery

1. **Set production defaults in `pipeline.yml` and commit them.** Scheduled runs use these
   defaults, and the template ships with demo values. At minimum set `demo_mode: false`,
   keep `notification_dry_run: true`, and set the brand, terms and sources you use.
   Push, and let Cloud sync.

2. **Run a bounded dry-run backfill** over one or two recent days. The window must be no
   longer than `source_window_max_days` (default 7) or `config.settings` rejects the run.

   ```bash
   bruin cloud runs trigger --project-id <PROJECT_ID> --pipeline social-listening \
     --start-date <YYYY-MM-DD> --end-date <YYYY-MM-DD> \
     --var demo_mode=false --var notification_dry_run=true \
     --note "dry-run backfill before enabling delivery"
   ```

   Follow it with `bruin cloud runs list --project-id <PROJECT_ID> --pipeline social-listening`
   or on the pipeline page. If Cloud will not run the pipeline while it is disabled,
   enable it first; item 5 below explains what enabling does.

3. **Verify the output** with `bruin query --connection social-listening-warehouse --environment production --query "<SQL>"`
   (or any SQL client on the warehouse):

   ```sql
   -- Settings used by the last run: expect demo_mode false, notification_dry_run true
   SELECT json_extract(settings_json, '$.demo_mode') AS demo_mode,
          json_extract(settings_json, '$.notification_dry_run') AS notification_dry_run,
          validated_at
   FROM config.settings ORDER BY validated_at DESC LIMIT 1;

   -- Queue: counts per state, and the top routed items to spot-check by hand
   SELECT queue_state, COUNT(*) AS items, MAX(published_at) AS latest_published
   FROM marts.mart_mention_queue GROUP BY queue_state ORDER BY queue_state;

   SELECT source_url, matched_text, rule_ids, intent, ROUND(priority, 2) AS priority,
          assessment_status, delivery_status
   FROM marts.mart_mention_queue
   WHERE queue_state = 'routed'
   ORDER BY priority DESC LIMIT 10;

   -- Health: every enabled source ok; delivery shows last_run_status = 'dry_run'
   SELECT component, health_status, health_reasons_json, last_run_status,
          last_success_at, data_as_of
   FROM marts.mart_pipeline_health ORDER BY component;

   -- Outbox: only dry_run (or expired) rows, nothing delivered yet
   SELECT destination, status, COUNT(*) AS alerts
   FROM operations.fct_alert_outbox GROUP BY destination, status ORDER BY destination, status;
   ```

   Open several routed `source_url` values and check that the matches are real. If there
   are false positives, adjust `term_rules`, `excluded_terms` or the thresholds before
   going further.

4. **Enable delivery.** Set `notification_dry_run: false` in `pipeline.yml` and commit.
   On the first live run, alerts recorded as `dry_run` whose content is newer than
   `alert_max_age_hours` (default 72) are sent, at most `max_alerts_per_run` (default 20)
   per run. Older ones are marked `expired` and never sent.

5. **Scheduling is opt-in.** `pipeline.yml` has no `schedule`, so Cloud does not run the
   pipeline on its own. Add one (for example `schedule: "@hourly"` or a cron), commit, and
   enable the pipeline in **Catalog > Pipelines** or with
   `bruin cloud pipelines enable --project-id <PROJECT_ID> --pipeline social-listening`.
   Enabling a pipeline can start its first run straight away, so do this only after
   step 4.1. Assets are tagged `collect`, `enrich` and `report`; to run those slices on
   their own cadence, use `bruin run --tag collect` (and so on) from your own scheduler.
   In Cloud, `bruin cloud runs trigger --tag` only labels a run and does not select
   assets; select assets with `--asset` (and `--downstream`) instead.

Check: the outbox shows `delivered` rows after the first live run, and the delivery row in
`marts.mart_pipeline_health` shows `last_run_status = 'live'` and `health_status = 'ok'`.

## 5. Create the operational agent

Create the agent from [`agents/mention-ops.prompt.md`](../agents/mention-ops.prompt.md),
with `<BRAND>` replaced. Attach only the read-only warehouse connection from step 2, on
its own or in a connection set. Leave Cloud CLI access and MCP servers off. Add a
messaging integration only if you will schedule plans on this agent.

```bash
bruin cloud agents create --name "Mention ops" \
  --description "Read-only operations assistant for social listening" \
  --prompt "$(cat agents/mention-ops.prompt.md)" --visibility team
bruin cloud connection-sets create --name <CONNECTION_SET_NAME> \
  --connection <READONLY_WAREHOUSE_CONNECTION> --environment production
bruin cloud agents update --agent-id <AGENT_ID> --connection-set-id <CONNECTION_SET_ID>
bruin cloud agents connections --agent-id <AGENT_ID>   # expect only the read-only connection
```

Test it in **AI > Chats** with the prompts in
[agents/README.md](../agents/README.md#3-review-and-test-in-chat).

## 6. Create the digest and health plans as drafts, test, then activate

1. Read [`agents/weekday-mention-digest.plan.yml`](../agents/weekday-mention-digest.plan.yml)
   and [`agents/pipeline-health-monitor.plan.yml`](../agents/pipeline-health-monitor.plan.yml):
   the SQL, permitted objects, output format, timezone, delivery target and the
   no-new-items and blocked-data responses. Set `late_arrival_hours` in the digest SQL to
   the value in `pipeline.yml` if you changed it.
2. Create each plan **without** a cron. A plan with a cron is active as soon as it is
   created; without one it stays a draft.

   ```bash
   bruin cloud scheduled-agents create --agent-id <AGENT_ID> \
     --title "Weekday mention digest" \
     --instructions "$(cat agents/weekday-mention-digest.plan.yml)" \
     --output-formatting "Markdown digest as described in output_format in the instructions" \
     --connection <READONLY_WAREHOUSE_CONNECTION>
   ```

   Repeat for the health plan. In the UI, leave the schedule empty and the status
   **Paused**, and point the notification at a test channel first.
3. Run each one once by hand and read the result:

   ```bash
   bruin cloud scheduled-agents trigger --scheduled-agent-id <SCHEDULED_AGENT_ID>
   ```

4. Activate with the reviewed cron and timezone:

   ```bash
   bruin cloud scheduled-agents update --scheduled-agent-id <SCHEDULED_AGENT_ID> \
     --cron "0 9 * * 1-5" --timezone "<TIMEZONE>" --active=true
   ```

   Use `"30 8 * * *"` for the health plan. Switch the notification to `<SLACK_CHANNEL>`.

Details, including the run-state files, are in [agents/README.md](../agents/README.md).

## 7. Keep research and reply drafting on demand

Create the researcher and reply drafter from
[`agents/mention-researcher.prompt.md`](../agents/mention-researcher.prompt.md) and
[`agents/reply-drafter.prompt.md`](../agents/reply-drafter.prompt.md) and use them in chat,
one `content_id` at a time. Do not schedule them.

A person reviews every reply draft. The flow is: the drafter returns one CSV row; a person
appends it to `assets/operations/review/reply_draft_submissions.csv` and commits it; the
next run loads it into `operations.fct_reply_draft` with status `needs_review` (only when
`reply_drafts_enabled` and `require_human_approval` are both true); a reviewer records a
decision in `assets/operations/review/reply_draft_reviews.csv`; and a person posts approved
text by hand on the source platform. Nothing in the template or the agents posts.

## Troubleshooting

| Symptom | Likely cause and fix |
| --- | --- |
| The run stops at `config.settings` | An invalid variable, a window longer than `source_window_max_days`, or an enabled source without its secret. The asset's log names the problem. |
| Demo items appear in production | `demo_mode` was still true. Set it to false in `pipeline.yml`; remove the demo rows with a full refresh of the affected assets. |
| Nothing is delivered | `notification_dry_run` is still true, or `notification_destination` is `none`. The outbox shows `dry_run` or `skipped_no_destination`. |
| Outbox rows are `failed_permanent` | The webhook secret is wrong or the receiver rejects the payload. Check `last_http_status` in `operations.fct_alert_outbox` and the delivery secret. |
| The agent cannot see the tables | Wrong connection set, or the read-only token cannot read the `social_listening` database. Check `bruin cloud agents connections --agent-id <AGENT_ID>`. |
| A plan went active on create | It was created with `--cron`. Pause it with `scheduled-agents update --scheduled-agent-id <SCHEDULED_AGENT_ID> --active=false`. |
| `--active=true` is rejected | Activation needs a cron (or a pipeline trigger). Pass `--cron` in the same command. |
| `trigger` is refused | A run of that scheduled agent is already in progress. Wait for it to finish. |
| The digest repeats items | The agent cannot write its run-state file. Check `scheduled-agents run-states list`; see [agents/README.md](../agents/README.md#run-state-files). |
| The digest says data is stale | The pipeline has not rebuilt the marts in 30 hours, or a source is stale. Check the latest run in Cloud. |
| A submitted draft does not appear | `reply_drafts_enabled` is false, or `source` and `external_id` in the CSV row do not match an item in staging. |

## What the agents can never do

- Post, reply, comment, vote, follow, or send DMs or emails on any platform.
- Create or update CRM, ticketing or marketing records.
- Trigger, rerun, backfill or mark pipeline runs, or change schedules, connections,
  variables or secrets. They can only name the manual step for a person.
- Write to any warehouse table. The only thing a scheduled plan writes is its own
  run-state file of keys and timestamps.
- Read objects outside their approved list, or private data of any kind.
- Identify, profile or enrich authors, or build lists of people.
- Publish a reply draft. Drafts stay `needs_review` until a person decides, and a person
  posts approved text by hand.
