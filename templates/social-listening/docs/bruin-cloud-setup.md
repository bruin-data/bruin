# Bruin Cloud setup

1. Create or select a Cloud team and connect this template repository/project.
2. Configure the production warehouse and grant agents read access only to `marts.mart_mention_queue`, `marts.mart_pipeline_health`, and explicitly approved views.
3. Add source and delivery secrets through Cloud Secrets or the selected secret backend. Never place secrets in variables, prompts, plans, state files or examples.
4. Deploy, run a bounded dry-run backfill, then verify the queue, health mart and outbox before delivery is enabled.
5. Create the operational agent from `agents/mention-ops.prompt.md`; attach only the approved read-only connection set.
6. Create digest and health schedules as drafts, review SQL/output/timezone, run each once manually, then activate them.
7. Keep research and reply drafting on-demand. A human reviews every draft and posts manually on the source platform.

Placeholder-only CLI examples:

```shell
bruin cloud agents create --name <agent-name> --prompt-file agents/mention-ops.prompt.md
bruin cloud agents connections add --agent <agent-name> --connection <read-only-warehouse-connection>
bruin cloud scheduled-agents create --name <schedule-name> --plan-file agents/weekday-mention-digest.plan.yml
```

The Cloud UI follows the same order: create agent, attach its read-only connection, save the schedule as a draft, test, review, and activate. Verify command flags against your installed CLI before use.
