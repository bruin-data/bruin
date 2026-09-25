# Cloud agent package

These are version-controlled draft definitions, not activated agents. Follow `../docs/bruin-cloud-setup.md`: connect the project, grant an agent only approved warehouse read access, store source/delivery secrets in Cloud Secrets, deploy a bounded dry-run, and verify the queue/health/outbox.

Create the operations agent from `mention-ops.prompt.md`, then attach its read-only connection. Create the weekday digest and health monitor from their plans as drafts, review timezone/SQL/output, manually test each once, and only then activate. Keep research and reply-drafting on-demand; every response is reviewed and posted manually by a human.

```shell
bruin cloud agents create --name <agent-name> --prompt-file agents/mention-ops.prompt.md
bruin cloud agents connections add --agent <agent-name> --connection <read-only-warehouse-connection>
bruin cloud scheduled-agents create --name <schedule-name> --plan-file agents/weekday-mention-digest.plan.yml
```

Use Cloud UI equivalents if preferred. Never include a real team, connection, channel, identifier, or token in a prompt, plan, state file, or configuration variable.
