# Lesson 13: logs-history-and-the-bill

## Objectives
- Locate structured Bruin run logs and validation output.
- Name the read-only Cloud MCP observability surfaces.
- Explain what warehouse-side history adds beyond DuckDB.

## Concepts to teach
Bruin stores run evidence under `logs/runs/<pipeline>/<run-id>.json`, relative to the Git project
root (the directory containing `.bruin.yml`), not necessarily beside the generated pipeline.
`bruin validate pipeline --output json` is useful in CI. A Cloud agent can read run history, asset
instances, logs, and validation errors through the read-only Cloud MCP tools
`pipeline-run-list`, `asset-instance-list`, `asset-instance-logs`, `asset-runs`, and
`validation-error-list`, making structured triage a safe agent task.

DuckDB has no equivalent billing or shared query-history service. BigQuery and Snowflake expose query text, user, permissions, timing, bytes or credits, and cost through warehouse-side history. Treat cost claims as platform-specific evidence.

## Quiz
1. Q: What belongs in a run log?
   A: Structured run and asset status, timing, errors, and validation information.
2. Q: Why is read-only run-history access useful to an agent?
   A: Triage is pattern matching over structured text and does not need write access.
3. Q: Does local DuckDB provide a warehouse bill?
   A: No. Cost and query-history evidence must come from the target warehouse.

## Task
Write `docs/observability.md` describing the local log path, a JSON validation command, the Cloud MCP read surfaces, and the warehouse-side BigQuery and Snowflake evidence you would inspect. Include one read-only triage query command with `--description`.

## Rubric (for `review my work`)
- [ ] Includes the project-root-relative path `logs/runs/<pipeline>/<run-id>.json` and `bruin validate pipeline --output json`.
- [ ] Names run history, asset instances, logs, and validation errors as Cloud read surfaces.
- [ ] Names the read-only Cloud MCP tools that expose those surfaces.
- [ ] Distinguishes DuckDB from BigQuery/Snowflake query history and cost evidence.

## Done signal
You can hand an agent enough structured evidence to diagnose safely. Carry forward: the capstone combines the controls into a governed pipeline.
