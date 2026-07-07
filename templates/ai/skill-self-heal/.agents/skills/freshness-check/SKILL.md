# Freshness Check

## When to Use

Use this skill when data is stale, a scheduled run is missing, or freshness checks fail.

## Inputs

- Affected pipeline, asset, or freshness check.
- Expected schedule or freshness threshold.
- Last successful run time, if known.

## Operating Context

- These starter skills are primarily meant for AI agents configured inside Bruin Cloud.
- In Bruin Cloud, use Bruin Cloud MCP tools when available. If using the CLI, list recent runs with `bruin cloud runs list --project-id <project-id> --pipeline <pipeline-name>`, diagnose the latest run with `bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline-name> --latest`, and inspect failed logs with `bruin cloud instances failed-logs --project-id <project-id> --run-id <run-id>`.
- In local development, inspect terminal output and the local `logs/` folder, especially `logs/runs`, query logs, and export logs when they exist. Create local runs with `bruin run <path>` and explicit dates when needed.
- For other agent runtimes or orchestrators, customize this skill with the correct scheduler, log source, and run trigger mechanism before using it.

## Context to Gather

- Inspect pipeline schedules, asset dependencies, and freshness checks.
- Check recent run logs and whether upstream assets completed.
- Compare expected partitions or timestamps with the latest available data.
- Confirm timezone assumptions for schedules and freshness thresholds.
- Use Bruin MCP docs tools or `bruin <command> --help` to confirm the current command syntax before running Cloud or local CLI commands.

## Decision Tree

- If the pipeline did not run, inspect scheduler or CI status.
- If the run failed before the asset, diagnose the upstream blocker first.
- If the run succeeded but data is stale, inspect source availability and incremental filters.
- If timestamps look stale only in one timezone, verify timezone conversion and display logic.

## Actions

Define repository-specific actions here. Until customized, this skill must report findings and stop before triggering backfills, changing schedules, or modifying data.

## Verification

- Re-run the freshness check or equivalent query.
- Confirm the latest source and destination timestamps.
- Verify the expected schedule against the current date and timezone.

## Output

Return:

- Freshness status.
- Expected vs actual timestamps.
- Blocker category.
- Recommended next action.
- Commands or queries run.
