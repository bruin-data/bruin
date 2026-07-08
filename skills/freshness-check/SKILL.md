---
name: freshness-check
description: Use when data is stale, a scheduled run is missing, or freshness checks fail.
connections:
  - bruin
  - github
---

# Freshness Check

## When to Use

Use this skill when data is stale, a scheduled run is missing, or freshness checks fail.

## Inputs

- Affected pipeline, asset, or freshness check.
- Expected schedule or freshness threshold.
- Last successful run time, if known.

## Operating Context

- These starter skills can be used by Bruin Cloud agents, local agents, and external assistants connected to Bruin Cloud.
- In Bruin Cloud, use Cloud CLI access when the agent has it enabled. Use the `bruin cloud` CLI when the assistant has shell access and a configured API key or `.bruin.yml`; use Bruin Cloud MCP only when the assistant is configured for MCP tool calls or does not have direct CLI access. If using the CLI, list recent runs with `bruin cloud runs list --project-id <project-id> --pipeline <pipeline-name>`, diagnose the latest run with `bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline-name> --latest`, and inspect failed logs with `bruin cloud instances failed-logs --project-id <project-id> --run-id <run-id>`.
- In local development, inspect terminal output and the local `logs/` folder, especially `logs/runs`, query logs, and export logs when they exist. Create local runs with `bruin run <path>` and explicit dates when needed.
- If investigation or fix verification requires running an asset or pipeline, prefer a dev or shadow environment. If none exists, ask whether to run in production or create temporary copies of the affected tables to reproduce and test the issue.
- For other agent runtimes or orchestrators, customize this skill with the correct scheduler, log source, and run trigger mechanism before using it.

## Context to Gather

- Inspect pipeline schedules, asset dependencies, and freshness checks.
- Check recent run logs and whether upstream assets completed.
- Compare expected partitions or timestamps with the latest available data.
- Confirm timezone assumptions for schedules and freshness thresholds.
- Use Bruin MCP docs tools or `bruin <command> --help` to confirm the current command syntax before running Cloud or local CLI commands.

## Lineage Investigation

- Find one specific stale partition, timestamp, date, tenant, or key first, then keep every upstream query filtered to that instance.
- If the data has bronze, silver, gold, or other tiers, start at the stale asset and trace upstream through lineage one asset at a time.
- Query the filtered instance in each upstream asset until you find the first asset or source where the expected data is missing, late, or filtered out.
- Once the first stale asset is identified, read its SQL query or Python script and isolate the specific schedule dependency, incremental filter, date predicate, timezone conversion, source extraction, or function that likely caused the lag.
- If the user has allowed fixes, change only that specific logic, then run the smallest asset-level validation in dev or shadow first. Recheck the same stale instance after the fix; only after that passes, run the full freshness check or affected pipeline check.

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
