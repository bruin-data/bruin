---
name: pipeline-diagnose
description: Use when a Bruin pipeline, asset, or command fails and the cause is not yet clear.
connections:
  - bruin
  - github
---

# Pipeline Diagnose

## When to Use

Use this skill when a Bruin pipeline, asset, or command fails and the cause is not yet clear.

## Inputs

- Failing command and full error output.
- Pipeline or asset path.
- Environment name, if one was used.
- Recent code or configuration changes, if known.

## Operating Context

- These starter skills can be used by Bruin Cloud agents, local agents, and external assistants connected to Bruin Cloud.
- In Bruin Cloud, use Cloud CLI access when the agent has it enabled. Use the `bruin cloud` CLI when the assistant has shell access and a configured API key or `.bruin.yml`; use Bruin Cloud MCP only when the assistant is configured for MCP tool calls or does not have direct CLI access. If using the CLI, prefer `bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline-name> --latest`, `bruin cloud runs get --project-id <project-id> --run-id <run-id>`, `bruin cloud instances logs --project-id <project-id> --run-id <run-id> --asset <asset-name>`, and `bruin cloud instances failed-logs --project-id <project-id> --run-id <run-id>` for logs and run context.
- In local development, inspect terminal output and the local `logs/` folder, especially `logs/runs`, query logs, and export logs when they exist. Create local runs with `bruin run <path>` rather than Bruin Cloud run commands.
- If investigation or fix verification requires running an asset or pipeline, prefer a dev or shadow environment. If none exists, ask whether to run in production or create temporary copies of the affected tables to reproduce and test the issue.
- For other agent runtimes or orchestrators, customize this skill with the correct log source and action mechanism before using it to read logs or trigger changes.

## Context to Gather

- Run `bruin validate <path>` for the affected pipeline or asset.
- Check `pipeline.yml`, asset definitions, and connection names referenced by the failing task.
- Inspect recent logs, stack traces, and changed files.
- Use Bruin MCP docs tools or `bruin <command> --help` to confirm the current command syntax before running Cloud or local CLI commands.
- Confirm whether the failure is parse-time, compile-time, connection-time, or execution-time.

## Data Failure Investigation

- If the failure is caused by bad data and there are bronze, silver, gold, or other tiers, start at the asset where the problem appears and trace upstream through lineage one asset at a time.
- Find one specific failing row, key, partition, or timestamp first, then keep every upstream query filtered to that instance.
- Query the filtered instance in upstream assets until you find the first asset where the problem appears.
- Once the first bad asset is identified, read its SQL query or Python script and isolate the specific function, join, filter, cast, incremental condition, or transformation step that likely caused the problem.
- If the user has allowed fixes, change only that specific logic, then run the smallest asset-level validation in dev or shadow first. Recheck the same failing instance after the fix; only after that passes, run the broader failing command or check.

## Decision Tree

- If validation fails, fix the configuration or asset definition first.
- If the connection is missing or invalid, report the missing connection and required fields.
- If rendering fails, inspect Jinja variables, macros, and included files.
- If execution fails after rendering, isolate the failing query or script and summarize the warehouse/runtime error.

## Actions

Define repository-specific actions here. Until customized, this skill must report findings and stop before modifying data, source systems, or repo files.

## Verification

- Re-run the smallest failing command.
- Run `bruin validate <path>` when files were changed.
- Capture the final command output or remaining error.

## Testing This Skill

- Use the local self-heal fixture from the Bruin `init` command docs.
- Run the pipeline-diagnose scenario and verify the agent classifies the failure, reads the affected asset, and identifies the exact missing table reference or broken SQL step.
- If fixes are allowed, verify the agent changes only the isolated failing reference, reruns the smallest failing command, then runs validation.

## Output

Return a concise diagnosis with:

- Root cause or strongest hypothesis.
- Evidence used.
- Recommended next action.
- Commands run and their result.
