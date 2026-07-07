# Pipeline Diagnose

## When to Use

Use this skill when a Bruin pipeline, asset, or command fails and the cause is not yet clear.

## Inputs

- Failing command and full error output.
- Pipeline or asset path.
- Environment name, if one was used.
- Recent code or configuration changes, if known.

## Operating Context

- These starter skills are primarily meant for AI agents configured inside Bruin Cloud.
- In Bruin Cloud, use Bruin Cloud MCP tools when available. If using the CLI, prefer `bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline-name> --latest`, `bruin cloud runs get --project-id <project-id> --run-id <run-id>`, `bruin cloud instances logs --project-id <project-id> --run-id <run-id> --asset <asset-name>`, and `bruin cloud instances failed-logs --project-id <project-id> --run-id <run-id>` for logs and run context.
- In local development, inspect terminal output and the local `logs/` folder, especially `logs/runs`, query logs, and export logs when they exist. Create local runs with `bruin run <path>` rather than Bruin Cloud run commands.
- For other agent runtimes or orchestrators, customize this skill with the correct log source and action mechanism before using it to read logs or trigger changes.

## Context to Gather

- Run `bruin validate <path>` for the affected pipeline or asset.
- Check `pipeline.yml`, asset definitions, and connection names referenced by the failing task.
- Inspect recent logs, stack traces, and changed files.
- Use Bruin MCP docs tools or `bruin <command> --help` to confirm the current command syntax before running Cloud or local CLI commands.
- Confirm whether the failure is parse-time, compile-time, connection-time, or execution-time.

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

## Output

Return a concise diagnosis with:

- Root cause or strongest hypothesis.
- Evidence used.
- Recommended next action.
- Commands run and their result.
