# Schema Drift Check

## When to Use

Use this skill when a pipeline fails because source, destination, or declared asset columns may have changed.

## Inputs

- Affected asset name or path.
- Error output mentioning missing columns, extra columns, type changes, or schema mismatch.
- Connection and environment names, if available.

## Operating Context

- These starter skills are primarily meant for AI agents configured inside Bruin Cloud.
- In Bruin Cloud, use Bruin Cloud MCP tools when available. If using the CLI, inspect runs with `bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline-name> --latest`, fetch run details with `bruin cloud runs get --project-id <project-id> --run-id <run-id>`, and fetch failed asset logs with `bruin cloud instances failed-logs --project-id <project-id> --run-id <run-id>`.
- In local development, inspect terminal output and the local `logs/` folder, especially `logs/runs`, query logs, and export logs when they exist. Use `bruin render <asset>`, `bruin validate <path>`, and `bruin query` with existing `.bruin.yml` connections for schema investigation.
- For other agent runtimes or orchestrators, customize this skill with the correct log source, schema inspection mechanism, and action mechanism before using it.

## Context to Gather

- Inspect the asset definition for declared columns and checks.
- Compare the rendered query or ingestion config with the current source schema.
- Check recent upstream changes, ingestion sync logs, and warehouse table metadata when available.
- Use Bruin MCP docs tools or `bruin <command> --help` to confirm the current command syntax before running Cloud or local CLI commands.
- Identify downstream assets that reference changed columns.

## Decision Tree

- If a column was added upstream, classify whether it is safe to ignore, declare, or propagate.
- If a column was removed or renamed, identify every asset that still references it.
- If a type changed, check whether casts or quality checks now fail.
- If only column order changed, verify whether the affected task depends on positional columns.

## Actions

Define repository-specific actions here. Until customized, this skill must report findings and stop before modifying schemas, source systems, warehouse tables, or repo files.

## Verification

- Re-run validation for the affected asset.
- Re-run the smallest schema inspection or render command used in diagnosis.
- Confirm whether downstream references are accounted for.

## Output

Return:

- Drift type: added, removed, renamed, type-changed, or unknown.
- Affected columns and assets.
- Recommended action.
- Verification commands and results.
