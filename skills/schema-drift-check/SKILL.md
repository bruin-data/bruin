---
name: schema-drift-check
description: Use when a pipeline fails because source, destination, or declared asset columns may have changed.
connections:
  - bruin
  - github
---

# Schema Drift Check

## When to Use

Use this skill when a pipeline fails because source, destination, or declared asset columns may have changed.

## Inputs

- Affected asset name or path.
- Error output mentioning missing columns, extra columns, type changes, or schema mismatch.
- Connection and environment names, if available.

## Operating Context

- These starter skills can be used by Bruin Cloud agents, local agents, and external assistants connected to Bruin Cloud.
- In Bruin Cloud, use Cloud CLI access when the agent has it enabled. Use the `bruin cloud` CLI when the assistant has shell access and a configured API key or `.bruin.yml`; use Bruin Cloud MCP only when the assistant is configured for MCP tool calls or does not have direct CLI access. If using the CLI, inspect runs with `bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline-name> --latest`, fetch run details with `bruin cloud runs get --project-id <project-id> --run-id <run-id>`, and fetch failed asset logs with `bruin cloud instances failed-logs --project-id <project-id> --run-id <run-id>`.
- In local development, inspect terminal output and the local `logs/` folder, especially `logs/runs`, query logs, and export logs when they exist. Use `bruin render <asset>`, `bruin validate <path>`, and `bruin query` with existing `.bruin.yml` connections for schema investigation.
- If investigation or fix verification requires running an asset or pipeline, prefer a dev or shadow environment. If none exists, ask whether to run in production or create temporary copies of the affected tables to reproduce and test the issue.
- For other agent runtimes or orchestrators, customize this skill with the correct log source, schema inspection mechanism, and action mechanism before using it.

## Context to Gather

- Inspect the asset definition for declared columns and checks.
- Compare the rendered query or ingestion config with the current source schema.
- Check recent upstream changes, ingestion sync logs, and warehouse table metadata when available.
- Use Bruin MCP docs tools or `bruin <command> --help` to confirm the current command syntax before running Cloud or local CLI commands.
- Identify downstream assets that reference changed columns.

## Lineage Investigation

- Find one specific missing, renamed, type-changed, or malformed column example first, then keep every upstream schema or data query scoped to the affected table, column, partition, or row.
- If the data has bronze, silver, gold, or other tiers, start at the failing asset and trace upstream through lineage one asset at a time.
- Compare the filtered data and schema metadata in each upstream asset until you find the first asset or source where the schema drift appears.
- Once the first drifted asset is identified, read its SQL query, ingestion config, or Python script and isolate the specific select list, cast, rename, source mapping, union, model contract, or function that likely introduced the mismatch.
- If the user has allowed fixes, change only that specific logic, then run the smallest render, schema inspection, or asset-level validation in dev or shadow first. Recheck the same column example after the fix; only after that passes, run the broader schema validation.

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

## Testing This Skill

- Use the local self-heal fixture from the Bruin `init` command docs.
- Run the schema-drift scenario and verify the agent identifies that `bronze_orders.amount` was renamed to `gross_amount` while `silver_orders` still references `amount`.
- If fixes are allowed, verify the agent updates only the affected select list or mapping, then runs render/schema validation before broader checks.

## Output

Return:

- Drift type: added, removed, renamed, type-changed, or unknown.
- Affected columns and assets.
- Recommended action.
- Verification commands and results.
