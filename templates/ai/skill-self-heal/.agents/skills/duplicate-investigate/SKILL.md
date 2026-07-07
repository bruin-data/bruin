# Duplicate Investigate

## When to Use

Use this skill when duplicate rows, unstable primary keys, repeated ingestion, or failed uniqueness checks appear in a Bruin asset.

## Inputs

- Affected asset name or table.
- Key columns or uniqueness checks.
- Error output, sample duplicate keys, or row counts.

## Operating Context

- These starter skills are primarily meant for AI agents configured inside Bruin Cloud.
- In Bruin Cloud, use Bruin Cloud MCP tools when available. If using the CLI, inspect failures with `bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline-name> --latest`, fetch failed logs with `bruin cloud instances failed-logs --project-id <project-id> --run-id <run-id>`, and inspect a specific asset with `bruin cloud assets get --project-id <project-id> --pipeline <pipeline-name> --asset <asset-name>`.
- In local development, inspect terminal output and the local `logs/` folder, especially `logs/runs`, query logs, and export logs when they exist. Use `bruin query` with existing `.bruin.yml` connections for read-only duplicate checks.
- For other agent runtimes or orchestrators, customize this skill with the correct log source, query mechanism, and action mechanism before using it.

## Context to Gather

- Inspect quality checks and declared primary or unique keys.
- Find the ingestion or transformation logic that creates the affected rows.
- Compare source row counts, destination row counts, and recent run history when available.
- Check whether incremental logic can replay the same source records.
- Use Bruin MCP docs tools or `bruin <command> --help` to confirm the current command syntax before running Cloud or local CLI commands.

## Decision Tree

- If duplicates share the same business key and timestamp, suspect replay or missing deduplication.
- If duplicates differ only by load metadata, inspect incremental merge logic.
- If the declared key is incomplete, identify additional columns needed for uniqueness.
- If duplicates originate upstream, report the source and avoid masking the issue silently.

## Actions

Define repository-specific actions here. Until customized, this skill must report findings and stop before deleting rows, changing deduplication logic, or modifying source systems.

## Verification

- Re-run the duplicate detection query.
- Re-run the affected quality check if available.
- Confirm whether the duplicate count changed after any reviewed fix.

## Output

Return:

- Duplicate pattern.
- Suspected source.
- Keys or columns involved.
- Recommended remediation.
- Commands or queries run.
