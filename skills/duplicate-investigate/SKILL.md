---
name: duplicate-investigate
description: Use when duplicate rows, unstable primary keys, repeated ingestion, or failed uniqueness checks appear in a Bruin asset.
connections:
  - bruin
  - github
---

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
- If investigation or fix verification requires running an asset or pipeline, prefer a dev or shadow environment. If none exists, ask whether to run in production or create temporary copies of the affected tables to reproduce and test the issue.
- For other agent runtimes or orchestrators, customize this skill with the correct log source, query mechanism, and action mechanism before using it.

## Context to Gather

- Inspect quality checks and declared primary or unique keys.
- Find the ingestion or transformation logic that creates the affected rows.
- Compare source row counts, destination row counts, and recent run history when available.
- Check whether incremental logic can replay the same source records.
- Use Bruin MCP docs tools or `bruin <command> --help` to confirm the current command syntax before running Cloud or local CLI commands.

## Lineage Investigation

- Find one specific duplicate key or bad row pattern first, then keep every upstream query filtered to that instance. For example, prove `COUNT(*) > 1` for one `user_id` and `transaction_date` before broad scans.
- If the data has bronze, silver, gold, or other tiers, start at the asset where the duplicate appears and trace upstream through lineage one asset at a time.
- Query the filtered duplicate instance in each upstream asset until you find the first asset where the duplicate appears.
- Once the first bad asset is identified, read its SQL query or Python script and isolate the specific join, union, incremental filter, merge key, deduplication step, or function that likely introduced the duplicate.
- If the user has allowed fixes, change only that specific logic, then run the smallest asset-level validation in dev or shadow first. Recheck the same duplicate instance after the fix; only after that passes, run the full duplicate check or affected quality check.

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

## Testing This Skill

- Use the local self-heal fixture from the Bruin `init` command docs.
- Run the duplicate scenario and verify the agent starts with `order_id = 1002`, traces the duplicate from `gold_order_report` to `silver_orders`, and identifies the extra `UNION ALL` as the likely cause.
- If fixes are allowed, verify the agent removes or corrects only that logic, checks `order_id = 1002` first, then runs the full duplicate check.

## Output

Return:

- Duplicate pattern.
- Suspected source.
- Keys or columns involved.
- Recommended remediation.
- Commands or queries run.
