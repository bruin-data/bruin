# Quality Check Investigate

## When to Use

Use this skill when a Bruin quality check fails or starts warning unexpectedly.

## Inputs

- Affected asset and check name.
- Check SQL or check definition.
- Failure output and sample rows, if available.

## Operating Context

- These starter skills are primarily meant for AI agents configured inside Bruin Cloud.
- In Bruin Cloud, use Bruin Cloud MCP tools when available. If using the CLI, inspect failures with `bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline-name> --latest`, fetch an asset instance with `bruin cloud instances get --project-id <project-id> --run-id <run-id> --asset <asset-name>`, and fetch logs with `bruin cloud instances logs --project-id <project-id> --run-id <run-id> --asset <asset-name>`.
- In local development, inspect terminal output and the local `logs/` folder, especially `logs/runs`, query logs, and export logs when they exist. Use `bruin run --only checks <path>` or a limited `bruin query` where appropriate.
- If investigation or fix verification requires running an asset or pipeline, prefer a dev or shadow environment. If none exists, ask whether to run in production or create temporary copies of the affected tables to reproduce and test the issue.
- For other agent runtimes or orchestrators, customize this skill with the correct check-result source, log source, and action mechanism before using it.

## Context to Gather

- Inspect the asset definition and check configuration.
- Determine whether the check tests completeness, uniqueness, freshness, accepted values, ranges, or custom SQL.
- Compare recent upstream changes with the first failing run.
- Sample failing rows only when it is safe and allowed by repository policy.
- Use Bruin MCP docs tools or `bruin <command> --help` to confirm the current command syntax before running Cloud or local CLI commands.

## Lineage Investigation

- Find one specific failing example first, then keep every upstream query filtered to that row, key, partition, or time window. Use the full failing set only after the specific example is understood.
- If the data has bronze, silver, gold, or other tiers, start at the asset where the quality check fails and trace upstream through lineage one asset at a time.
- Query the filtered example in each upstream asset until you find the first asset where the invalid value, missing row, duplicate, stale timestamp, or unexpected aggregate appears.
- Once the first bad asset is identified, read its SQL query or Python script and isolate the specific calculation, join, filter, cast, window function, incremental condition, or Python function that likely caused the failure.
- If the user has allowed fixes, change only that specific logic, then run the smallest asset-level or check-level validation in dev or shadow first. Recheck the same failing example after the fix; only after that passes, run the full quality check.

## Decision Tree

- If the check definition is wrong, identify the intended rule and affected files.
- If the data violates a valid rule, locate the upstream source or transformation that introduced it.
- If the threshold is outdated, document why the threshold changed and who should approve it.
- If the check is flaky, inspect nondeterministic ordering, time windows, and incremental logic.

## Actions

Define repository-specific actions here. Until customized, this skill must report findings and stop before changing thresholds, deleting rows, or modifying data.

## Verification

- Re-run the failed quality check or the smallest equivalent query.
- Re-run `bruin validate <path>` if files were changed.
- Confirm whether failing row counts and examples match the diagnosis.

## Testing This Skill

- Use the local self-heal fixture from the Bruin `init` command docs.
- Run the quality-check scenario and verify the agent starts with `order_id = 1003`, traces the negative `amount` upstream, and identifies the specific calculation in `silver_orders`.
- If fixes are allowed, verify the agent changes only the isolated expression, checks `order_id = 1003` first, then runs the full quality check.

## Output

Return:

- Failed rule.
- Evidence and sample pattern.
- Root cause or strongest hypothesis.
- Recommended fix.
- Verification commands and results.
