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
- For other agent runtimes or orchestrators, customize this skill with the correct check-result source, log source, and action mechanism before using it.

## Context to Gather

- Inspect the asset definition and check configuration.
- Determine whether the check tests completeness, uniqueness, freshness, accepted values, ranges, or custom SQL.
- Compare recent upstream changes with the first failing run.
- Sample failing rows only when it is safe and allowed by repository policy.
- Use Bruin MCP docs tools or `bruin <command> --help` to confirm the current command syntax before running Cloud or local CLI commands.

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

## Output

Return:

- Failed rule.
- Evidence and sample pattern.
- Root cause or strongest hypothesis.
- Recommended fix.
- Verification commands and results.
