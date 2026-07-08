---
name: maintenance-action
description: Use after a diagnosis skill has identified a likely fix and the repository owner wants to define a controlled action.
connections:
  - bruin
  - github
---

# Maintenance Action

## When to Use

Use this skill after a diagnosis skill has identified a likely fix and the repository owner wants to define a controlled action.

## Inputs

- Diagnosis summary.
- Exact action requested by the repository owner.
- Files, assets, connections, or systems in scope.
- Required approval process.

## Operating Context

- These starter skills are primarily meant for AI agents configured inside Bruin Cloud.
- In Bruin Cloud, use Bruin Cloud MCP tools when available. If using the CLI, supported operational commands include `bruin cloud runs trigger --project-id <project-id> --pipeline <pipeline-name>`, `bruin cloud runs rerun --project-id <project-id> --run-id <run-id> --only-failed`, `bruin cloud pipelines enable --project-id <project-id> --pipeline <pipeline-name>`, and `bruin cloud pipelines disable --project-id <project-id> --pipeline <pipeline-name>`.
- In local development, create runs with direct terminal commands such as `bruin run <path>`, `bruin run <asset>`, `bruin validate <path>`, and `bruin query`. Read troubleshooting context from terminal output and the local `logs/` folder, especially `logs/runs`, query logs, and export logs when they exist.
- If action verification requires running an asset or pipeline, prefer a dev or shadow environment. If none exists, ask whether to run in production or create temporary copies of the affected tables to reproduce and test the issue.
- For other agent runtimes or orchestrators, customize this skill with the correct log source and action mechanism before using it to trigger runs, enable or disable schedules, mark statuses, or change external systems.

## Context to Gather

- Confirm the action is explicitly requested and scoped.
- Inspect affected files and downstream dependencies.
- Identify whether the action touches data, source systems, credentials, or production environments.
- Determine the smallest validation command that proves the action worked.
- Use Bruin MCP docs tools or `bruin <command> --help` to confirm the current command syntax before running Cloud or local CLI commands.

## Decision Tree

- If the action touches production data or credentials, stop unless explicit approval is present.
- If the action changes repo files, make a minimal diff and run validation.
- If the action fixes data logic, first verify the known bad row, key, partition, or timestamp, then run the broader validation only after the specific example is resolved.
- If the action requires a pull request, prepare a clear summary and verification notes.
- If the action is not yet defined, return the diagnosis and ask for the missing policy.

## Actions

Define repository-specific actions here. Until customized, this skill must not modify data, source systems, production settings, credentials, repo files, Bruin Cloud pipeline state, or run state.

## Verification

- Run the smallest relevant Bruin validation command.
- For Cloud actions, verify the resulting state with `bruin cloud runs get`, `bruin cloud runs list`, or `bruin cloud pipelines get`.
- For local actions, verify with the local command output, `bruin validate <path>`, and local `logs/` files when present.
- Record any command that could not be run.

## Testing This Skill

- Use the local self-heal fixture from the Bruin `init` command docs after a diagnosis skill has found an isolated issue.
- Give explicit permission to fix one scenario and verify the agent changes only the approved asset logic.
- Verify the agent checks the known bad row, key, partition, or timestamp first, then runs the broader validation only after the specific example is resolved.

## Output

Return:

- Action taken or reason no action was taken.
- Files or systems touched.
- Validation results.
- Remaining manual approval or follow-up.
