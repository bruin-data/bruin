# AGENTS.md

This file gives AI agents the repo-specific context they need for Bruin pipeline development, data analysis, and troubleshooting.

<!-- BEGIN BRUIN AI AGENTS -->
## Bruin Pipeline Agent Guidance

### Scope

- Use this file for Bruin pipeline development, asset work, data analysis, and troubleshooting.
- Do not treat this as a general application-code instruction file.
- Prefer existing Bruin CLI commands, pipeline patterns, and project conventions over ad hoc scripts.

### Bruin MCP and Docs

- Use Bruin MCP when it is available to inspect Bruin docs and project context.
- Local Bruin MCP runs with `bruin mcp`.
- If configuring an AI client, register the command as `bruin mcp` and then use the exposed Bruin docs tools.
- Use the docs tree first, then fetch the specific doc page you need. Prefer docs such as `commands/run`, `commands/query`, `commands/validate`, `commands/connections`, `assets/sql`, `assets/ingestr`, `assets/definition-schema`, and platform-specific docs.
- When MCP is not available, use `bruin --help`, `bruin <command> --help`, and the repository docs.

### Navigating Pipelines and Assets

- Start from the nearest `pipeline.yml` to understand pipeline name, schedule, defaults, and asset layout.
- Inspect asset definitions before editing SQL or Python. SQL assets usually include Bruin metadata between `/* @bruin` and `@bruin */`.
- Follow dependencies through `depends`, upstream asset names, source tables, and materialization settings.
- Use `bruin validate <path>` on the affected pipeline or asset after changes.
- Use `bruin render <asset>` to inspect rendered SQL before running it.
- Keep changes scoped to the affected pipeline, asset, checks, or documentation.

### Environments and Secrets

- Treat `.bruin.yml` as a local development config file. Do not commit real credentials, tokens, passwords, private keys, or personal connection details.
- In Bruin Cloud and other server environments, connections and secrets may be initialized from environment variables instead of a local `.bruin.yml`.
- Prefer environment-variable references in local config, such as `${MY_SECRET}`, over literal secret values.
- If `.bruin.yml` defines both `dev` and `prod` environments, run commands in `dev` unless the user explicitly asks for `prod`.
- Never run against `prod` by assumption. State the environment you are using in your response.
- Before `--full-refresh`, backfills, destructive operations, or commands that can replace data, get explicit user confirmation unless the user already gave specific instructions.

### Querying Data

- Prefer `bruin query` and the connections already configured in `.bruin.yml` when querying data.
- Use read-only, narrow queries first: select only needed columns, include limits, and filter partitions or date ranges where possible.
- Avoid large scans, full table reads, exports, or expensive joins unless the user confirms the scope.
- Do not query sensitive columns unless needed for the task. Mask or aggregate sensitive results in summaries.
- When investigating an asset, prefer rendering the asset query and running a limited diagnostic query before changing logic.
- Save useful diagnostic SQL only when it belongs in the repository; otherwise report the query and result.

### Skills

- Check `.agents/skills/` for repository-specific skills before starting specialized work.
- Open the relevant `SKILL.md` and follow its workflow when the task matches its purpose.
- Use the smallest applicable skill. Do not run broad maintenance or action skills when a diagnosis skill is enough.
- If a skill has an `Actions` placeholder, treat it as a policy gap: report findings and stop before modifying data, source systems, production settings, credentials, or repo files.
- When a skill and Bruin docs disagree, verify with the current Bruin CLI help or MCP docs and report the discrepancy.

### Reporting

- Summarize commands run, environment used, files changed, and validation results.
- If validation cannot be run, say why and provide the exact command the user can run.
- If a fix requires warehouse, source-system, or Bruin Cloud action, document the exact follow-up instead of guessing.
<!-- END BRUIN AI AGENTS -->
