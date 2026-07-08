# AI Skills Command

## Overview

The `bruin ai skills` command installs or updates Bruin-provided agent skills for a repository.

Run it without arguments to open a checkbox selector, or pass a skill name directly. Use `all` to install every bundled skill and the `AGENTS.md` agent guidance.

Bruin detects `.agents` and `.claude` at the project root:

- If `.agents` exists, skills are installed under `.agents/skills`.
- If only `.claude` exists, skills are installed under `.claude/skills`.
- If both exist as separate directories, `.agents/skills` is the primary location and `.claude/skills/<skill>` is symlinked to it.
- If neither exists, Bruin creates `.agents/skills`.

Existing bundled skills are updated when their installed files differ from the bundled Bruin skill content. Updates cleanly rewrite the skill directory, which removes stale files from older bundled skill definitions.

## Usage

```bash
bruin ai skills
bruin ai skills bruin-semantic-layer
bruin ai skills agents-md
bruin ai skills all
```

## Bundled Skills

### `AGENTS.md`

Installs or updates the repository-level `AGENTS.md` guidance used by AI agents. If the file already exists, Bruin replaces the marked Bruin AI section when present or appends that section when absent.

### `bruin-semantic-layer`

Installs guidance for creating, editing, reviewing, and troubleshooting Bruin semantic layer models and semantic query usage.

The skill points agents to the local source of truth:

- `docs/core-concepts/semantic-layer.md`
- `docs/commands/query.md`
- `pkg/semantic/`

### Self-healing Skills

Bruin also includes starter troubleshooting skills:

- `pipeline-diagnose`
- `schema-drift-check`
- `duplicate-investigate`
- `freshness-check`
- `quality-check-investigate`
- `maintenance-action`

These skills help agents diagnose failed runs, schema drift, duplicates, freshness issues, quality checks, and approved maintenance actions. When installed, Bruin can optionally add placeholder Bruin Cloud and GitHub connections to `.bruin.yml` if matching connections do not already exist.

### Runtime Expectations

The starter skills can be used by Bruin Cloud agents, local agents, and external assistants connected to Bruin Cloud. The Bruin CLI exposes Bruin Cloud operations through `bruin cloud`, which is the clearest path when an assistant has shell access plus a configured API key or `.bruin.yml`. Bruin Cloud MCP is optional; use it when the assistant is configured for MCP tool calls, needs structured Cloud tools directly in chat, or should not shell out to the local CLI.

In local development, the skills should rely on terminal commands such as `bruin validate`, `bruin render`, `bruin query`, and `bruin run`, plus the local `logs/` folder when it exists.

### Test With `self-heal-demo`

Use the regular `self-heal-demo` template project to test the data-problem skills locally with DuckDB:

```bash
mkdir tmp-pipeline
cd tmp-pipeline
git init

bruin ai skills all
bruin init self-heal-demo

bruin run self-heal-demo/demo-seed
bruin validate self-heal-demo/demo-pipeline
bruin run --tag duplicate-investigate self-heal-demo/demo-pipeline || true
bruin run --tag quality-check-investigate self-heal-demo/demo-pipeline || true
bruin run --tag freshness-check self-heal-demo/demo-pipeline || true
bruin run --tag schema-drift-check self-heal-demo/demo-pipeline || true
```

The template includes `demo-seed` for raw DuckDB tables and `demo-pipeline` for intentional duplicate, quality, freshness, and schema drift issues in normal business-named SQL assets. See the generated `self-heal-demo/README.md` or the [template docs](/getting-started/templates-docs/self-heal-demo-README) for the scenario map, proof queries, and expected diagnoses.

## Example

```bash
bruin ai skills bruin-semantic-layer
```

Example output:

```plaintext
Bruin skills initialized in /path/to/repo/.agents/skills
- installed bruin-semantic-layer
```

Running `bruin ai skills` without arguments opens a checkbox list where multiple skills and `AGENTS.md` can be selected in one run.
