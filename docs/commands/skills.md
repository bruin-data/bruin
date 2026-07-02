# Skills Command

## Overview

The `bruin skills` command manages Bruin-provided agent skills for a repository.

Use `bruin skills init` to install or update bundled skills under the repository's agent home. Bruin detects `.agents` and `.claude` at the project root:

- If `.agents` exists, skills are installed under `.agents/skills`.
- If only `.claude` exists, skills are installed under `.claude/skills`.
- If both exist as separate directories, `.agents/skills` is the primary location and `.claude/skills/<skill>` is symlinked to it.
- If neither exists, Bruin creates `.agents/skills`.

Existing bundled skills are updated when their installed files differ from the bundled Bruin skill content. Updates cleanly rewrite the skill directory, which removes stale files from older bundled skill definitions.

## Usage

```bash
bruin skills init
```

## Bundled Skills

### `bruin-semantic-layer`

Installs guidance for creating, editing, reviewing, and troubleshooting Bruin semantic layer models and semantic query usage.

The skill points agents to the local source of truth:

- `docs/core-concepts/semantic-layer.md`
- `docs/commands/query.md`
- `pkg/semantic/`

## Example

```bash
bruin skills init
```

Example output:

```plaintext
Bruin skills initialized in /path/to/repo/.agents/skills
- installed bruin-semantic-layer
```
