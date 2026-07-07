# Init Command

## Overview

The `bruin init` command bootstraps a new **Bruin pipeline** from a predefined template.
It automatically sets up the folder structure, initializes configuration files, and optionally creates a new Git repository.

You can use it to start a new data pipeline project quickly, or to add a new pipeline inside an existing repository.

`bruin init` also includes an `ai` template category for adding generic AI agent starter files to an existing repository.

## Usage

```bash
bruin init [template] [folder] [--in-place]
```

### Examples

```bash
# Start an interactive prompt to choose a template
bruin init

# Create a pipeline from the "default" template in a new folder
bruin init default ecommerce-pipeline

# Create a pipeline in the current directory (no parent folder)
bruin init default --in-place

# Choose an AI starter template
bruin init ai

# Install a generic AGENTS.md file
bruin init ai-agents-md

# Install starter troubleshooting skills
bruin init ai-skill-self-heal
```

## How It Works

When you run `bruin init`, it:

1. Lists available templates from Bruin’s internal template registry.
   You can interactively select one via a terminal UI.
2. Copies all template files (e.g. `.asset.yml`, `.sql`, `.py`) into the target folder.
3. Merges any template-level `.bruin.yml` configuration into your existing (or newly created) root `.bruin.yml`.
4. Optionally initializes a **Git repository** if none exists.
5. Outputs next steps, such as validating or running your new pipeline.

---

## AI Templates

AI templates are not pipeline templates. They install files into the Git repository root when Bruin can find one, otherwise into the current directory.

```bash
# Open the AI template selector
bruin init ai

# Install AGENTS.md directly
bruin init ai-agents-md

# Install the troubleshooting skill pack directly
bruin init ai-skill-self-heal
```

AI templates do not create a pipeline folder, initialize Git, or create `.bruin.yml` by default. They also do not accept a folder argument or `--in-place`.

### Available AI Templates

| Template | What it installs |
| --- | --- |
| `ai-agents-md` | A generic `AGENTS.md` starter with Bruin-oriented agent instructions. |
| `ai-skill-self-heal` | Starter troubleshooting skills under `.agents/skills/`. |

The self-healing starter is a pack of focused skills:

* `pipeline-diagnose`
* `schema-drift-check`
* `duplicate-investigate`
* `freshness-check`
* `quality-check-investigate`
* `maintenance-action`

Each skill includes a placeholder `Actions` section for repository-specific behavior. Until customized, the skills only diagnose and report findings.

### AI Skill Runtime Expectations

The starter skills are primarily meant for AI agents configured inside Bruin Cloud. In that environment, agents should use Bruin Cloud MCP tools when available. If they use the CLI, the relevant Cloud commands include:

```bash
# Diagnose the latest failed or recent run
bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline-name> --latest

# Read run and asset logs
bruin cloud runs get --project-id <project-id> --run-id <run-id>
bruin cloud instances logs --project-id <project-id> --run-id <run-id> --asset <asset-name>
bruin cloud instances failed-logs --project-id <project-id> --run-id <run-id>

# Create or rerun Cloud runs
bruin cloud runs trigger --project-id <project-id> --pipeline <pipeline-name>
bruin cloud runs rerun --project-id <project-id> --run-id <run-id> --only-failed

# Enable or disable Cloud pipelines
bruin cloud pipelines enable --project-id <project-id> --pipeline <pipeline-name>
bruin cloud pipelines disable --project-id <project-id> --pipeline <pipeline-name>
```

For local development, the skills should rely on local terminal commands such as `bruin validate`, `bruin render`, `bruin query`, and `bruin run`. Local troubleshooting should read terminal output and the local `logs/` folder, especially `logs/runs`, query logs, and export logs when they exist. Local runs should be created with `bruin run` rather than Bruin Cloud run commands.

For any other agent runtime or orchestrator, customize the installed skills with the correct log source and action mechanism before using them to read logs, trigger runs, enable or disable pipelines, mark statuses, or change external systems.

### AI Template Conflicts

If an AI template target already exists, Bruin asks what to do:

* `add`: append or update the marked Bruin AI section in `AGENTS.md`; for skill files, keep existing files.
* `overwrite`: replace the existing target file.
* `skip`: leave the existing target unchanged.

In non-interactive shells, existing targets fail safely and Bruin prints the path that needs a choice.

### Optional AI Connections

Some AI starter skills can use Bruin Cloud or GitHub context. If `bruin init ai-skill-self-heal` does not find matching connections in `.bruin.yml`, Bruin asks whether to add placeholder connections:

```yaml
default_environment: default
environments:
  default:
    connections:
      bruin:
        - name: bruin-cloud
          api_token: "${BRUIN_CLOUD_API_TOKEN}"
      github:
        - name: github
          access_token: "${GITHUB_TOKEN}"
          owner: "<github-owner>"
          repo: "<github-repo>"
```

Bruin infers `owner` and `repo` from the `origin` remote when possible. Existing connections are never duplicated.

## Folder Structure

Every initialized pipeline follows this convention:

```text
my-pipeline/
├─ pipeline.yml        # Defines the pipeline metadata
└─ assets/             # Contains all assets for this pipeline
   ├─ raw.orders.asset.yml
   ├─ stg.orders.sql
   └─ mart.sales_daily.sql
```

If `--in-place` is used, the structure is created inside your current directory instead of nesting under `bruin/`.

## Behavior Details

### Template Selection

* If no template is passed, Bruin opens an interactive terminal picker built with [Bubble Tea](https://github.com/charmbracelet/bubbletea).
* Templates are loaded from the internal `templates` directory.

### Git Initialization

If Bruin detects no existing `.git` repository:

* A new Git repository is created (via `git init`).
* The pipeline is placed under `bruin/` unless `--in-place` is used.

### Configuration Merge

If the selected template contains its own `.bruin.yml`, Bruin merges:

* **Environment connections**
* **Secrets**
* **Default settings**

into the existing `.bruin.yml` at your project root.
This ensures shared environments (like `dev`, `prod`, etc.) stay consistent across pipelines.

## Arguments

### `template`

Name of the template to use. If omitted, an interactive selector appears.

* **Type:** `string`
* **Default:** `default`
* **Required:** `false`

### `folder`

Name of the folder where the pipeline will be created.

* **Type:** `string`
* **Default:** `bruin-pipeline` (when using default template), template name (when using other templates)
* **Required:** `false`

## Flags

### `in-place`

Initialize the pipeline directly in the current folder, instead of creating a `bruin/` directory.

* **Type:** `boolean`
* **Default:** `false`

## Example Output

### Initializing the default template

When you run `bruin init`, you'll see a list of available templates and a prompt to select one:

```bash
$ bruin init

Please select a template below:

 [x] default
 [ ] athena
 [ ] clickhouse
 [ ] duckdb
 [ ] chess
 [ ] python

A new 'default' pipeline created successfully in folder 'bruin-pipeline'.

You can run the following commands to get started:
    bruin validate bruin-pipeline
```

### Initializing the Shopify-Bigquery template

``` bash
bruin init shopify-bigquery
```

#### Output

<img alt="Bruin - clean" src="/init.gif" style="margin: 10px;" />

## Notes

* Traversing up/down the filesystem (e.g., `../pipeline`) is not allowed for safety.
* `.bruin.yml` is automatically created or updated during initialization.
* The command is safe to run multiple times — Bruin intelligently merges existing configuration.
