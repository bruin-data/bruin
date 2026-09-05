# Init Command

## Overview

The `bruin init` command bootstraps a new **Bruin pipeline** from a predefined template. It automatically sets up the folder structure, initializes configuration files, and optionally creates a new Git repository.

You can use it to start a new data pipeline project quickly, or to add a new pipeline inside an existing repository.

## Usage

```bash
bruin init [template] [folder] [--in-place] [--merge]
```

### Examples

```bash
# Start an interactive prompt to choose a template
bruin init

# Create a pipeline from the "default" template in a new folder
bruin init default ecommerce-pipeline

# Create a pipeline in the current directory (no parent folder)
bruin init default --in-place

# Create the self-healing DuckDB demo pipeline
bruin init self-heal-demo

# Add the default template's assets to an existing pipeline
bruin init default pipelines/existing --merge
```

## How It Works

When you run `bruin init`, it:

1. Lists available templates from Bruin’s internal template registry. You can interactively select one via a terminal UI.
2. Copies all template files (e.g. `.asset.yml`, `.sql`, `.py`) into the target folder.
3. Merges any template-level `.bruin.yml` configuration into your existing (or newly created) root `.bruin.yml`.
4. Optionally initializes a **Git repository** if none exists.
5. Outputs next steps, such as validating or running your new pipeline.

With `--merge`, Bruin instead adds the selected template's `assets/` and
`macros/` files — plus a pipeline-level `requirements.txt`, `pyproject.toml` or
`uv.lock` when the template ships one — to the existing pipeline at `folder`, and
merges the template's `.bruin.yml` into the project-level config. It leaves the
existing `pipeline.yml`, `README.md` and every other pipeline file unchanged.

Because `pipeline.yml` is not merged, assets that rely on the template's own
pipeline configuration may need a manual follow-up. For example
`templates/default` sets `default_connections`, and `templates/stripe-bigquery`
sets an ingestr `default:` block; copy the keys you need into the destination
`pipeline.yml` if the merged assets fail to validate.

## Folder Structure

`bruin init` keeps your connection config (`.bruin.yml`) at the **project root** and places pipeline files in a named pipeline folder. `.bruin.yml` is **never** written inside the pipeline folder.

For a brand-new project (no existing Git repo), Bruin creates a `bruin/` root:

```text
bruin/                 # project root, created by bruin init
├─ .bruin.yml          # environments & connections
└─ my-pipeline/        # your pipeline
   ├─ pipeline.yml     # defines the pipeline metadata
   └─ assets/          # contains all assets for this pipeline
      ├─ raw.orders.asset.yml
      ├─ stg.orders.sql
      └─ mart.sales_daily.sql
```

When you run `bruin init` **inside an existing Git repository**, no `bruin/` wrapper is created: the pipeline folder is created in your current directory, and `.bruin.yml` is placed at the **repository root** — which may be several levels above the pipeline folder. Use `--in-place` in a fresh project to skip the `bruin/` wrapper and use the current directory instead.

## Behavior Details

### Template Selection

* If no template is passed, Bruin opens an interactive terminal picker built with [Bubble Tea](https://github.com/charmbracelet/bubbletea).
* Templates are loaded from the internal `templates` directory.

### Git Initialization

If Bruin detects no existing `.git` repository:

* A new Git repository is created (via `git init`).
* The pipeline is placed under `bruin/` unless `--in-place` is used.

Merge mode does not create a Git repository. If the selected template contains
`.bruin.yml`, the destination pipeline must already belong to a Git repository so
Bruin can resolve the project-level config path.

### Configuration Merge

If the selected template contains its own `.bruin.yml`, Bruin merges:

* **Environment connections**
* **Secrets**
* **Default settings**

into the existing `.bruin.yml` at your project root. This ensures shared environments (like `dev`, `prod`, etc.) stay consistent across pipelines.

The same configuration merge is performed with `--merge`. Merge mode checks every
destination path for conflicts first, then merges the config, then copies the
files. A conflict aborts before anything is written, and a copy that fails partway
removes the files it had already created, so a failed `--merge` is always safe to
re-run. If the project does not have a `.bruin.yml` yet, Bruin creates it at the
Git repository root. Templates without `.bruin.yml` leave the project config
unchanged.

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

### `merge`

Merge the selected template's assets into an existing pipeline instead of creating
a pipeline from the whole template.

* **Type:** `boolean`
* **Default:** `false`
* **Requires:** an explicit `folder` containing `pipeline.yml` or `pipeline.yaml`

The destination may be a nested or absolute path. Bruin preserves the directory
structure beneath each template `assets/` and `macros/` folder and merges template
connections into the project-level `.bruin.yml`. If any destination path already
exists, the command stops before copying anything and lists the conflicts;
existing files are never overwritten. `--merge` cannot be combined with
`--in-place`.

If a template contains more than one pipeline (for example `self-heal-demo`), the
files of all of them are merged into the single destination pipeline.

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

Config:   /Users/me/my-repo/.bruin.yml
Pipeline: /Users/me/my-repo/deep/nested/bruin-pipeline

Created .bruin.yml at /Users/me/my-repo/.bruin.yml.
This is your Git repo root, so it may sit several levels above the pipeline folder.

Next steps:
  1. Add your connection credentials to /Users/me/my-repo/.bruin.yml
  2. Run: bruin validate bruin-pipeline
  3. Run: bruin run bruin-pipeline
```

The summary always prints the resolved `.bruin.yml` path, so you never have to guess where
it landed (see [Folder Structure](#folder-structure)). If a `.bruin.yml` was already there,
Bruin says whether it merged the template's configuration into it:

```bash
Using existing .bruin.yml at /Users/me/my-repo/.bruin.yml (merged template config).
```

or left it as it was, which happens for templates that ship no `.bruin.yml`:

```bash
Using existing .bruin.yml at /Users/me/my-repo/.bruin.yml (left unchanged).
```

### Initializing a Shopify template

``` bash
bruin init shopify-clickhouse
```

The `shopify-clickhouse` template creates raw Shopify ingestion assets, conformed commerce models, and ClickHouse reporting marts. Configure the generated pipeline with your Shopify and ClickHouse connections before running it.

#### Output

<img alt="Bruin - clean" src="/init.gif" style="margin: 10px;" />

## Notes

* Traversing up/down the filesystem (e.g., `../pipeline`) is not allowed for safety.
  This restriction does not apply to `--merge`, which must point to an existing pipeline.
* `.bruin.yml` is automatically created or updated at your **project root** during initialization — the Git repository root when you are inside an existing repo, or a new `bruin/` folder for a fresh project.
  Merge mode also updates it when the selected template provides configuration.
* The command is safe to run multiple times — Bruin intelligently merges existing configuration.
