# Init Command

## Overview

The `bruin init` command bootstraps a new **Bruin pipeline** from a predefined template.
It automatically sets up the folder structure, initializes configuration files, and optionally creates a new Git repository.

You can use it to start a new data pipeline project quickly, or to add a new pipeline inside an existing repository.

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

## Folder Structure

Every initialized pipeline follows this convention:

```
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

- **Type:** `string`
- **Default:** `default`
- **Required:** `false`

### `folder`

Name of the folder where the pipeline will be created.

- **Type:** `string`
- **Default:** `bruin-pipeline`
- **Required:** `false`

## Flags

### `in-place`

Initialize the pipeline directly in the current folder, instead of creating a `bruin/` directory.

- **Type:** `boolean`
- **Default:** `false`

## Example Output

### Initializing the default template

When you run `bruin init`, you'll see a list of available templates and a prompt to select one:

```bash
$ bruin init

Please select a template below:

 [x] default
 [ ] postgres-duckdb
 [ ] analytics-starter

A new 'default' pipeline created successfully in folder 'ecommerce'.

You can run the following commands to get started:
    bruin validate ecommerce
```

### Initializing the Shopify-Bigquery template

``` bash
bruin init shopify-bigquery
```
#### Output:


<img alt="Bruin - clean" src="/init.gif" style="margin: 10px;" />


## Notes

* Traversing up/down the filesystem (e.g., `../pipeline`) is not allowed for safety.
* `.bruin.yml` is automatically created or updated during initialization.
* The command is safe to run multiple times — Bruin intelligently merges existing configuration.