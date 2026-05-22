# Assets

The **Assets** section in Bruin Cloud is the catalog of every table, view, file, model, and check that Bruin manages for your team. Use it to browse what exists, see how each asset is built, read its column metadata, run quality checks, and apply AI-generated improvements. Every asset shown here is defined in your repo using the [asset definition schema](/assets/definition-schema).

Open it from **Catalog → Assets** in the top nav.

## Asset catalog

The catalog is a searchable table of every asset across every pipeline. Two layouts:

- **Tree view** (left sidebar): folder/file structure as it appears in the repo.
- **Table view** (main panel): one row per asset.

### Filters

The top of the page exposes filters that combine with AND logic by default:

- **Pipeline** and **Project** (multi-select)
- **Asset type** — SQL, Python, ingestr, dbt, etc.
- **Owner**
- **Domain** (from glossary)
- **Connection**
- **Tags**
- **Description status** — quick toggle for *no description*
- **Quality score** — min/max sliders
- **Has quality checks** — toggle

Tag and domain filters can be switched to OR with a per-filter toggle. **Clear all** resets every filter.

### Search

Full-text search across asset names sits at the top. Results sort by downstream asset count by default — the most-depended-on assets first.

## Asset detail

Click any asset to open its detail page. The page has six primary tabs:

### Overview

The default tab. Collapsible sections, top to bottom:

- **Definition** — the asset's source code (SQL, Python, YAML) with syntax highlighting and a copy button. For Jinja-templated SQL, toggle **Rendered SQL** to see what actually runs.
- **Schedule** — the pipeline's schedule and any per-asset interval modifiers.
- **Owner** — the owner's name and any tags. "No Owner" if unset.
- **Materialisation** — type and strategy (table, view, incremental), partition / cluster-by keys, incremental key.
- **Connections** — which connections this asset uses, with missing ones flagged.
- **Suggestions** — AI-generated improvements (see below).
- **Chat** — open the **ChatBot** panel to ask the agent questions scoped to this asset.

### Lineage

Upstream and downstream dependencies as a graph. Double-click a node to expand two levels. External assets (from other pipelines or repos via [URI](/cloud/cross-pipeline)) render distinctly.

### Columns

One row per column, showing:

- **Name** and **type**
- **Description** (editable inline if you have edit permission)
- **Primary Key** toggle
- **Custom checks** attached to that column — name, expected value, non-blocking flag

You can add or remove columns from this tab. Custom metadata keys configured on the team (see [Team Settings → Column Metadata Keys](/cloud/team-settings#projects)) appear as additional editable fields.

### Custom Checks

The asset-level checks (not column-level). Each check is collapsible, showing name, expected value, non-blocking flag, description, and the SQL the check runs. Editing happens via the asset's YAML file; this tab is for review.

### Governance

How the asset scores against Bruin's built-in [governance rules](/cloud/governance) — description present, owner set, columns defined, checks present, primary key set, column-level descriptions, column-level checks. A green checkmark means full score; a bar shows partial credit.

### Runs

Recent runs of this specific asset, with status, duration, and links to the full [run detail](/cloud/runs#run-detail). Visible if the team has the runs feature enabled.

Two extra tabs may appear depending on team permissions: **Query Costs** (per-asset BigQuery/Snowflake spend) and **Usage** (compute time consumed).

## Asset profile

Bruin Cloud profiles your assets periodically and stores row count, null percentages, and distinct values per column. The Overview tab shows **today's row count** as a headline and an area chart of historical counts.

If profiling hasn't run for the project, the section reads *"Profiling is disabled for this project"*.

## AI suggestions

When the agent has access to an asset, it can suggest improvements — missing descriptions, additional column checks, type corrections. Suggestions show up under the **Suggestions** card on the Overview tab, each with a title, type badge, and a markdown description.

To apply suggestions:

1. Tick the boxes next to the ones you want.
2. Click **Apply Suggestions**.
3. Bruin merges them into the asset YAML and commits.

You can also generate suggestions on demand from the [Bruin CLI](/commands/ai-enhance) with `bruin ai enhance`.

## Global lineage

For the bird's-eye view of how all your pipelines connect, open **Catalog → Lineage**. See [Catalog → Lineage](/cloud/catalog#global-lineage) for details.

## Related

- [Catalog](/cloud/catalog) — glossary, owners, and the global lineage view.
- [Pipelines](/cloud/pipelines) — how assets are grouped into runnable units.
- [Runs](/cloud/runs) — per-run history for an asset.
- [Governance](/cloud/governance) — what each rule checks and how the score is computed.
- [Cross-pipeline dependencies](/cloud/cross-pipeline) — depending on an asset that lives elsewhere.
- [Asset definition schema](/assets/definition-schema) — the YAML shape every asset follows.
- [Columns](/assets/columns) and [Materialization](/assets/materialization) — fields surfaced on the Columns tab.
- [Quality checks](/quality/overview) and [Custom checks](/quality/custom) — what powers the Custom Checks tab.
- [Jinja templating](/assets/templating/templating) and [`bruin render`](/commands/render) — how the "Rendered SQL" toggle works under the hood.
- [`bruin ai-enhance`](/commands/ai-enhance) — generate the same suggestions locally from the CLI.
