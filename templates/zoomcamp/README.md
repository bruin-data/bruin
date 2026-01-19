# Zoomcamp - Data Platform (Bruin) Template

This template is an **educational scaffold** for building an end-to-end data pipeline in Bruin (ingestion → staging → reporting) with **no implementation provided**.

## Learning goals

- Understand how Bruin projects are structured (`pipeline.yml` + `assets/`)
- Use **materialization strategies** intentionally (append, time_interval, etc.)
- Declare **dependencies** and explore lineage (`bruin lineage`)
- Apply **metadata** (columns, primary keys, descriptions) and **quality checks**
- Parameterize runs with **pipeline variables**

## Pipeline skeleton (what goes where)

You will implement the pipeline in the folder structure below.
The suggested structure using a medallion (tiered) data model, but you may structure your pipeline however you like.

```text
.bruin.yml
pipeline/
  pipeline.yml
  assets/
    ingestion/
    staging/
    reports/
```

## Suggested workflow (CLI)

### Step 1: Configure the `.bruin.yml` and `pipeline.yml` files
- create the `.bruin.yml` file in the root directory
  - configure environments
  - create a connection for DuckDB

- create a `pipeline.yml` file in the pipeline directory
  - set the pipeline name/schedule/start_date
  - initialize the `default_connections`
  - add custom `variables`

### Step 2: Create the pipeline assets
- ingestion
  - python script to extract the files from source endpoint
  - sql/yml assets to load/seed lookup tables
- staging
  - sql asset(s) to clean, normalize schema, deduplicate
- reports
  - sql asset(s) to aggregate and transform data

### Step 3: Validate & run the pipeline
- 
```bash
# Validate structure & definitions
bruin validate ./templates/zoomcamp/pipeline.yml --environment default

# First-time run tip:
# Use --full-refresh to create/replace tables from scratch (helpful on a new DuckDB file).
bruin run ./templates/zoomcamp/pipeline.yml --environment default --full-refresh

# Run an ingestion asset, then downstream (to test incrementally)
bruin run ./templates/zoomcamp/assets/ingestion/trips.py \
  --environment default \
  --start-date 2021-01-01 \
  --end-date 2021-01-31 \
  --var taxi_types='["yellow"]' \
  --downstream

# Query your tables using `bruin query`
# Docs: https://getbruin.com/docs/bruin/commands/query
bruin query --connection duckdb-default --query "SHOW TABLES"
bruin query --connection duckdb-default --query "SELECT COUNT(*) FROM ingestion.trips"
bruin query --connection duckdb-default --query "SELECT COUNT(*) FROM staging.trips"

# Open DuckDB UI (useful for exploring tables interactively)
# Requires DuckDB CLI installed locally.
duckdb duckdb.db -ui
```

## Suggested workflow (VS Code extension)

Please refer to the doc page for more details:
  - https://getbruin.com/docs/bruin/vscode-extension/overview.html
  - https://getbruin.com/docs/bruin/getting-started/features.html#vs-code-extension

1. Install the **Bruin VS Code extension**:
   - Open VS Code → Extensions
   - Search: "Bruin" (publisher: bruin)
   - Install, then reload VS Code

2. Open this template folder and run from the Bruin panel:
   - Open `pipeline.yml` or any asset file
   - Use the Bruin panel to run `validate`, `run`, and see rendered code
      - To open the panel, click the Bruin logo located in the top-right corner of the file

3. Set run parameters when creating a run:
   - **Start / end dates** for incremental windows
   - **Custom variables** like `'taxi_types=["yellow"]'`

### Addition Docs:
- `bruin run`: https://getbruin.com/docs/bruin/commands/run.html
- Materialization: https://getbruin.com/docs/bruin/assets/materialization.html
- Python assets: https://getbruin.com/docs/bruin/assets/python.html
- Quality checks: https://getbruin.com/docs/bruin/quality/overview.html
