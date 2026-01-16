# Zoomcamp - Data Platform (Bruin) Template

This template is an **educational scaffold** for building an end-to-end data pipeline in Bruin (ingestion → staging → reporting) with **no implementation provided**.

## Learning goals

- Understand how Bruin projects are structured (`pipeline.yml` + `assets/`)
- Use **materialization strategies** intentionally (append, time_interval, etc.)
- Declare **dependencies** and explore lineage (`bruin lineage`)
- Apply **metadata** (columns, primary keys, descriptions) and **quality checks**
- Parameterize runs with **pipeline variables**

## Pipeline skeleton (what goes where)

You will implement the pipeline in the **three folders** below. Keep each folder focused.

```text
assets/
  ingestion/
    TODO: Python asset (ingest raw data) using Bruin Python materialization
    TODO: SQL asset (load/seed lookup data) OR a seed asset (.asset.yml + CSV)
  staging/
    TODO: SQL asset (clean + normalize schema, deduplicate, apply quality checks)
  reports/
    TODO: SQL asset (aggregate for analytics / dashboards)
```

## Suggested workflow (CLI)

```bash
# Validate structure & definitions
bruin validate ./templates/zoomcamp/pipeline.yml --environment default

# Run an ingestion asset, then downstream (to test incrementally)
bruin run ./templates/zoomcamp/assets/ingestion/trips.py \
  --environment default \
  --start-date 2021-01-01 \
  --end-date 2021-01-31 \
  --var 'taxi_types=["yellow"]' \
  --downstream

# First-time run tip:
# Use --full-refresh to create/replace tables from scratch (helpful on a new DuckDB file).
bruin run ./templates/zoomcamp/pipeline.yml --environment default --full-refresh
```

## Suggested workflow (VS Code extension)

1. Install the **Bruin VS Code extension**:
   - Open VS Code → Extensions
   - Search: "Bruin" (publisher: Bruin / bruin-data)
   - Install, then reload VS Code

2. Open this template folder and run from the Bruin panel:
   - Open `pipeline.yml` or any asset file
   - Use the Bruin panel to run `validate`, `run`, and see logs

3. Set run parameters when executing:
   - **Start / end dates**: set `--start-date` and `--end-date` for incremental windows
   - **Custom variables**: set with `--var`, e.g. `--var 'taxi_types=["yellow"]'`
   - Docs:
     - Run flags: https://getbruin.com/docs/bruin/commands/run.html
     - Variables: https://getbruin.com/docs/bruin/getting-started/pipeline-variables.html

Docs:
- `bruin run`: https://getbruin.com/docs/bruin/commands/run.html
- Materialization: https://getbruin.com/docs/bruin/assets/materialization.html
- Python assets: https://getbruin.com/docs/bruin/assets/python.html
- Quality checks: https://getbruin.com/docs/bruin/quality/overview.html

