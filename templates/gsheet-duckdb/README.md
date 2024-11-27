# Bruin - GSheet to DuckDB Template

This pipeline is a simple example of a Bruin pipeline that copies data from GSheet to DuckDB. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes two sample assets already:
- `gsheet_raw.customers`: A simple ingestr asset that copies a table from GSheet to DuckDB

## Setup

Example Sheet: https://docs.google.com/spreadsheets/d/1p40qR9t6DM5a1IskTkqEX9eZYZmBeILzUX_AdMkg__A/edit?usp=sharing

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/ingestion/google_sheets.html).
Here's a sample `.bruin.yml` file:

```yaml
default_environment: default
environments:
    default:
        connections:
            duckdb:
                - name: "duckdb-default"
                  path: "<Path to your DuckDB database file>"
            google_sheets:
                - name: "gsheet-default"
                  credentials_path: "<Path to your Google Sheets credentials JSON file>"
```

## Running the pipeline

bruin CLI can run the whole pipeline or any task with the downstreams:


```shell
❯ bruin run ./templates/gsheet-duckdb/                                                       (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...

Executed 1 tasks in 9.656s
```

You can also run a single task:


```shell
❯ bruin run ./templates/gsheet-duckdb/                                                       (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...


Executed 1 tasks in 9.656s
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!