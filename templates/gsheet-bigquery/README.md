# Bruin - GSheet to BigQuery Template

This pipeline is a simple example of a Bruin pipeline that copies data from GSheet to BigQuery. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes two sample assets already:
- `gsheet_raw.customers`: A simple ingestr asset that copies a table from GSheet to BigQuery

## Setup

Example Sheet: https://docs.google.com/spreadsheets/d/1p40qR9t6DM5a1IskTkqEX9eZYZmBeILzUX_AdMkg__A/edit?usp=sharing

Add your connections and environments to the `.bruin.yml` file at your project root, not inside the pipeline folder. You can read more about connections [here](https://getbruin.com/docs/bruin/ingestion/google_sheets.html).

Here's a sample `.bruin.yml` file:

```yaml
default_environment: default
environments:
    default:
        connections:
            google_cloud_platform:
                - name: "gcp-default"
                  service_account_file: "<Path to your Google Cloud service account JSON file>"
                  project_id: "<Your Google Cloud project ID>"
            google_sheets:
                - name: "gsheet-default"
                  service_account_file: "<Path to your Google service account JSON file>"
```

## Running the pipeline

Run these commands from the pipeline directory.

bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
bruin run .
```

You can also run a single task:

```shell
bruin run assets/gsheet.asset.yml
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!