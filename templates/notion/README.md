# Bruin - Notion to Bigquery Template

This pipeline is a simple example of a Bruin pipeline that copies data from Notion to BigQuery. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes two sample assets already:
- `raw.notion`: A simple ingestr asset that takes copies a table from Notion to BigQuery
- `myschema.example`: A simple SQL asset that creates a table in BigQuery.
  - Feel free to change the type from `bq.sql` to anything.

## Setup
Add your connections and environments to the `.bruin.yml` file at your project root, not inside the pipeline folder. You can read more about connections [here](https://getbruin.com/docs/bruin/commands/connections.html).

Here's a sample `.bruin.yml` file:

```yaml
environments:
  default:
    connections:
      google_cloud_platform:
        - name: "gcp"
          service_account_file: "/path/to/my/key.json"
          project_id: "my-project-dev"
      notion:
        - name: "my-notion-connection"
          api_key: "XXXXXXXX"
```

## Running the pipeline

bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
bruin run .
```

You can also run a single task:

```shell
bruin run assets/notion.asset.yml
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!