## Bruin - Bruin Cloud Template

This template ingests metadata from the [Bruin Cloud API](https://getbruin.com/docs/bruin/ingestion/bruin.html) into DuckDB under the `bruin_cloud_logs` schema and builds a summary table on top.

The Bruin Cloud source exposes every table available from the API:

- `pipelines` — pipeline metadata (name, project, owner, schedule, default connections, commit, start date).
- `assets` — asset metadata (name, type, pipeline, project, uri, description, upstreams, downstream, owner, content, columns, materialization, parameters).

### Assets in this pipeline

- `assets/pipelines.asset.yml` — ingestr asset that loads the `pipelines` table into `bruin_cloud_logs.pipelines`.
- `assets/assets.asset.yml` — ingestr asset that loads the `assets` table into `bruin_cloud_logs.assets`.
- `assets/pipeline_summary.sql` — per-pipeline summary (asset counts, owner, schedule) materialized as `bruin_cloud_logs.pipeline_summary`.

Every asset includes table-level and column-level descriptions so the catalogue is self-documenting.

### Setup

Edit `.bruin.yml` and replace `your_api_token_here` with an API token created in your Bruin Cloud team settings (Teams → Create API Token → Pipeline List permission).

```yaml
default_environment: default
environments:
  default:
    connections:
      bruin:
        - name: "bruin-cloud-default"
          api_token: "your_api_token_here"
      duckdb:
        - name: "duckdb-default"
          path: "duckdb.db"
```

### Running the pipeline

```shell
bruin run ./bruin-cloud/pipeline.yml
```

You can also run a single asset:

```shell
bruin run assets/pipelines.asset.yml
```
