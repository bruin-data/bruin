# `run` Command

This command is used to execute a Bruin pipeline or a specific asset within a pipeline.

- You can run the pipeline from the current directory or a specific path to the pipeline/task definition.
- If you don't specify a path, Bruin will run the pipeline from the current directory.
- If you specify a path, Bruin will run the pipeline/asset from the directory of the file.
  - Bruin will try to infer if the given path is a pipeline or an asset and will run accordingly.
- You can give specific start and end dates to run the pipeline/asset for a specific range.
- You can limit the types of tasks to run by using the `--only` flag.
  - E.g. only run the quality checks: `bruin run --only checks`

```bash
bruin run [FLAGS] [optional path to the pipeline/asset]
```
<img alt="Bruin - init" src="/chesspipeline.gif" style="margin: 10px;" />
## Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--downstream` | bool | `false` | Run all downstream tasks as well. |
| `--start-date` | str | Beginning of yesterday | The start date of the range the pipeline will run for. Format: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, or YYYY-MM-DD HH:MM:SS.ffffff |
| `--end-date` | str | End of yesterday | The end date of the range the pipeline will run for. Format: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, or YYYY-MM-DD HH:MM:SS.ffffff |
| `--environment` | str | - | The environment to use. |
| `--force` | bool | `false` | Do not ask for confirmation in a production environment. |
| `--full-refresh` | bool | `false` | Truncate the table before running. |
| `--no-log-file` | bool | `false` | Do not create a log file for this run. |
| `--only` | []str | `main`, `checks` | Limit the types of tasks to run. Options: `main`, `checks`, `push-metadata`. |
| `--push-metadata` | bool | `false` | Push metadata to the destination database if supported (currently BigQuery). |
| `--tag` | str | - | Pick assets with the given tag. |
| `--workers` | int | `16` | Number of workers to run tasks in parallel. |

## Examples

Run the pipeline from the current directory:
```bash
bruin run
```

Run the pipeline from a file:
```bash
bruin run ./pipelines/project1/pipeline.yml
```

Run a specific asset:
```bash
bruin run ./pipelines/project1/assets/my_asset.sql
```

Run the pipeline with a specific environment:
```bash
bruin run --environment dev
```

Run the pipeline with a specific start and end date:
```bash
bruin run --start-date 2024-01-01 --end-date 2024-01-31
```

Run the assets in the pipeline that contain a specific tag:
```bash
bruin run --tag my_tag
```

Run only the quality checks:
```bash
bruin run --only checks
```

Run only the main tasks and not the quality checks:
```bash
bruin run --only main
```


## Metadata Push

Metadata push is a feature that allows you to push metadata to the destination database/data catalog if supported. Currently, we support BigQuery as the catalog.

There are two ways to push metadata:
1. You can set the `--push-metadata` flag to `true` when running the pipeline/asset.
2. You can fill out the `metadata_push` dictionary in the pipeline/asset definition.

```yaml
# pipeline.yml
name: bruin-init
schedule: daily
start_date: "2024-09-01"

default_connections:
   google_cloud_platform: "my-gcp-connection"

metadata_push:
  bigquery: true 
```

When pushing the metadata, Bruin will detect the right connection to use, same way as it happens with running the asset.








