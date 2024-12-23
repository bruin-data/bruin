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
|  `--continue` | bool | `false` | Continue from the last failed task. |


## Continue from the last failed task

If you want to continue from the last failed task, you can use the `--continue` flag. This will run the pipeline/asset from the last failed task. Bruin will automatically retrive all the flags used in the last run.

```bash
bruin run --continue 
```

NOTE: This will only work if the pipeline structure is not changed. If Pipeline/Asset dependencies are changed, you will need to run the pipeline/asset from the beginning. This is to ensure that the pipeline/asset is run in the correct order.

### Focused Runs: Filtering by Tags and Task Types
As detailed in the flag section above, the  `--tag`, `--downstream`, and `--only` flags provide powerful ways to filter and control which tasks in your pipeline are executed. These flags can also be combined to fine-tune pipeline runs, allowing you to execute specific subsets of tasks based on tags, include their downstream dependencies, and restrict execution to certain task types.

Let’s explore how combining these flags enables highly targeted pipeline execution scenarios:

### Combining Tags and Downstream

Using `--tag` with `--downstream` filters the assets by tag and includes all downstream dependencies for those tagged assets. For example:
```bash
bruin run --tag important_tag --downstream
```
This will run all tasks of the assets tagged with `important_tag` and include all downstream tasks that depend on them.

### Combining Tags and Task Types
Using `--tag` with `--only` restricts the tasks to specific types for the assets filtered by the given tag. For example:
```bash
bruin run --tag quality_tag --only checks
```
This runs only the `checks` tasks for the assets tagged with `quality_tag` while excluding other task types.

### Combining Tags, Downstream, and Task Types

Adding `--only` to the mix  with `--tag` and `--downstream` allows you to specify the types of tasks to run for the tagged assets and their downstream dependencies. For example:
```bash
bruin run --tag critical_tag --downstream --only main
```
This command runs only the `main` tasks for the assets tagged with `critical_tag` and their downstream dependencies.
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








