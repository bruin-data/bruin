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

<style>
table {
  width: 100%;
}
table th:first-child,
table td:first-child {
  white-space: nowrap;
}
</style>

## Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--downstream` | bool | `false` | Run all downstream tasks as well. |
| `--start-date` | str | Beginning of yesterday | The start date of the range the pipeline will run for. Format: `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, or `YYYY-MM-DD HH:MM:SS.ffffff` |
| `--end-date` | str | End of yesterday | The end date of the range the pipeline will run for. Format: `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, or `YYYY-MM-DD HH:MM:SS.ffffff` |
| `--environment` | str | - | The environment to use. |
| `--force` | bool | `false` | Do not ask for confirmation in a production environment. |
| `--full-refresh` | bool | `false` | Truncate the table before running. |
| `--no-log-file` | bool | `false` | Do not create a log file for this run. |
| `--only` | []str | `main`, `checks` | Limit the types of tasks to run. Options: `main`, `checks`, `push-metadata`. |
| `--push-metadata` | bool | `false` | Push metadata to the destination database if supported (currently BigQuery). |
| `--tag` | str | - | Pick assets with the given tag. |
| `--exclude-tag` | []str | - | Exclude assets with the given tag. |
| `--workers` | int | `16` | Number of workers to run tasks in parallel. |
|  `--continue` | bool | `false` | Continue from the last failed asset. |
| `--config-file` | str | - | The path to the `.bruin.yml` file. |


### Continue from the last failed asset

If you want to continue from the last failed task, you can use the `--continue` flag. This will run the pipeline/asset from the last failed task. Bruin will automatically retrive all the flags used in the last run. 

```bash
bruin run --continue 
```

> [!NOTE]
> This will only work if the pipeline structure is not changed. If the pipeline structure has changed in any way, including asset dependencies, you will need to run the pipeline/asset from the beginning. This is to ensure that the pipeline/asset is run in the correct order.

### Focused Runs: Filtering by Tags and Task Types
As detailed in the flag section above, the  `--tag`, `--downstream`, `--exclude-tag`, and `--only` flags provide powerful ways to filter and control which tasks in your pipeline are executed. These flags can also be combined to fine-tune pipeline runs, allowing you to execute specific subsets of tasks based on tags, include their downstream dependencies, and restrict execution to certain task types.

Let's explore how combining these flags enables highly targeted pipeline execution scenarios:


### Combining Tags and Task Types
Using `--tag` with `--only` restricts the tasks to specific types for the assets filtered by the given tag. For example:
```bash
bruin run --tag quality_tag --only checks
```
This runs only the `checks` tasks for the assets tagged with `quality_tag` while excluding other task types.

### Combining Exclude Tag and Task Types
Using `--exclude-tag` with `--only` allows you to run specific task types while excluding assets with certain tags. For example:
```bash
bruin run --exclude-tag quality_tag --only checks
```
This runs the `checks` tasks for all assets EXCEPT those tagged with `quality_tag`. This is useful when you want to skip certain assets while running specific task types.

### Combining Tag and Exclude-Tag
Using `--tag` with `--exclude-tag` allows you to include specific assets and then exclude certain ones based on another tag. For example:
```bash
bruin run --tag important_tag --exclude-tag quality_tag
```
This command will run tasks for assets tagged with `important_tag` but will exclude those that also have the `quality_tag`. This is useful for focusing on a subset of assets while excluding others that meet certain criteria.


### Combining Downstream and Other Filtering Flags
The `--downstream` flag can be used when running a single asset. You can combine it with other flags like `--exclude-tag` and `--only` to refine your task execution. For example:

- **Using `--downstream` with `--exclude-tag`:**
  ```bash
  bruin run --downstream --exclude-tag quality_tag
  ```
  This command will run tasks for a single asset and exclude any tasks for assets tagged with `quality_tag`.

- **Using `--downstream` with `--only`:**
  ```bash
  bruin run --downstream --only checks
  ```
  This command will run only the `checks` tasks for a single asset, allowing you to focus on specific task types.

These combinations provide flexibility in managing task execution by allowing you to exclude certain assets or focus on specific task types while using the `--downstream` flag.



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








