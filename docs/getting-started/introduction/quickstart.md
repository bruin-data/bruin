---
outline: deep
---

# Quickstart
Make sure you have [installed](./installation.md) Bruin CLI already in your device.

## Create your first pipeline

Bruin includes a handy command called `init`, you can simply run that command to create a new Bruin project.
      
```bash
bruin init default my-pipeline   
```

This command will:
- Create a project named `my-pipeline`.
- Generate a folder called `my-pipeline` containing the following:
    - An `assets` folder
    - `.bruin.yml` file (where you will add connection and credentials )
    - `pipeline.yml` file to manage your pipeline.


Once you have the project structure, you can run the whole pipeline:
```
bruin run
```

## Adding a new asset

Adding a new asset is as simple as creating a new file inside the `assets` folder. For example, let's create a new SQL asset `assets/bruin_test.sql`:

```sql
/* @bruin
name: dataset.bruin_test
type: bq.sql
materialization:
  type: table
@bruin */

SELECT 1 as result
```

bruin will take this result, and will create a `dataset.bruin_test` table on BigQuery. You can also use `view`
materialization type instead of `table` to create a view instead.

> [!INFO]
> Bruin supports many asset types, including Snowflake, Python, Redshift, Databricks, and more.
