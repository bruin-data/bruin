---
outline: deep
---

# Quickstart
Make sure you have [installed](./installation.md) Bruin CLI already in your device.

> [!INFO]
> The steps here assume you have DuckDB installed. You can replace to any reference to `duckdb.sql` with your data warehouse if you'd like to run the examples elsewhere.

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

In a nutshell, an asset is anything that generates value with data. In practice, an asset can be a table in your data warehouse, a Python script, or a file in S3. Bruin represents assets in code, put under the `assets` folder.

In order to create a new asset, you can simply create a new file under the `assets` folder. Let's start with a SQL asset.

### Creating a `ingestr` asset
Let's start by ingesting some data from an external source.

ingestr is an [open-source CLI tool](https://github.com/bruin-data/ingestr) that allows ingesting data from any source into any destination.

You can create ingestr assets with a file `assets/players.asset.yml`

```yaml
name: dataset.players
type: ingestr
connection: duckdb
parameters:
  source_connection: chess
  source_table: players
```

The configuration in the YAML file has a few bits:
- `name`: the name of the asset, also the table that will store the ingested data
- `connection`: the destination that will be used to load the data
- `parameters`: the configuration for ingestr
  - `source_connection`: the connection that will be used for the source data
  - `source_table`: the table that will be ingested

That's it, this asset will load data from the `chess` source and load it into your DuckDB database.

You can run this asset either via the Bruin VS Code extension, or in the terminal:
```bash
bruin run assets/players.asset.yml
```

### Creating a SQL asset

Let's create a new SQL asset with a file `assets/player_stats.sql`:

```bruinsql
/* @bruin

name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table
   
depends:
   - dataset.players

@bruin */

SELECT name, count(*)
FROM dataset.players
GROUP BY 1
```

This asset have a few lines of configuration at the top:
- `name`: the name of the asset, needs to be unique within a pipeline
- `type`: `duckdb.sql` means BigQuery SQL, Bruin supports many other types of assets.
- `materialization`: take the query result and materialize it as a table

Bruin will take the result of the given query, and will create a `dataset.player_stats` table on DuckDB with it. You can also use `view`
materialization type instead of `table` to create a view instead.

> [!INFO]
> Bruin supports many asset types, including BigQuery, Snowflake, Python, Redshift, Databricks, and more.

You can run this asset either via the Bruin VS Code extension, or in the terminal:
```bash
bruin run assets/player_stats.sql
```

### Creating a Python asset
Similar to SQL, Bruin supports running Python natively as well.

You can create a Python asset with a file `assets/my_python_asset.py`:

```bruin-python
"""@bruin
name: my_python_asset
@bruin"""

print('hello world')
```

- The sections between `"""@bruin` and `@bruin"""` are comment markers, they are the configuration for the asset.
- The rest of the body is a regular Python script.

You can run this asset either via the Bruin VS Code extension, or in the terminal:
```bash
bruin run assets/my_python_asset.sql
```

At this point, you can also run the whole pipeline:
```bash
bruin run
```

## Data Quality Checks
Bruin supports data quality checks natively, as part of the asset definition. It includes a handful of data quality checks, and it also supports custom checks.

Let's add a few data quality checks to our table:
```bruinsql
/* @bruin

name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table
   
depends:
   - dataset.players

# you can define column metadata and quality checks
columns: // [!code ++]
  - name: name // [!code ++]
    type: string // [!code ++]
    description: this column contains the player names // [!code ++]
    checks: // [!code ++]
      - name: not_null // [!code ++]
      - name: unique // [!code ++]
  - name: player_count // [!code ++]
    type: int // [!code ++]
    description: the number of players with the given name // [!code ++]
    checks: // [!code ++]
      - name: not_null // [!code ++]
      - name: positive // [!code ++]

# you can also define custom checks 
custom_checks: // [!code ++]  
  - name: row count is greater than zero // [!code ++]  
    description: this check ensures that the table is not empty // [!code ++]  
    query: SELECT count(*) > 1 FROM dataset.player_count // [!code ++]  
   
@bruin */

SELECT name, count(*)
FROM dataset.players
GROUP BY 1
```

We have added a new `columns` section in the asset, it contains a list of all the columns and the checks applied to them.

Under the `checks` section, each column defines some quality checks:
- `name` column is marked to be not null and unique
- `player_count` column is marked to be not null and consisting of positive numbers

Under the `custom_checks` section, we have added a new custom quality check that checks if the table is empty or not.

You can refresh the asset & run the quality checks via simply running the asset:
```bash
bruin run assets/player_stats.sql
```

If you'd like to run only the checks, you can run:
```bash
bruin run --only checks assets/player_stats.sql
```

## Next steps
You have created your first pipeline, ingested data from a new source, added a bunch of assets, and ran quality checks on that, you are ready to dig deeper. Jump into the [Concepts](../concepts.md) to learn more about the underlying concepts Bruin use for your data pipelines. 