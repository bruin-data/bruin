---
outline: deep
---

# Quickstart
Make sure you have [installed](./installation.md) Bruin CLI already in your device.

> [!INFO]
> The steps here assume you have [DuckDB](https://duckdb.org/install/) installed. You can replace to any reference to `duckdb.sql` with your data warehouse if you'd like to run the examples elsewhere.

## Create your first pipeline

Bruin includes a handy command called `init`, you can simply run that command to create a new Bruin project. [learn more](../../commands/init.md)
      
```bash
bruin init default my-pipeline  
cd my-pipeline
```

This command will:
- Create a project named `my-pipeline`.
- Generate a folder called `my-pipeline` containing the following:
    - `assets/` with four sample assets: ingestr, Python, R, and SQL (we will walk through them below)
    - `.bruin.yml` with sample duckdb connections used in this guide (you can edit it or use a secrets manager, see [Secrets](../../secrets/overview.md))
    - `pipeline.yml` file to manage your pipeline.


Once you have the project structure, you can run the whole pipeline:
```bash
bruin run
```

## Exploring the sample assets

In a nutshell, an asset is anything that generates value with data. In practice, an asset can be a table in your data warehouse, a Python script, or a file in S3. Bruin represents assets in code, put under the `assets` folder.

The `default` template already includes a 4 sample assets under `assets/`. In the next sections, we will walk through each one. You can use the same patterns to add new assets by creating new files under `assets/`.

### Creating a `ingestr` asset
Let's start by ingesting some data from an external source.

ingestr is an [open-source CLI tool](https://github.com/bruin-data/ingestr) that allows ingesting data from any source into any destination.

The template includes an ingestr asset in `assets/players.asset.yml`:

```yaml
name: dataset.players
type: ingestr

parameters:
  destination: duckdb
  source_connection: chess-default
  source_table: profiles

```

The configuration in the YAML file has a few bits:
- `name`: the name of the asset, also the table that will store the ingested data
- `connection`: the destination that will be used to load the data
- `parameters`: the configuration for ingestr
  - `source_connection`: the connection that will be used for the source data
  - `source_table`: the table that will be ingested

That's it, this asset will load data from the `chess` source and load it into your DuckDB database.

### Setting up your `.bruin.yml` file
This file specifies environments and the connections your pipeline will use. To ensure assets you add work correctly, configure your environments and connections by editing your `.bruin.yml` file. 
If you used `bruin init default`, this is already included. Otherwise, add the following:
```yaml
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: "duckdb-default"
          path: "duckdb.db"
      chess:
        - name: "chess-default"
          players:
            - "erik"
            - "vadimer2"

```

_Bear_ in mind, there are other ways to provide credentials and secrets. See [Secrets](../../secrets/overview.md).

You can run this asset either via the Bruin VS Code extension, or in the terminal:
```bash
bruin run assets/players.asset.yml
```

### Creating a SQL asset

The template includes a SQL asset in `assets/player_stats.sql`:
The file in your directory might include extra sections such as `columns`, `checks`, or `custom_checks`. Later in this guide, we will cover adding quality checks.

```bruin-sql
/* @bruin

name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table
   
depends:
   - dataset.players

@bruin */

SELECT name, count(*) AS player_count
FROM dataset.players
GROUP BY 1
```

This asset has a few lines of configuration at the top:
- `name`: the name of the asset, needs to be unique within a pipeline
- `type`: `duckdb.sql` means DuckDB SQL, Bruin supports many other types of assets.
- `materialization`: take the query result and materialize it as a table

Bruin will take the result of the given query, and will create a `dataset.player_stats` table on DuckDB with it. You can also use `view`
materialization type instead of `table` to create a view instead.

> [!INFO]
> Bruin supports many [asset](/assets/definition-schema.html) types, including BigQuery, Snowflake, Python, R, Redshift, Databricks, and more.

You can run this asset either via the Bruin VS Code extension, or in the terminal:
```bash
bruin run assets/player_stats.sql
```

### Creating a Python asset
Similar to SQL, Bruin supports running Python natively as well.

The template includes a Python asset in `assets/my_python_asset.py`:

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
bruin run assets/my_python_asset.py
```

### Creating an R asset
Similar to SQL and Python, Bruin supports running R natively as well.

The template includes an R asset in `assets/my_r_asset.r`:

```r
"@bruin
name: my_r_asset
type: r
@bruin"

cat("Hello from R!\n")
result <- 2 + 2
cat(sprintf("2 + 2 = %d\n", result))
```

- The sections between `"@bruin` and `@bruin"` contain YAML configuration for the asset using R's multiline string syntax.
- You can use either double quotes `"` or single quotes `'` - just make sure they match.
- The rest of the file is a regular R script.

You can run this asset either via the Bruin VS Code extension, or in the terminal:
```bash
bruin run assets/my_r_asset.r
```

At this point, you can also run the whole pipeline:
```bash
bruin run
```

## Data Quality Checks
Bruin supports data quality checks natively, as part of the asset definition. It includes a handful of data quality checks, and it also supports custom checks.

The default template already includes some example data quality checks on `dataset.player_stats`. If you'd like to add or change checks, this is what the asset definition can look like:
```bruin-sql
/* @bruin

name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table
   
depends:
   - dataset.players

# you can define column metadata and quality checks
columns: # [!code ++]
  - name: name # [!code ++]
    type: string # [!code ++]
    description: this column contains the player names # [!code ++]
    checks: # [!code ++]
      - name: not_null # [!code ++]
      - name: unique # [!code ++]
  - name: player_count # [!code ++]
    type: int # [!code ++]
    description: the number of players with the given name # [!code ++]
    checks: # [!code ++]
      - name: not_null # [!code ++]
      - name: positive # [!code ++]

# you can also define custom checks 
custom_checks: # [!code ++]
  - name: row count is greater than zero # [!code ++]  
    description: this check ensures that the table is not empty # [!code ++]  
    query: SELECT count(*) > 1 FROM dataset.player_stats # [!code ++]  
    value: 1 # [!code ++]
   
@bruin */

SELECT name, count(*) AS player_count
FROM dataset.players
GROUP BY 1
```

The asset includes a `columns` section that contains a list of columns and the checks applied to them.

Under the `checks` section, each column defines some quality checks:
- `name` column is marked to be not null and unique
- `player_count` column is marked to be not null and consisting of positive numbers

Under the `custom_checks` section, there is a custom quality check that verifies the table is not empty.

You can refresh the asset & run the quality checks via simply running the asset:
```bash
bruin run assets/player_stats.sql
```

If you'd like to run only the checks, you can run:
```bash
bruin run --only checks assets/player_stats.sql
```

## Next steps
You have created your first pipeline, ingested data from a new source, explored the sample assets, and ran quality checks. You are ready to dig deeper. Jump into the [Concepts](../concepts.md) to learn more about the underlying concepts Bruin uses for your data pipelines.
