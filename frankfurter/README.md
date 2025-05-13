# Bruin - Frankfurter Template

This pipeline is a simple example of a Bruin pipeline. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes three sample assets already:
- `rates.asset.yml`: An ingestr asset which transfers exchange rate data from source to DuckDB.
- `currencies.asset.yml`: An ingestr asset which copies the list of available currencies.
- `currency_performance.sql`: An SQL-asset which shows the latest exchange rates as well as each currency's performance over the past 30 days. 

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/overview.html).

Here's a sample `.bruin.yml` file:

```yaml
default_environment: default
environments:
  default:
    connections:
      frankfurter:
        - name: "frankfurter-default"
      duckdb:
        - name: "duckdb-default"
          path: "duckdb.db"
```

You can simply switch the environment using the `--environment` flag, e.g.:


## Running the pipeline

bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
bruin run ./frankfurter/pipeline.yml
```

You can also run a single task:

```shell
bruin run assets/hello.py                            
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:59Z] [worker-0] Running: hello
[2023-03-16T18:26:00Z] [worker-0] [hello] >> Hello, world!
[2023-03-16T18:26:00Z] [worker-0] Completed: hello (103ms)


Executed 1 tasks in 103ms
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!