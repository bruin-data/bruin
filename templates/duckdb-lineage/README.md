# Bruin - Sample Pipeline

This pipeline is a simple example of a Bruin pipeline for DuckDB, demonstrating a data transformation workflow with multiple interconnected SQL assets. The pipeline features table materializations with dependencies between assets and schema definitions including `primary_key` constraints.

The example includes:
- `users.sql`: A base table with defined schema constraints and column descriptions
- `people.sql` and `country.sql`: Intermediate tables that depend on the users table
- `example.sql`: A final joined view combining people and country data

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/gorgias.html).

Here's a sample `.bruin.yml` file:


```yaml
environments:
  default:
    connections:
      duckdb:
        - name: "duckdb_default"
          path: "/path/to/your/database.db"
      
```

## Running the pipeline

bruin CLI can run the whole pipeline or any task with the downstreams:

```shell

bruin run ./duckdb/pipeline.yml
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