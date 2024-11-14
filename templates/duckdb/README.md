# Bruin - Sample Pipeline

This pipeline is a simple example of a Bruin pipeline for DuckDB, 
featuring `example.sql`—a SQL asset that creates a table with sample data and enforces schema constraints 
like `not_null`, `unique`, and `primary_key`.

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

bruin run .
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:14Z] [worker-0] Running: dashboard.bruin-test
[2023-03-16T18:25:16Z] [worker-0] Completed: dashboard.bruin-test (1.681s)
[2023-03-16T18:25:16Z] [worker-4] Running: hello
[2023-03-16T18:25:16Z] [worker-4] [hello] >> Hello, world!
[2023-03-16T18:25:16Z] [worker-4] Completed: hello (116ms)

Executed 2 tasks in 1.798s
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