# Bruin - DuckDB Template

This pipeline is a simple example of a Bruin pipeline for DuckDB,
featuring `example.sql`—a SQL asset that creates a table with sample data and enforces schema constraints
like `not_null`, `unique`, and `primary_key`.

## Setup

Add your connections and environments to the `.bruin.yml` file at your project root, not inside the pipeline folder. You can read more about connections [here](https://getbruin.com/docs/bruin/commands/connections.html).

Here's a sample `.bruin.yml` file:

```yaml
environments:
  default:
    connections:
      duckdb:
        - name: "duckdb-default"
          path: "/path/to/your/database.db"
      
```

## Running the pipeline

Run these commands from the generated `duckdb` pipeline directory. To run the whole pipeline:

```shell
bruin run .
```

You can also run a single task:

```shell
bruin run assets/example.sql
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!
