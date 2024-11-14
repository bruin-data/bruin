# Bruin - Sample Pipeline

This pipeline is a simple example of a Bruin pipeline. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes three sample assets already:
- `chess_games.asset.yml`: Transfers chess game data from source database to DuckDB.
- `chess_profiles.asset.yml`: Transfers chess player profiles data from source to DuckDB.
- `player_summary.sql`:Creates a summary table of chess player stats, including games, wins, and win rates as white/black.

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/overview.html).

Here's a sample `.bruin.yml` file:

```yaml
environments:
    default:
        connections:
            duckdb:
                - name: "duckdb_default"
                  path: "/path/to/your/database.db"

            chess:
                - name: "chess_connection"
                  players:
                      - "MagnusCarlsen"
                      - "Hikaru"
```

You can simply switch the environment using the `--environment` flag, e.g.:


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

That's it, good luc