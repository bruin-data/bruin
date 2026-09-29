# Bruin - Chess Template

This pipeline is a simple example of a Bruin pipeline. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes three sample assets already:

- `chess_games.asset.yml`: Transfers chess game data from source database to DuckDB.
- `chess_profiles.asset.yml`: Transfers chess player profiles data from source to DuckDB.
- `player_summary.sql`:Creates a summary table of chess player stats, including games, wins, and win rates as white/black.

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

            chess:
                - name: "chess-default"
                  players:
                      - "FabianoCaruana"
                      - "Hikaru"
                      - "MagnusCarlsen"
                      - "GothamChess"
                      - "DanielNaroditsky"
                      - "AnishGiri"
                      - "Firouzja2003"
                      - "LevonAronian"
                      - "WesleySo"
                      - "GarryKasparov"
```

You can simply switch the environment using the `--environment` flag, e.g.:

## Running the pipeline

Run these commands from the generated `chess` pipeline directory. To run the whole pipeline:

```shell
bruin run .
```

You can also run a single task:

```shell
bruin run assets/player_summary.sql
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!
