# Bruin - Sample Pipeline

Congrats! 🎉 You just created your first Bruin Pipeline!

This pipeline is a simple example of a Bruin project. It demonstrates how to use the `bruin` CLI to build and run a pipeline.
DuckDB was chosen for its simplicity. This setup assumes DuckDB is available; you can swap `duckdb.sql` asset types.

The pipeline includes the following sample assets:
- `dataset.players`: An ingestr asset that loads chess player data into DuckDB.
- `dataset.player_stats`: A DuckDB SQL asset that builds a table from `dataset.players`.
- `my_python_asset`: A Python asset that prints a message.

## Setup
This template includes a `.bruin.yml` with sample DuckDB and chess connections. You can replace or extend with your connections and environments as needed.

Here's a sample `.bruin.yml` file:

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
            - "MagnusCarlsen"
            - "Hikaru"
```

You can simply switch the environment using the `--environment` flag, e.g.:

```shell
bruin validate --environment production . 
```

## Running the pipeline

bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
bruin run .
```

```shell
Starting the pipeline execution...

[18:42:58] Running:  my_python_asset
[18:42:58] Running:  dataset.players
[18:42:58] [my_python_asset] >> warning: `--no-sync` has no effect when used outside of a project
[18:42:58] [my_python_asset] >> hello world
[18:42:58] Finished: my_python_asset (191ms)
⋮
[18:43:04] Finished: dataset.player_stats:player_count:not_null (24ms)
[18:43:04] Finished: dataset.player_stats:player_count:positive (33ms)
[18:43:04] Finished: dataset.player_stats:name:unique (42ms)

==================================================

PASS my_python_asset 
PASS dataset.players 
PASS dataset.player_stats .....


bruin run completed successfully in 5.439s

 ✓ Assets executed      3 succeeded
 ✓ Quality checks       5 succeeded
```

You can also run a single task:

```shell
bruin run assets/my_python_asset.py                         
```

```shell
Starting the pipeline execution...

[23:00:02] Running:  my_python_asset
[23:00:02] >> warning: `--no-sync` has no effect when used outside of a project
[23:00:02] >> hello world
[23:00:02] Finished: my_python_asset (162ms)

==================================================

PASS my_python_asset 


bruin run completed successfully in 162ms

 ✓ Assets executed      1 succeeded
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, you are all set. Happy Building!

If you want to dig deeper, jump into the [Concepts](https://getbruin.com/docs/bruin/getting-started/concepts.html) to learn more about the underlying concepts Bruin use for your data pipelines.