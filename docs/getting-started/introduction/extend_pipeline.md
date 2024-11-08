# Extend Your Pipeline: Adding SQL Data Transformations

In this tutorial, we'll build upon the pipeline you previously created that ingests chess data into DuckDB. We'll extend it by adding SQL data transformations to process the data before further analysis.

## Prerequisites
Before starting, ensure you have:
- Completed the [Getting Started: Chess Data to DuckDB](link-to-getting-started-guide) tutorial.

## Step 1: Review Your Current Pipeline
If you've completed the previous tutorial, your pipeline should have:
- A `pipeline.yml` file that references the `chess_ingestion.asset.yml` asset.
- Connections configured in `bruin.yml` for DuckDB and the chess data source.



## Step 2: Create a New Transformation Asset
To extend the pipeline with SQL data transformations, create a new asset file called `chess_transform.asset.yml` in your `assets` folder:

```yaml
name: transformed.chess_data
type: sql
parameters:
  source:
    connection: duckdb_default
    table: chess_games
  query: |
    SELECT 
      player,
      COUNT(*) AS total_games,
      SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS wins,
      SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) AS losses
    FROM chess_games
    GROUP BY player
  destination:
    connection: duckdb_default
    table: chess_player_summary
```

## Step 3: Run Your Extended Pipeline

Execute the updated pipeline with:
```bash
bruin run ./bruin-default/pipeline.yml
```

### Congratulations!

You've successfully extended your pipeline to include SQL data transformations. 
Your data has now been processed, aggregated, and stored in DuckDB for further exploration.