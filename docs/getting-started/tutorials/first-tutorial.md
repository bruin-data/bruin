# Getting Started with Your First Bruin Pipeline: Chess Data to DuckDB

Welcome! 👋 This guide is your starting point for building data pipelines with Bruin—beginning with an easy, hands-on project. You'll learn how to set up a pipeline that fetches chess data from a simple, no-credentials-needed API and stores it in DuckDB. The chess API provides a hassle-free way to work with real-world data, making it the perfect starting project for experimenting with Bruin. Think of this as your playground for exploring Bruin’s capabilities.

**By the end of this tutorial, you’ll:**

- Understand the basics of Bruin CLI
- Build a pipeline that ingests data from the chess API
- Store and query data in DuckDB, ready for analysis or integration

Whether you're new to Bruin, pipelines, or even data engineering in general, this guide is designed to help you take your first steps confidently. You’ll finish with a working pipeline that you can use as a foundation for more advanced projects. Let's dive in and start building! 🚀

## Prerequisites

Before you start, make sure you have the Bruin CLI installed. [Get instructions](../introduction/installation.md).

## Step 1: Initialize Your Pipeline

Run the `bruin init chess` command to set up your Bruin project using the `chess` template. This template brings in the necessary project structure and configuration files for a pipeline specific to chess data.

```bash
bruin init chess
```

After running the command you will see the following folder appear on your project :

```plaintext
chess/
├─ assets/
│  ├─ chess_games.asset.yml
│  ├─ chess_profiles.asset.yml
│  ├─ player_summary.sql
│
├─ .bruin.yml
├─ pipeline.yml  
├─ .gitignore
└─ README.md
```

## Step 2: Edit Your `.bruin.yml` file

After initializing your project with `bruin init`, edit the `.bruin.yml` file to configure your environments and connections. This file specifies the default environment settings and connections your pipeline will use.

Add or modify the `.bruin.yml` file as follows:

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
            - "rpragchess"
            - "legendisback1"

```

## Step 3: Take a look at your assets

Since we initialized our project with the `chess template`, the necessary assets are already included in the assets folder,
pre-configured for chess data ingestion. Open this file to take a look at your assets, you should see :

- **`chess_games.asset.yml`**: Ingests chess game data from the source connection into DuckDB.
- **`chess_profiles.asset.yml`**: Ingests player profile data from the source connection into DuckDB.
- **`player_summary.sql`**: A SQL asset that creates a summary table of player statistics, aggregating game results, wins, and win rates for each player based on the ingested data.

> [!INFO]
> **What is an Asset?**  
> An asset in Bruin is a configuration that details specific data processing steps, such as ingestion or transformation. Learn more about [Bruin assets](../../assets/definition-schema.md).

## Step 4: Check Your `pipeline.yml` file

Just like in Step 3, the pipeline.yml file also comes pre-configured for our task, and it follows this structure:

### Example `pipeline.yml`

```yaml
name: chess_duckdb
default_connections:
    duckdb: "duckdb-default"
    chess: "chess-default"
```

> [!INFO]
> **What is a Pipeline?**  
>A pipeline is a group of assets that are executed together in the right order.  Learn more about [Bruin Pipelines](../concepts.md#pipeline).
>
## Step 5: Run Your Pipeline and Ingest the Data

Now that your pipeline is set up, you're ready to run it and ingest the data into DuckDB. Use the following command to execute the pipeline:

```bash
bruin run ./chess/pipeline.yml
```

<img alt="Chess - Pipeline" src="/chesspipeline.gif" />

## Step 6: Query Your Data in DuckDB

Now that the data is in DuckDB, you can query it to verify the results. Open a terminal and run the following commands to inspect your data:

```bash
bruin query --c duckdb-default  --q "SELECT * FROM chess_playground.player_summary LIMIT 10;"
```

After executing the query, you will see the following results displayed in your terminal in a clear and organized table format:

```plaintext
┌───────────────┬────────────────────────────────────────────────┬─────────────┬────────────┬────────────┬─────────────┬─────────────┬────────────────┬────────────────┐
│ USERNAME      │ AID                                            │ TOTAL_GAMES │ WHITE_WINS │ BLACK_WINS │ WHITE_GAMES │ BLACK_GAMES │ WHITE_WIN_RATE │ BLACK_WIN_RATE │
├───────────────┼────────────────────────────────────────────────┼─────────────┼────────────┼────────────┼─────────────┼─────────────┼────────────────┼────────────────┤
│ legendisback1        │ https://api.chess.com/pub/player/legendisback1        │ 102         │ 39         │ 41         │ 50          │ 52          │ 78             │ 78.85          │
│ rpragchess │ https://api.chess.com/pub/player/rpragchess │ 17          │ 6          │ 6          │ 9           │ 8           │ 66.67          │ 75             │
└───────────────┴────────────────────────────────────────────────┴─────────────┴────────────┴────────────┴─────────────┴─────────────┴────────────────┴────────────────┘
```

🎉Congratulations!

You've successfully created and run your first Bruin pipeline! Your chess data is now ingested into DuckDB, ready for you to query and explore. This is just the beginning—feel free to extend this pipeline, add more data sources, or incorporate data transformations to suit your needs. For more advanced features and customization, check out the Bruin documentation.

Happy data engineering! 🚀
