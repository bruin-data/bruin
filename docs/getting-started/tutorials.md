# Getting Started with Your First Bruin Pipeline: Chess Data to DuckDB

In this guide, you'll learn how to create a simple pipeline that ingests chess data and stores it in DuckDB using Bruin.

## Prerequisites
Before you start, make sure you have the Bruin CLI installed. [Get instructions](./introduction/installation.md).



## Step 1: Initialize Your Pipeline Project

Run the `bruin init chess` command to set up your Bruin project using the `chess` template.
This template brings in the necessary project structure and configuration files for a pipeline specific to chess data.

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
                  path: "/path/to/your/database.db"

            chess:
                - name: "chess-default"
                  players:
                      - "MagnusCarlsen"
                      - "Hikaru"
```
## Step 3: Take a look at your assets
Since we initialized our project with the `chess template`, the necessary assets are already included in the assets folder,
pre-configured for chess data ingestion. Open this file to take a look at your assets, you should see :

- **`chess_games.asset.yml`**: Ingests chess game data from the source connection into DuckDB.
- **`chess_profiles.asset.yml`**: Ingests player profile data from the source connection into DuckDB.
- **`player_summary.sql`**: A SQL asset that creates a summary table of player statistics, aggregating game results, wins, and win rates for each player based on the ingested data.
> [!INFO]
> **What is an Asset?**  
> An asset in Bruin is a configuration that details specific data processing tasks, such as ingestion, transformation. Learn more about [Bruin assets](../assets/definition-schema.md).

## Step 4: Check Your `pipeline.yml` file
Just like in Step 3, the pipeline.yml file also comes pre-configured for our task, and it follows this structure:
### Example `pipeline.yml`:
```yaml
name: chess_duckdb
default_connections:
    duckdb: "duckdb_default"  
    chess: "chess_connection"
```
> [!INFO]
> **What is a Pipeline?**  
>A pipeline is a group of assets that are executed together in the right order.  Learn more about [Bruin Pipelines](concepts.md#pipeline).
## Step 5: Run Your Pipeline and Ingest the Data
Now that your pipeline is set up, you're ready to run it and ingest the data into DuckDB. Use the following command to execute the pipeline:

```bash
bruin run ./chess/pipeline.yml
```

## Step 6: Query Your Data in DuckDB
Now that the data is in DuckDB, you can query it to verify the results. Open a terminal and run the following commands to inspect your data:


```bash
bruin fetch query --connection duckdb_default  --query "SELECT * FROM chess_playground.player_summary LIMIT 10;"
```

Congratulations!

You've successfully created and run your first Bruin pipeline! Your chess data is now ingested into DuckDB, ready for you to query and explore. This is just the beginning—feel free to extend this pipeline, add more data sources, or incorporate data transformations to suit your needs.

For more advanced features and customization, check out the Bruin documentation. Happy data engineering!
