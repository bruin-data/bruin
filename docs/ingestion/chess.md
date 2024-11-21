# Chess
[chess](https://www.chess.com/) is an online platform offering chess games, tournaments, lessons, and more.

Bruin supports Chess as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Chess into your data warehouse.

In order to set up Chess connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Chess as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Chess, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      chess:
        - name: "my-chess"
          players:
            - "MagnusCarlsen"
            - "Hikaru"
```
- `players`: A list of players usernames for which you want to fetch data.

### Step 2: Create an asset file for data ingestion

To ingest data from Chess, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., chess_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.chess
type: ingestr
connection: postgres

parameters:
  source_connection: my-chess
  source_table: 'profiles'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Chess connection defined in .bruin.yml.
- `source_table`: The name of the data table in Chess that you want to ingest. For example, `profiles` is the table of Chess that you want to ingest.You can find the available source tables in Chess [here](https://bruin-data.github.io/ingestr/supported-sources/chess.html#tables).

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/chess_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Chess table into your Postgres database.