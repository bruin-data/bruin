# Chess
[chess](https://www.chess.com/) is an online platform offering chess games, tournaments, lessons, and more.

ingestr supports Chess as a source for [ingestr assets](https://bruin-data.github.io/bruin/assets/ingestr.html), allowing you to ingest data from Chess into your data warehouse.
It is designed to play around with the data of players, games, and more since it doesn't require any authentication.

In order to set up Chess connection, you need to add a configuration item in the `.bruin.yml` and in `asset` file.

Follow the steps below to correctly set up Chess as a data source and run ingestion.

**Step 1: Add a Connection to .bruin.yml**

To connect to Chess, you need to add a configuration item to the connections section of the .bruin.yml file. This configuration must comply with the following schema:

```yaml
    connections:
      chess:
        - name: "my-chess"
          players:
            - "MagnusCarlsen"
            - "Hikaru"
```
- players: A list of players usernames for which you want to fetch data.

**Step 2: Create an Asset File for Data Ingestion**

To ingest data from Chess, you need to create an [asset configuration](https://bruin-data.github.io/bruin/assets/ingestr.html#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., chess_ingestion.yml) and add the following content:

```yaml
name: public.chess
type: ingestr
connection: postgres

parameters:
  source_connection: my-chess
  source_table: 'profiles'

  destination: postgres
```

**Step 3: [Run](https://bruin-data.github.io/bruin/commands/run.html) Asset to Ingest Data**
```
bruin run --file chess_ingestion.yml
```
It will ingest chess data to postgres. 