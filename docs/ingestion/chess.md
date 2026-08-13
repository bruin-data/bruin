# Chess

[chess](https://www.chess.com/) is an online platform offering chess games, tournaments, lessons, and more.

Bruin supports Chess as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Chess into your data warehouse.

In order to set up Chess connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Chess as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to Chess, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      chess:
        - name: "my-chess"
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

- `players`: A list of players usernames for which you want to fetch data.

::: warning DEPRECATED
Specifying `players` on the connection is deprecated and will be removed in a future release. Provide the players in the `source_table` instead (see [Specifying players in the source table](#specifying-players-in-the-source-table)). If both are set, the players in the `source_table` take precedence.
:::

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
- `source_table`: The name of the data table in Chess that you want to ingest. For example, `profiles` is the table of Chess that you want to ingest.

### Specifying players in the source table

Instead of listing players on the connection, you can pass them directly in the `source_table` value, appended after a colon as a comma-separated list:

```yaml
parameters:
  source_connection: my-chess
  source_table: 'profiles:MagnusCarlsen,Hikaru'

  destination: postgres
```

Players given this way take precedence over the connection's `players`, so a single connection can feed different players into different tables. This is the recommended approach; the connection-level `players` field is deprecated.

### Using Chess without a connection

Chess is a public API and needs no credentials, so you can skip the connection entirely. Set `source_connection: chess.com` and provide the players in the `source_table`; no `chess` entry is required in `.bruin.yml`:

```yaml
name: public.chess
type: ingestr
connection: postgres

parameters:
  source_connection: chess.com
  source_table: 'profiles:MagnusCarlsen,Hikaru'

  destination: postgres
```

If a connection with that same name is defined in `.bruin.yml`, it takes precedence and is used as before; the public source is only used when no such connection exists.

## Available Source Tables

| Table     | PK | Inc Key | Inc Strategy | Details                                                          |
|-----------|----|---------|--------------|-----------------------------------------------------------------|
| profiles  | -  | -       | replace      | Retrieves player profiles based on a list of player usernames. |
| games     | -  | -       | replace      | Retrieves players games for specified players.                 |
| archives  | -  | -       | replace      | Retrieves the URLs to game archives for specified players.     |

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/chess_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Chess table into your Postgres database.

<img width="1161" alt="chess" src="https://github.com/user-attachments/assets/12418c5b-5483-46fb-9bb3-998e112d8030">
