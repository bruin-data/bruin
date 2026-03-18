# Dune

[Dune](https://dune.com/) is a blockchain analytics platform that provides access to on-chain data through SQL queries and a powerful API.

Bruin supports Dune as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Dune into your data warehouse.

In order to set up Dune connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need `api_key`. For details on how to obtain these credentials, please refer [here](https://docs.dune.com/api-reference/overview/authentication).

Follow the steps below to correctly set up Dune as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to Dune, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
    dune:
        - name: 'my-dune'
          api_key: 'your_dune_api_key'
```

- `api_key`: the API key used for authentication with the Dune API

### Step 2: Create an asset file for data ingestion

To ingest data from Dune, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., dune_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.dune
type: ingestr
connection: postgres

parameters:
  source_connection: my-dune
  source_table: 'queries'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: "postgres" indicates that the ingested data will be stored in a PostgreSQL database.
- `source_connection`: The name of the Dune connection defined in .bruin.yml.
- `source_table`: The source table to ingest. Supports the following formats:

| Format | Example | Description |
|--------|---------|-------------|
| `queries` | `queries` | Lists all saved queries |
| `query:<id>` | `query:1234567` | Executes a saved query by its numeric ID |
| `query:<id>:<params>` | `query:1234567:bar=1000&foo=value` | Executes a saved query with query parameters |
| `sql:<raw SQL>` | `sql:SELECT * FROM ethereum.transactions LIMIT 100` | Executes raw SQL directly |

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/dune_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Dune source into your Postgres database.
