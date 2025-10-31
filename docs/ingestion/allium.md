# Allium

[Allium](https://allium.so/) is a blockchain data platform that provides access to indexed blockchain data through a powerful query interface.

Bruin supports Allium as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Allium into your data platform.

In order to set up Allium connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. You need the `api_key` for authentication.

To get your Allium API credentials:
1. Sign up for an Allium account at [allium.so](https://allium.so/)
2. Navigate to your account settings
3. Generate an API key
4. Find your query ID from the Allium explorer interface

Follow the steps below to correctly set up Allium as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Allium, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
  allium:
    - name: my-allium
      api_key: "your_api_key"
```

- `api_key`: The API key used for authentication with the Allium API (required)

### Step 2: Create an asset file for data ingestion

To ingest data from Allium, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., allium_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.allium
type: ingestr
connection: postgres

parameters:
  source_connection: my-allium
  source_table: 'query:abc123def456'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Allium connection defined in .bruin.yml.
- `source_table`: The query ID from Allium explorer in the format `query:your_query_id`. Each query ID represents a specific blockchain data query that you've created in the Allium explorer.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| `query:<query_id>` | - | - | replace | Allium source uses query IDs as table identifiers. Format must be `query:abc123def456` where the query ID is from your Allium explorer. |



### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/allium_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Allium query into your Postgres database.

## Notes

- Query execution is asynchronous and may take time depending on the complexity of your query
- The connector will wait up to 5 minutes for query completion
- Make sure your query ID is valid and accessible with your API key
- The source table format must be `query:your_query_id`
