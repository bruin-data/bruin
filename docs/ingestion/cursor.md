# Cursor

[Cursor](https://cursor.com/) is an AI-powered code editor built for productivity.

Bruin supports Cursor as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Cursor API into your data warehouse.

In order to set up Cursor connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. You need an API key from your Cursor team settings.

Follow the steps below to correctly set up Cursor as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Cursor, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
  cursor:
    - name: "my-cursor"
      api_key: "your_api_key_here"
```

- `api_key`: Your Cursor API key (required)

### Step 2: Create an asset file for data ingestion

To ingest data from Cursor, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., cursor_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.cursor
type: ingestr
connection: postgres

parameters:
  source_connection: my-cursor
  source_table: 'team_members'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Cursor connection defined in .bruin.yml.
- `source_table`: The name of the table in Cursor you want to ingest. See available tables below.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| `team_members` | - | - | replace | Team member information including names, emails, and roles |
| `daily_usage_data` | - | - | replace | Daily usage statistics including lines added/deleted, AI requests, model usage. Supports optional date filtering |
| `team_spend` | - | - | replace | Team spending data for the current billing cycle |
| `filtered_usage_events` | - | - | replace | Detailed usage events with timestamps, models, token usage, and costs. Supports optional date filtering |

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/cursor_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Cursor table into your Postgres database.

## Notes

- **Authentication**: The Cursor API uses API key authentication.
- **Date Range Limit**: The `daily_usage_data` and `filtered_usage_events` endpoints have a 30-day limit per request. If you need more than 30 days of historical data, make multiple requests with different date ranges.
- **Date Filtering**: `daily_usage_data` and `filtered_usage_events` tables support optional date filtering. When dates are provided, only data within that range is fetched. When dates are omitted, the API returns default data (typically last 30 days).

