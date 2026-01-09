# Fireflies

[Fireflies.ai](https://fireflies.ai/) is an AI-powered meeting assistant that automatically records, transcribes, and analyzes voice conversations from meetings across various video conferencing platforms.

Bruin supports Fireflies as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Fireflies into your data warehouse.

In order to set up Fireflies connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need an `api_key` for authentication. For details on how to obtain the API key, please refer to the [Obtaining an API Key](#obtaining-an-api-key) section below.

Follow the steps below to correctly set up Fireflies as a data source and run ingestion.

## Step 1: Add a connection to .bruin.yml file

To connect to Fireflies, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
  fireflies:
    - name: "my_fireflies"
      api_key: "your_api_key"
```

- `api_key`: Your Fireflies API key for authentication (required)

## Step 2: Create an asset file for data ingestion

To ingest data from Fireflies, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., fireflies_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.fireflies
type: ingestr
connection: postgres

parameters:
  source_connection: my_fireflies
  source_table: 'transcripts'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: "postgres" indicates that the ingested data will be stored in a PostgreSQL database.
- `source_connection`: The name of the Fireflies connection defined in .bruin.yml.
- `source_table`: The name of the data table in Fireflies you want to ingest. You can find the available source tables below.

## Available Tables

Fireflies source allows ingesting the following sources into separate tables:

| Table | Inc Key | Inc Strategy | Details |
| ----- | ------- | ------------ | ------- |
| active_meetings | - | replace | Currently active/ongoing meetings in your Fireflies account |
| analytics | end_time | merge | Meeting analytics including duration, speaker stats, and sentiment |
| channels | - | replace | Channels (workspaces) configured in your Fireflies account |
| users | - | replace | Users in your Fireflies team/organization |
| user_groups | - | replace | User groups configured in your organization |
| transcripts | date | merge | Meeting transcripts with full conversation details and metadata |
| bites | - | replace | Short audio/video clips (bites) extracted from meetings |
| contacts | - | replace | Contacts associated with your Fireflies account |

## Analytics Granularity

You can customize the chunk size for analytics by appending a granularity suffix to the table name:

| Table Name | Chunk Size | Use Case |
| ---------- | ---------- | -------- |
| `analytics` | 30 days (default) | Monthly reports |
| `analytics:HOUR` | 1 hour | Detailed hourly analysis |
| `analytics:DAY` | 1 day | Daily metrics |
| `analytics:MONTH` | Month boundaries | Calendar month alignment |

## Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/fireflies_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Fireflies table into your Postgres database.

## Obtaining an API Key

To set up Fireflies integration, you need to obtain an API key:

1. Log in to your [Fireflies account](https://app.fireflies.ai/)
2. Go to **Settings** → **Developer Settings** → **API & Integrations**
3. Generate a new API key

## Notes

- **Authentication**: The Fireflies API uses a GraphQL API with API key authentication.
- **Incremental Loading**: Supported for `analytics` and `transcripts` tables.
- **Analytics API Limit**: The analytics API has a 30-day limit per request. ingestr automatically chunks larger date ranges into 30-day intervals.
- **Analytics Data**: The `analytics` table returns pre-aggregated data for each chunk. When querying periods longer than the chunk size, each chunk is stored as a separate row.
