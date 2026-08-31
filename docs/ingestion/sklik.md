# Sklik

[Sklik](https://www.sklik.cz/) is the paid-search advertising platform of Seznam.cz, the dominant Czech search engine.

Bruin supports Sklik as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Sklik into your data platform.

To set up a Sklik connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need a permanent API `token`, which you can generate in the Sklik UI.

Follow the steps below to set up Sklik correctly as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
    sklik:
        - name: "sklik"
          token: "your_api_token"
```

- `token` (required): The permanent API token generated in the Sklik UI.
- `user_id` (optional): A numeric Sklik account id to scope the connection to a specific account.

The token belongs to a single Sklik account. To load several accounts into one destination, run ingestion once per token; every row carries a `_user_id` column identifying the account it came from.

### Step 2: Create an asset file for data ingestion

To ingest data from Sklik, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., sklik_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.sklik
type: ingestr

parameters:
  source_connection: sklik
  source_table: 'campaigns'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset's type. Set this to `ingestr` to use the ingestr data pipeline. For Sklik, it will always be `ingestr`.
- `source_connection`: The name of the Sklik connection defined in `.bruin.yml`.
- `source_table`: The name of the table in Sklik to ingest. See the available tables below.
- `destination`: The destination platform/type, for example `postgres`.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/sklik_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Sklik table into your Postgres database.

## Available Source Tables

### Entity tables

Snapshots of current state. They have no date dimension — each run returns the account as it stands now.

| Table | Primary Key | Incremental Strategy | Details |
| ----- | ----------- | -------------------- | ------- |
| `campaigns` | `id` | merge | Campaigns with budget, status and settings |
| `groups` | `id` | merge | Ad groups |
| `ads` | `id` | merge | Ads |
| `keywords` | `id` | merge | Keywords, including match type and bids |
| `conversions` | `id` | merge | Conversion definitions |

### Report tables

One row per entity per day. Report tables support `interval_start` / `interval_end` to bound the date window.

| Table | Primary Key | Incremental Key | Incremental Strategy | Details |
| ----- | ----------- | --------------- | -------------------- | ------- |
| `campaign_stats_daily` | `id, date` | `date` | merge | Daily campaign performance |
| `search_queries` | `query, keyword_id, date` | `date` | merge | Search terms that triggered ads |

Every row also gets a `_user_id` column holding the Sklik account id. It is deliberately not part of any primary key; load each account into its own destination table to avoid merge collisions across accounts.
