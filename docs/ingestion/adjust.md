# Adjust

[Adjust](https://www.adjust.com/) is a mobile marketing analytics platform that provides solutions for measuring and optimizing campaigns, as well as protecting user data.

Bruin supports Adjust as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Adjust into your data warehouse.

In order to set up Adjust connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need the `api_key`. For details on how to obtain these credentials, please refer [here](https://dev.adjust.com/en/api/rs-api/authentication/).

Follow the steps below to correctly set up Adjust as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to Adjust, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      adjust:
        - name: "my_adjust"
          api_key: "abc123"
```

- `api_key`: The API key for the Adjust account.
- `lookback_days`: Optional. The number of days to go back from the given start date for data. Defaults to 30 days. To know more about it, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/adjust.html#lookback-days)

### Step 2: Create an asset file for data ingestion

To ingest data from Adjust, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., adjust_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.adjust
type: ingestr
connection: postgres

parameters:
  source_connection: my_adjust
  source_table: 'creatives'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Adjust connection defined in .bruin.yml.
- `source_table`: The name of the data table in Adjust that you want to ingest. For example, `creatives` is the table of Adjust that you want to ingest. You can also filter by app token by appending `:<app_token>` to the table name (e.g., `creatives:abc123`).

### App Token Filtering

You can filter data for a specific app by appending `:<app_token>` to the source table name. Multiple app tokens can be separated by commas.

```yaml
parameters:
  source_connection: my_adjust
  source_table: 'campaigns:abc123'
```

```yaml
# Multiple app tokens
parameters:
  source_connection: my_adjust
  source_table: 'campaigns:abc123,def456'
```

### Attribution Types

The `campaigns` and `creatives` tables default to `click,engaged_ad` attribution. You can override which attribution types are included with the `attribution_types` query parameter. Valid values are `click`, `impression`, and `engaged_ad`, comma-separated.

```yaml
parameters:
  source_connection: my_adjust
  source_table: 'creatives?attribution_types=click,impression,engaged_ad'
```

```yaml
# Combined with an app token
parameters:
  source_connection: my_adjust
  source_table: 'creatives?app_token=abc123&attribution_types=click,engaged_ad'
```

The existing `creatives:abc123` colon form (app token only) continues to work. To set `attribution_types`, use the query-parameter form and pass the app token as `app_token` too — the two forms cannot be combined, so `creatives:abc123?attribution_types=...` is **not** valid. For custom tables, pass `attribution_types` in the filters section instead.

> [!WARNING]
> Adjust is changing its API-side default on **July 13, 2026** to include **all** attribution types (including `impression`). ingestr currently pins `click,engaged_ad` for these tables to preserve existing behavior. To keep your metrics stable regardless of Adjust's default, set `attribution_types` explicitly for the behavior you want.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| `campaigns` | day | – | merge | Retrieves data for a campaign, showing the app's revenue and network costs over multiple days. `Columns:` campaign, day, app, app_token, store_type, channel, country, network_cost, all_revenue_total_d0, ad_revenue_total_d0, revenue_total_d0, all_revenue_total_d1, ad_revenue_total_d1, revenue_total_d1, all_revenue_total_d3, ad_revenue_total_d3, revenue_total_d3, all_revenue_total_d7, ad_revenue_total_d7, revenue_total_d7, all_revenue_total_d14, ad_revenue_total_d14, revenue_total_d14, all_revenue_total_d21 |
| `creatives` | day | – | merge | Retrieves data for creative assets, detailing the app's revenue and network costs across multiple days. `Columns:` campaign, day, app, app_token, store_type, channel, country, adgroup, creative, network_cost, all_revenue_total_d0, ad_revenue_total_d0, revenue_total_d0, all_revenue_total_d1, ad_revenue_total_d1, revenue_total_d1, all_revenue_total_d3, ad_revenue_total_d3, revenue_total_d3, all_revenue_total_d7, ad_revenue_total_d7, revenue_total_d7, all_revenue_total_d14, ad_revenue_total_d14, revenue_total_d14, all_revenue_total_d21 |
| `events` | id | – | replace | Retrieves data for `events` and event slugs. |
| `custom` | configurable | – | merge | Retrieves custom data based on the dimensions and metrics specified. Please refer to the `custom reports` section below for more information. |

### Custom reports: `custom:<dimensions>:<metrics>[:<filters>]`

The custom table allows you to retrieve data based on specific dimensions and metrics, and apply filters to the data.

The format for the custom table is:

```plaintext
custom:<dimensions>:<metrics>[:<filters>]
```

Parameters:
- `dimensions`: A comma-separated list of [dimensions](https://dev.adjust.com/en/api/rs-api/reports#dimensions) to retrieve.
- `metrics`: A comma-separated list of [metrics](https://dev.adjust.com/en/api/rs-api/reports#metrics) to retrieve.
- `filters`: A comma-separated list of [filters](https://dev.adjust.com/en/api/rs-api/reports#filters) to apply to the data. For example, `app_token__in=abc123` filters results to a specific app.

> [!WARNING]
> Custom tables require a time-based dimension for efficient operation, such as `hour`, `day`, `week`, `month`, or `year`.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/adjust_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Adjust table into your Postgres database.
