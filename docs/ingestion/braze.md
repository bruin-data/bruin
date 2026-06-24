# Braze

[Braze](https://www.braze.com/) is a customer engagement platform for cross-channel messaging and customer analytics.

Bruin supports Braze as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Braze into your data warehouse.

To set up a Braze connection, you need a REST API key and your instance's REST endpoint. For more information, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/braze.html)

Follow the steps below to correctly set up Braze as a data source and run ingestion:

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to Braze, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      braze:
        - name: "my_braze"
          api_key: "YOUR_BRAZE_REST_API_KEY"
          endpoint: "rest.iad-01.braze.com"
```

- `api_key`: A Braze REST API key with access to the relevant export endpoints. Required.
- `endpoint`: Your instance's REST endpoint host (e.g. `rest.iad-01.braze.com`). Braze is multi-instance, so the host depends on which cluster your account is on. Required.

### Step 2: Create an asset file for data ingestion

To ingest data from Braze, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., braze_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.braze
type: ingestr
connection: postgres

parameters:
  source_connection: my_braze
  source_table: 'campaigns'
  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will be always ingestr type for Braze.
- `connection`: This is the destination connection.
- `source_connection`: The name of the Braze connection defined in .bruin.yml.
- `source_table`: The name of the data table in Braze you want to ingest. For example, `campaigns` would ingest campaign records.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| `campaigns` | id | last_edited | merge | Marketing campaigns (including archived) with their name, tags, and API flags. |
| `canvases` | id | last_edited | merge | Canvas (journey) definitions (including archived) with their name and tags. |
| `segments` | id | - | replace | Audience segments with their name and analytics-tracking flag. |
| `events` | event_name | - | replace | Names of the custom events tracked in the workspace. |
| `products` | product_id | - | replace | Product IDs seen in purchase events. |
| `kpi_dau` | time | time | merge | Daily active users by date. |
| `kpi_mau` | time | time | merge | Monthly active users (rolling 30-day) by date. |
| `kpi_new_users` | time | time | merge | New users by date. |
| `kpi_uninstalls` | time | time | merge | App uninstalls by date. |
| `user_data` | braze_id, segment_id | - | replace | Users of a segment with their email/push subscription state and profile fields (a point-in-time snapshot). |

The `kpi_*` tables aggregate across all apps by default. Append a comma-separated list of app identifiers to break a KPI down by app, e.g. `source_table: 'kpi_dau:app-one-id,app-two-id'`; each row then carries an `app_id` column.

The `user_data` table requires one or more segment ids, passed as a comma-separated suffix, e.g. `source_table: 'user_data:<segment_id>'` or `'user_data:<segment_id_1>,<segment_id_2>'`. Each row is tagged with the `segment_id` it came from.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run ingestr.braze.asset.yml
```

As a result of this command, Bruin will ingest data from the given Braze table into your Postgres database.
