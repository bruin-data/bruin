# Amplitude

[Amplitude](https://amplitude.com/) is a product analytics platform used to track and analyze user behavior across web and mobile applications.

Bruin supports Amplitude as a source for [Ingestr assets](/assets/ingestr). You can ingest data from Amplitude into your data platform.

To set up an Amplitude connection, add a configuration item in the `.bruin.yml` file and in your asset file. You authenticate with your project's `api_key` and `secret_key`. Optionally you can set `region` (defaults to `us`).

Follow these steps to set up Amplitude and run ingestion.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
  amplitude:
    - name: "amplitude"
      api_key: "your-api-key"
      secret_key: "your-secret-key"
      region: "us"
```

- `api_key`: (Required) Amplitude project API key.
- `secret_key`: (Required) Amplitude project secret key.
- `region`: (Optional) Data residency region (`us` or `eu`). Defaults to `us`.

### Step 2: Create an asset file for data ingestion

Create an [asset configuration](/assets/ingestr#asset-structure) file (e.g., `amplitude_ingestion.yml`) inside the assets folder with the following content:

```yaml
name: public.amplitude
type: ingestr

parameters:
  source_connection: amplitude
  source_table: 'events'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Always `ingestr` for Amplitude.
- `source_connection`: The Amplitude connection name defined in `.bruin.yml`.
- `source_table`: Name of the Amplitude table to ingest.
- `destination`: The destination connection name.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| events | uuid | event_time | merge | Raw event data, ingested incrementally. |
| cohorts | id | | replace | Behavioral cohorts. |
| annotations | id | | replace | Chart annotations. |
| event_types | event_type | | replace | Event type definitions. |
| event_categories | id | | replace | Event categories. |
| event_properties | event_property | | replace | Event property definitions. |
| user_properties | user_property | | replace | User property definitions. |

The `events` table is loaded incrementally (merge on `uuid` using `event_time`). All other tables are fully refreshed on each run (replace).

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/amplitude_ingestion.yml
```

Running this command ingests data from Amplitude into your Postgres database.
