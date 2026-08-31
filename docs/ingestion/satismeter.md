# SatisMeter

[SatisMeter](https://www.satismeter.com/) collects in-app NPS, CSAT and CES survey responses.

Bruin supports SatisMeter as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from SatisMeter into your data platform through the SatisMeter REST API v3.

To set up a SatisMeter connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need a project-scoped API key, which you can create in the SatisMeter UI under **Settings → Integrations → API**, and the project id it belongs to.

Follow the steps below to set up SatisMeter correctly as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
    satismeter:
        - name: "satismeter"
          api_key: "your_api_key"
          project_id: "your_project_id"
```

- `api_key` (required): The project-scoped SatisMeter API key, sent as a bearer token. The key is scoped to a single project.
- `project_id` (required): The project the key belongs to. Every endpoint is nested under `/projects/{projectId}`, so it cannot be inferred from the key.

### Step 2: Create an asset file for data ingestion

To ingest data from SatisMeter, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., satismeter_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.satismeter
type: ingestr

parameters:
  source_connection: satismeter
  source_table: 'responses'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset's type. Set this to `ingestr` to use the ingestr data pipeline. For SatisMeter, it will always be `ingestr`.
- `source_connection`: The name of the SatisMeter connection defined in `.bruin.yml`.
- `source_table`: The name of the table in SatisMeter to ingest. See the available tables below.
- `destination`: The destination platform/type, for example `postgres`.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/satismeter_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given SatisMeter table into your Postgres database.

## Available Source Tables

| Table | Primary Key | Incremental Key | Incremental Strategy | Details |
| ----- | ----------- | --------------- | -------------------- | ------- |
| `responses` | `id` | `created` | merge | Individual survey responses: the answer payload, the responding user, device, location, language, referrer and a `created` timestamp |
| `campaigns` | `id` | – | merge | The surveys defined in the project — id, name, type (`nps` / `csat` / …) and state |
| `project` | `id` | – | merge | Project metadata: name, default language, branding |

`responses` is loaded incrementally on its `created` timestamp. `campaigns` and `project` are small snapshots that are re-fetched in full and de-duplicated on `id`; neither tracks deletions, so a survey removed in SatisMeter remains in the table. Nested objects (`answers`, `user`, `device`, `location`) are passed through as JSON rather than flattened.

## A note on date ranges

SatisMeter's responses endpoint defaults to **the last 30 days** when no start date is supplied. Bruin always sends an explicit start date, so a run without `interval_start` fetches the full history rather than silently returning a month. Pass `interval_start` / `interval_end` to narrow it.

## Personal data

Response records embed the respondent's email, name, user id and the full custom trait bag exactly as SatisMeter returns them. Treat the destination table as containing personal data.
