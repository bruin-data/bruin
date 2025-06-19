# Mixpanel
[Mixpanel](https://mixpanel.com/) is an analytics service used for tracking user interactions with web and mobile applications.

Bruin supports Mixpanel as a source for [Ingestr assets](/assets/ingestr). You can ingest data from Mixpanel into your data platform.

To set up a Mixpanel connection, add a configuration item in the `.bruin.yml` file and in your asset file. The configuration requires `username`, `password`, `project_id` and optionally `server` (defaults to `eu`).

Follow these steps to set up Mixpanel and run ingestion.

### Step 1: Add a connection to the .bruin.yml file
```yaml
connections:
  mixpanel:
    - name: "mixpanel"
      username: "service-account-username"
      password: "service-account-secret"
      project_id: "12345"
      server: "eu"
```
- `username`: Mixpanel service account username.
- `password`: Secret associated with the service account.
- `project_id`: The numeric project ID.
- `server`: (Optional) Server region (`us`, `eu`, or `in`). Defaults to `eu`.

### Step 2: Create an asset file for data ingestion
Create an [asset configuration](/assets/ingestr#asset-structure) file (e.g., `mixpanel_ingestion.yml`) inside the assets folder with the following content:
```yaml
name: public.mixpanel
type: ingestr

parameters:
  source_connection: mixpanel
  source_table: 'events'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Always `ingestr` for Mixpanel.
- `source_connection`: The Mixpanel connection name defined in `.bruin.yml`.
- `source_table`: Name of the Mixpanel table to ingest.
- `destination`: The destination connection name.

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/mixpanel_ingestion.yml
```
Running this command ingests data from Mixpanel into your Postgres database.
