# PostHog

[PostHog](https://posthog.com/) is an open-source product analytics platform that helps teams understand user behavior, track events, and manage feature flags.

Bruin supports PostHog as a source for [Ingestr assets](/assets/ingestr). You can ingest data from PostHog into your data platform.

To set up a PostHog connection, add a configuration item in the `.bruin.yml` file and in your asset file. The configuration requires `personal_api_key` and `project_id`.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
  posthog:
    - name: "posthog"
      personal_api_key: "phx_1234567890abcdef"
      project_id: "12345"
```

- `personal_api_key`: Personal API key used to authenticate with the PostHog API.
- `project_id`: The ID of the PostHog project to ingest data from.

### Step 2: Create an asset file for data ingestion

Create an [asset configuration](/assets/ingestr#asset-structure) file (e.g., `posthog_ingestion.yml`) inside the assets folder with the following content:

```yaml
name: public.posthog
type: ingestr

parameters:
  source_connection: posthog
  source_table: 'persons'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Always `ingestr` for PostHog.
- `source_connection`: The PostHog connection name defined in `.bruin.yml`.
- `source_table`: Name of the PostHog table to ingest.
- `destination`: The destination connection name.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| `persons` | id | last_seen_at | merge | Person profiles with their properties |
| `feature_flags` | id | updated_at | merge | Feature flags configured in the project |
| `events` | id | timestamp | append | Events tracked in the project |
| `cohorts` | id | last_calculation | merge | Cohorts defined in the project |
| `event_definitions` | id | last_updated_at | merge | Event definitions in the project |
| `property_definitions:event` | id | updated_at | merge | Event property definitions |
| `property_definitions:person` | id | updated_at | merge | Person property definitions |
| `property_definitions:session` | id | updated_at | merge | Session property definitions |
| `annotations` | id | updated_at | merge | Annotations created in the project |

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/posthog_ingestion.yml
```

Running this command ingests data from PostHog into your Postgres database.
