# Okta

[Okta](https://www.okta.com/) is an identity and access management platform for managing users, groups, applications, and authentication policies.

Bruin supports Okta as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Okta into your data warehouse.

> [!WARNING]
> Okta support requires an ingestr release that includes the Okta source ([bruin-data/ingestr#1088](https://github.com/bruin-data/ingestr/pull/1088)). If that PR hasn't been released yet, this connection type won't be usable until Bruin's pinned ingestr version is bumped past it.

In order to set up an Okta connection, you need to add a configuration item to `connections` in the `.bruin.yml` file and in the `asset` file.

Follow the steps below to correctly set up Okta as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to Okta, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
  okta:
    - name: "my_okta"
      domain: "dev-123456.okta.com"
      api_key: "your_api_token"
```

- `domain`: Your Okta org domain, for example `dev-123456.okta.com` or `mycompany.okta.com`.
- `api_key`: An Okta API token used to authenticate requests. Create one in the Okta Admin Console under **Security → API → Tokens → Create Token**. The token inherits the permissions of the admin who creates it, so use an account that can read the resources you want to ingest.

### Step 2: Create an asset file for data ingestion

To ingest data from Okta, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., okta_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.okta_users
type: ingestr
connection: postgres

parameters:
  source_connection: my_okta
  source_table: 'users'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to `ingestr` to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Okta connection defined in `.bruin.yml`.
- `source_table`: The name of the data table in Okta that you want to ingest.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| `users` | id | lastUpdated | merge | All users in the org |
| `groups` | id | lastUpdated | merge | All groups in the org |
| `group_members` | group_id, id | – | replace | Members of each group (one row per user per group); full snapshot each run |
| `applications` | id | lastUpdated | merge | All applications configured in the org |
| `application_users` | app_id, id | – | replace | Users assigned to each application; full snapshot each run |
| `application_groups` | app_id, id | – | replace | Groups assigned to each application; full snapshot each run |
| `system_log_events` | uuid | published | merge | System log events (Okta retains roughly the last 90 days) |
| `devices` | id | lastUpdated | merge | Devices enrolled in the org |
| `policies` | id | lastUpdated | merge | Policies across all policy types |
| `policy_rules` | id | lastUpdated | merge | Rules belonging to each policy |
| `roles` | id | – | replace | Custom admin role definitions |

Nested objects (such as a user's `profile` and `credentials`) are preserved as JSON columns.

Tables with an incremental key support incremental loading. When no interval is provided, ingestr performs a full load. The System Log can only be backfilled as far as Okta's retention window (about 90 days).

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/okta_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Okta table into your Postgres database.
