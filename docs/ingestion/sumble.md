# Sumble

[Sumble](https://sumble.com/) provides organization, people, and intent-signal data for go-to-market teams.

Bruin supports Sumble as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Sumble into your data platform through Sumble's v9 API.

To set up a Sumble connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need an API key, which you can create from the API keys page in your Sumble account.

Follow the steps below to set up Sumble correctly as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
    sumble:
        - name: "sumble"
          api_key: "your_api_key"
```

- `api_key` (required): The Sumble API key, sent to Sumble as a bearer token.

### Step 2: Create an asset file for data ingestion

To ingest data from Sumble, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., sumble_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.sumble
type: ingestr

parameters:
  source_connection: sumble
  source_table: 'signals'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset's type. Set this to `ingestr` to use the ingestr data pipeline. For Sumble, it will always be `ingestr`.
- `source_connection`: The name of the Sumble connection defined in `.bruin.yml`.
- `source_table`: The name of the table in Sumble to ingest. See the available tables below.
- `destination`: The destination platform/type, for example `postgres`.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/sumble_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Sumble table into your Postgres database.

## Available Source Tables

| Table | Primary Key | Incremental Key | Incremental Strategy | Details |
| ----- | ----------- | --------------- | -------------------- | ------- |
| `organization_lists` | `id` | – | replace | Saved organization-list metadata, including deleted lists |
| `organization_list_organizations` | `_ingestr_id` | – | replace | Organizations in every saved organization list, including deleted lists |
| `contact_lists` | `id` | – | replace | Saved contact-list metadata |
| `contact_list_people` | `_ingestr_id` | – | replace | People in every saved contact list, including available contact information |
| `signals` | `_ingestr_id` | `date` | merge | Intent signals visible to the authenticated account |
| `priority_signals` | `id` | `date` | merge | Curated priority signals and relevance feedback |
| `signal_configs` | `id` | – | replace | Signal configuration definitions visible to the authenticated account |

Nested Sumble objects and arrays are preserved as JSON columns.

## Table parameters

Tables accept optional filters appended to the table name as query parameters. Multiple values are comma-separated, e.g. `signals?organization_ids=12,34`.

| Table | Parameters |
| ----- | ---------- |
| `organization_list_organizations` | `list_ids` — restrict to specific lists instead of every list |
| `contact_list_people` | `list_ids` — restrict to specific lists instead of every list |
| `signals` | `organization_ids`, `person_ids`, `signal_ids`, `technology_slugs`, `job_functions`, `priorities`, `account_list_ids`, `signal_config_ids` |
| `priority_signals` | `organization_ids`, `person_ids`, `signal_ids`, `job_post_ids`, `is_relevant` |
| `signal_configs` | `signal_config_ids`, `types`, `priorities` |

`priorities` accepts `high`, `medium`, or `low`. `is_relevant` accepts `true` or `false`. For `signal_configs`, `signal_config_ids` cannot be combined with `types` or `priorities`.

```yaml
name: public.sumble_signals
type: ingestr

parameters:
  source_connection: sumble
  source_table: 'signals?organization_ids=12,34&priorities=high'

  destination: postgres
```

## Incremental loading

`signals` and `priority_signals` support interval-based loading on `date`. The Sumble API does not provide a server-side date range for these resources, so ingestr pages through the matching results and applies the interval locally. The interval start is inclusive and the interval end is exclusive. The remaining tables are replaced in full so removals from lists and changes to configuration are reflected in the destination.

Sumble's `signals` and `priority_signals` are paged with offset pagination that stops at an offset of 10,000; extracts larger than that are truncated with a warning — narrow them with the table parameters above. Sumble charges API credits according to the resource and number of records returned, so check your plan and credit balance before running large extracts.
