# Twenty CRM

[Twenty](https://twenty.com/) is an open-source CRM. It runs both as a hosted workspace (`api.twenty.com`) and self-hosted on your own domain; Bruin supports both.

Bruin supports Twenty CRM as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Twenty into your data platform.

To set up a Twenty connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need an API key, which you can create in the workspace under **Settings → API & Webhooks**. The key is shown only once and covers one workspace.

Follow the steps below to set up Twenty correctly as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
    twenty:
        - name: "twenty"
          host: "api.twenty.com"
          api_key: "your_api_key"
```

- `host` (required): The workspace host. `api.twenty.com` for Twenty Cloud, or your own domain for a self-hosted instance (e.g. `crm.example.com`).
- `api_key` (required): Created in the workspace under **Settings → API & Webhooks**.
- `scheme` (optional): The transport used to reach the workspace, `https` or `http`. Defaults to `https`.
- `base_path` (optional): Where the REST API is mounted. Defaults to `/rest`.
- `page_size` (optional): Rows per request. Defaults to and is capped at `200`.
- `rate_limit` (optional): Requests per second. Defaults to `1.33` (80% of Twenty's documented 100 requests/minute).
- `include_deleted` (optional): Whether a second pass re-reads soft-deleted records so deletions are reflected downstream. Defaults to `true`.

### Step 2: Create an asset file for data ingestion

To ingest data from Twenty, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., twenty_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.twenty
type: ingestr

parameters:
  source_connection: twenty
  source_table: 'people'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset's type. Set this to `ingestr` to use the ingestr data pipeline. For Twenty, it will always be `ingestr`.
- `source_connection`: The name of the Twenty connection defined in `.bruin.yml`.
- `source_table`: The name of the table in Twenty to ingest. See the available tables below.
- `destination`: The destination platform/type, for example `postgres`.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/twenty_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Twenty table into your Postgres database.

## Available Source Tables

| Table | Primary Key | Incremental Key | Incremental Strategy | Details |
| ----- | ----------- | --------------- | -------------------- | ------- |
| `companies` | `id` | `updatedAt` | merge | Companies in the workspace |
| `notes` | `id` | `updatedAt` | merge | Notes attached to workspace records |
| `opportunities` | `id` | `updatedAt` | merge | Sales opportunities |
| `people` | `id` | `updatedAt` | merge | People in the workspace |
| `tasks` | `id` | `updatedAt` | merge | Tasks attached to workspace records |
| `workspaceMembers` | `id` | `updatedAt` | merge | Members of the workspace |
| `custom:<object_name>` | `id` | `updatedAt` | merge | A custom object, using its plural API name |

### Custom objects

Twenty exposes custom objects through the same REST API as standard objects. To ingest one, prefix its plural API name with `custom:`, for example `custom:leads`. The connector reads the object's metadata at runtime, so custom fields are included automatically.

```yaml
name: public.twenty_leads
type: ingestr

parameters:
  source_connection: twenty
  source_table: 'custom:leads'

  destination: postgres
```

## Incremental loading and deletions

Every Twenty object carries `updatedAt`, which is used as the incremental key with the `merge` strategy, filtered server-side. Only the start of the interval is applied — `merge` is idempotent, so a wider window costs requests rather than correctness.

Twenty soft-deletes records: a deleted record keeps its row with `deletedAt` set and is excluded from every list response by default. With `include_deleted` left at `true`, Bruin makes a second pass that re-reads exactly those records so their `deletedAt` lands populated; filter on `deletedAt IS NULL` downstream to see the live set. Set `include_deleted: false` to skip that pass at the cost of never learning about a deletion.
