# Monday.com
[Monday.com](https://monday.com/) is a Work OS that powers teams to run projects and workflows with confidence. It's a simple, yet powerful platform that enables people to manage work, meet deadlines, and build a culture of transparency.


Bruin supports Monday.com as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Monday.com into your data warehouse.

In order to set up Monday.com connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Monday.com as a data source and run ingestion:

### Step 1: Add a connection to .bruin.yml file

To connect to Monday.com, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      monday:
        - name: "my-monday"
          api_token: "YOUR_API_TOKEN"

```

* `api_token`: is your Monday.com API token for authentication.

For details on how to obtain these credentials, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/monday.html#setting-up-a-mondaycom-integration).

### Step 2: Create an asset file for data ingestion

To ingest data from Monday.com, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., monday_integration.asset.yml) inside the assets folder and add the following content:

```yaml
name: public.monday
type: ingestr
connection: postgres

parameters:
  source_connection: my-monday
  source_table: 'boards'
  destination: postgres
```

- name: The name of the asset.
- type: Specifies the type of the asset. It will be always ingestr type for Monday.com.
- connection: This is the destination connection.
- source_connection: The name of the Monday.com connection defined in .bruin.yml.
- source_table: The name of the data table in Monday.com you want to ingest.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| `account` | - | - | replace | Account information including name, slug, tier, and plan details. Full reload on each run. |
| `account_roles` | - | - | replace | Account roles with their types and permissions. Full reload on each run. |
| `users` | - | - | replace | Users in your Monday.com account with their profiles and permissions. Full reload on each run. |
| `boards` | id | updated_at | merge | Boards with their metadata, state, and configuration. Incrementally loads only updated boards. |
| `workspaces` | - | - | replace | Workspaces containing boards and their settings. Full reload on each run. |
| `webhooks` | - | - | replace | Webhooks configured for boards with their events and configurations. Full reload on each run. |
| `updates` | id | updated_at | merge | Updates (comments) on items with their content and metadata. Incrementally loads only updated entries. |
| `teams` | - | - | replace | Teams in your account with their members. Full reload on each run. |
| `tags` | - | - | replace | Tags used across your account for organizing items. Full reload on each run. |
| `custom_activities` | - | - | replace | Custom activity types defined in your account. Full reload on each run. |
| `board_columns` | - | - | replace | Columns defined in all boards with their types and settings. Full reload on each run. |
| `board_views` | - | - | replace | Views configured for boards with their filters and settings. Full reload on each run. |

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/monday_integration.asset.yml
```
As a result of this command, Bruin will ingest data from the given Monday.com table into your Postgres database.

## Incremental Loading

The `boards` and `updates` tables support incremental loading based on the `updated_at` field. This means subsequent runs will only fetch records that have been modified since the last run, making the data ingestion more efficient.

> [!NOTE]
> Most tables use "replace" write disposition, meaning they will overwrite existing data on each run. Only the `boards` and `updates` tables support incremental loading with "merge" disposition.

> [!NOTE]
> Monday.com has rate limits for API requests. The source handles pagination automatically and respects the API's maximum page size of 100 items.
