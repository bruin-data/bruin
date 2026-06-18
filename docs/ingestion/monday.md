# Monday.com

[Monday.com](https://monday.com/) is a Work OS that powers teams to run projects and workflows with confidence. It's a simple, yet powerful platform that enables people to manage work, meet deadlines, and build a culture of transparency.

Bruin supports Monday.com as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Monday.com into your data warehouse.

In order to set up Monday.com connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Monday.com as a data source and run ingestion:

## Configuration

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

* name: The name of the asset.
* type: Specifies the type of the asset. It will be always ingestr type for Monday.com.
* connection: This is the destination connection.
* source_connection: The name of the Monday.com connection defined in .bruin.yml.
* source_table: The name of the data table in Monday.com you want to ingest.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| `account` | - | - | replace | Account information including name, slug, tier, and plan details. Full reload on each run. |
| `account_roles` | - | - | replace | Account roles with their types and permissions. Full reload on each run. |
| `users` | - | - | replace | Users in your Monday.com account with their profiles and permissions. Full reload on each run. |
| `boards` | id | updated_at | merge | Boards with their metadata, state, and configuration. Incrementally loads only updated boards. |
| `items` | id | - | replace | Items (rows) across all boards, including each cell's raw values as a JSON array in `column_values`. Full reload on each run. |
| `workspaces` | - | - | replace | Workspaces containing boards and their settings. Full reload on each run. |
| `webhooks` | - | - | replace | Webhooks configured for boards with their events and configurations. Full reload on each run. |
| `updates` | id | updated_at | merge | Updates (comments) on items with their content and metadata, including the creator and item names. Incrementally loads only updated entries. |
| `teams` | - | - | replace | Teams in your account with their members. Full reload on each run. |
| `tags` | - | - | replace | Tags used across your account for organizing items. Full reload on each run. |
| `custom_activities` | - | - | replace | Custom activity types defined in your account. Full reload on each run. |
| `board_columns` | - | - | replace | Columns defined in all boards with their types and settings. Full reload on each run. |
| `board_views` | - | - | replace | Views configured for boards with their filters and settings. Full reload on each run. |

### Scoping to specific boards

`items`, `boards`, `board_columns`, `board_views`, and `updates` accept an optional `:<board_id>[,<board_id>...]` suffix in `source_table` to restrict the result to the listed boards. Without a suffix they behave as before (every board in the account).

```yaml
parameters:
  source_connection: my-monday
  source_table: 'items:5091839751'                       # items on a single board
  # source_table: 'board_columns:5091839751,5091841857'  # board_columns from two boards
  # source_table: 'updates:5091841883'                   # updates on items of a single board
  destination: postgres
```

### `items:<board_id>:linked`

`items` additionally supports a `:linked` suffix that treats the given board as a "master" board and also pulls items from any **sub-boards whose name matches one of the master's item titles**. Useful for a "master board → linked sub-boards" fan-out pattern where the master's items name the sub-boards. Requires at least one master board id.

```yaml
parameters:
  source_connection: my-monday
  source_table: 'items:5091839751:linked'
  destination: postgres
```

> [!NOTE]
> `:linked` discovers sub-boards by the convention that **an item on the master board has the same title as the sub-board's name**. So if your master board has an item called `Q1 Roadmap` and you also have a board called `Q1 Roadmap`, that board is treated as a linked sub-board and its items are included in the result.
>
> This is a naming convention, not a real Monday-side relationship — so keep your master item titles and sub-board names in sync. If they ever drift apart (renames, typos, casing), the link is lost without any error and the sub-board's items are silently omitted. After a rename on either side, sanity-check the row count.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/monday_integration.asset.yml
```

As a result of this command, Bruin will ingest data from the given Monday.com table into your Postgres database.

## Incremental Loading

The `boards` and `updates` tables support incremental loading based on the `updated_at` field. This means subsequent runs will only fetch records that have been modified since the last run, making the data ingestion more efficient.

> [!NOTE]
> Most tables use "replace" write disposition, meaning they will overwrite existing data on each run. Only the `boards` and `updates` tables support incremental loading with "merge" disposition.
> [!NOTE]
> Monday.com has rate limits for API requests. The source handles pagination automatically and respects the API's maximum page size of 100 items.
