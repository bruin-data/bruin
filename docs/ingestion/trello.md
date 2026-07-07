# Trello

[Trello](https://trello.com/) is a visual work-management tool that organizes projects into boards, lists, and cards.

Bruin supports Trello as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Trello into your data warehouse.

In order to set up Trello connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need an `api_key` and a `token` for authentication. For details on how to obtain them, please refer to the [Obtaining credentials](#obtaining-credentials) section below.

Follow the steps below to correctly set up Trello as a data source and run ingestion.

## Step 1: Add a connection to .bruin.yml file

To connect to Trello, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
  trello:
    - name: "my_trello"
      api_key: "your_api_key"
      token: "your_token"
```

- `api_key`: Your Trello Power-Up API key (required)
- `token`: A Trello token authorizing access to your account's data (required)

## Step 2: Create an asset file for data ingestion

To ingest data from Trello, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., trello_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.trello
type: ingestr
connection: postgres

parameters:
  source_connection: my_trello
  source_table: 'cards'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: "postgres" indicates that the ingested data will be stored in a PostgreSQL database.
- `source_connection`: The name of the Trello connection defined in .bruin.yml.
- `source_table`: The name of the data table in Trello you want to ingest. You can find the available source tables below.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| `boards` | id | - | replace | All boards the account can access. |
| `organizations` | id | - | replace | Workspaces (organizations) the account belongs to. |
| `lists` | id | - | replace | Lists across all accessible boards. |
| `members` | id | - | replace | Members across all accessible boards, de-duplicated. |
| `labels` | id | - | replace | Labels defined on each board. |
| `checklists` | id | - | replace | Checklists across all accessible boards. |
| `cards` | id | dateLastActivity | merge | Cards across all accessible boards. |
| `actions` | id | date | merge | Activity log (actions) across all accessible boards. |

## Scoping to specific boards

By default the board-scoped tables (`lists`, `members`, `labels`, `checklists`, `cards`, `actions`) fetch data from every board the account can access. To scope a run to specific boards, append a comma-separated list of board IDs after the table name with a colon, e.g. `source_table: 'cards:5f2a1c,60b1d2'`.

## Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/trello_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Trello table into your Postgres database.

## Obtaining credentials

Trello's REST API authenticates with an API key and a token:

1. Log in to Trello and open the [Power-Ups admin](https://trello.com/power-ups/admin).
2. Create a Power-Up (or open an existing one) and generate an **API key** on its "API key" page.
3. From the same page, use the **Token** link to authorize your account and generate a token.

## Notes

- **Authentication**: The Trello REST API uses an API key + token passed as query parameters.
- **Incremental Loading**: Supported for `cards` (by `dateLastActivity`) and `actions` (by `date`).
