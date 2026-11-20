# Breezy HR

[Breezy HR](https://breezy.hr/) is an applicant tracking system for managing job positions, hiring pipelines, and candidates.

Bruin supports Breezy HR as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Breezy HR into your data warehouse.

In order to set up a Breezy HR connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Breezy HR as a data source and run ingestion.

## Configuration

### Step 1: Get an API key and company ID

Your Breezy plan must include API access.

1. Sign in to Breezy as a company admin. The token belongs to the user who creates it and sees the same data that user does.
2. Click your profile icon, open **My Settings**, select the **API Keys** tab, and click **Create API Key**. Copy the token; it starts with `breezy_pat_`.
3. Find the company ID: it is the `_id` returned by `GET https://api.breezy.hr/v3/companies` for your token, not the company name or URL slug.

### Step 2: Add a connection to .bruin.yml file

To connect to Breezy HR, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      breezyhr:
        - name: "breezyhr"
          api_key: "breezy_pat_123"
          company_id: "abc123"
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | Yes | - | The name to identify this Breezy HR connection. |
| `api_key` | Yes | - | A Breezy personal access token, starting with `breezy_pat_`. |
| `company_id` | Yes | - | The ID of the Breezy company to load. To load several companies, add one connection per company. |

Bruin URL-encodes every value, so tokens containing special characters can be used as-is.

### Step 3: Create an asset file for data ingestion

To ingest data from Breezy HR, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., breezyhr_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.breezyhr_candidates
type: ingestr
connection: postgres

parameters:
  source_connection: breezyhr
  source_table: 'candidates'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Breezy HR connection defined in .bruin.yml.
- `source_table`: The name of the table to read. See [Source tables](#source-tables).

Position activity is the only incremental table:

```yaml
name: public.breezyhr_position_stream
type: ingestr
connection: postgres

parameters:
  source_connection: breezyhr
  source_table: 'position_stream'

  destination: postgres
```

### Step 4: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/breezyhr_ingestion.yml
```

As a result of this command, Bruin will ingest the candidates from Breezy HR into your Postgres database.

## Source tables

| Source table | Primary key | Incremental key | Strategy | Details |
|--------------|-------------|-----------------|----------|---------|
| `departments` | `company_id`, `id` | - | replace | Departments configured for the company |
| `pipelines` | `company_id`, `_id` | - | replace | Hiring pipelines and their stages |
| `questionnaires` | `company_id`, `_id` | - | replace | Questionnaire definitions with their sections and questions |
| `custom_fields` | `company_id`, `type`, `_id` | - | replace | Candidate and position custom field definitions |
| `positions` | `_id` | - | replace | Positions in every state; `state` shows whether each is open or closed |
| `position_team_members` | `position_id`, `_id` | - | replace | Hiring team members on each position and their roles |
| `position_stream` | `_id` | `timestamp` | delete+insert | Activity on each position, such as state changes and added candidates |
| `position_custom_fields` | `position_id`, `_id` | - | replace | Custom field values on each position |
| `candidates` | `_id` | - | replace | Full candidate profiles, one row per candidate per position |
| `candidate_stream` | `candidate_id`, `_id` | - | replace | Candidate activity, such as stage moves, notes, and interviews |
| `candidate_scorecards` | `candidate_id`, `_id` | - | replace | Scorecard ratings on each candidate |
| `candidate_questionnaires` | `candidate_id`, `_id` | - | replace | Questionnaires sent to each candidate, with their answers |
| `candidate_custom_fields` | `candidate_id`, `_id` | - | replace | Custom field values on each candidate |

Every row has a `company_id` column. `position_team_members`, `position_stream`, `position_custom_fields` and `candidates` have a `position_id` column, and the other candidate tables have `position_id` and `candidate_id` columns. A person who applied to several positions appears once per position; `meta_id` on `candidates` identifies the person across positions.

## Incremental loading

`position_stream` is the only incremental table. When a run has a start and end date, it deletes the rows whose `timestamp` falls in that range and inserts the events Breezy returns for it, so events deleted in Breezy within the range, such as removed notes, are removed too. Besides position events, it also contains activity on the position's candidates. Events deleted before the range stay in the table, including those of deleted candidates, and a range with no events at all is skipped; use [`--full-refresh`](/commands/run) to remove them.

All other tables are fully replaced on every run and ignore the date range. Candidate tables are refreshed in full because changes such as a new scorecard or custom field value don't update the candidate's `updated_date`, and a full refresh also removes deleted candidates.

## Limitations

- Breezy allows 100 requests per minute per token; ingestr stays at 80% of that, so candidate tables on large accounts can take a while.
