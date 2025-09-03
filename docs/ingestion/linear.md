# Linear
[Linear](https://linear.app/) is a project management platform for software teams.

Bruin supports Linear as a source for [Ingestr assets](/assets/ingestr). You can ingest data from Linear into your data platform.

To set up a Linear connection, add a configuration item in the `.bruin.yml` file and in your asset file. The configuration requires `api_key`.

### Step 1: Add a connection to the .bruin.yml file
```yaml
connections:
  linear:
    - name: "linear"
      api_key: "lin_api_123"
```
- `api_key`: Linear API key.

### Step 2: Create an asset file for data ingestion
Create an [asset configuration](/assets/ingestr#asset-structure) file (e.g., `linear_ingestion.yml`) inside the assets folder with the following content:
```yaml
name: public.linear
type: ingestr

parameters:
  source_connection: linear
  source_table: 'teams'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Always `ingestr` for Linear.
- `source_connection`: The Linear connection name defined in `.bruin.yml`.
- `source_table`: Name of the Linear table to ingest.
- `destination`: The destination connection name.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| issues | id | updatedAt | merge | Fetches all issues from your Linear workspace. |
| users | id | updatedAt | merge | Fetches users from your workspace. |
| workflow_states | id | updatedAt | merge | Fetches workflow states used in your Linear workspace. |
| cycles | id | updatedAt | merge | Fetches cycle information and planning data. |
| attachments | id | updatedAt | merge | Fetches file attachments associated with issues. |
| comments | id | updatedAt | merge | Fetches comments on issues and other entities. |
| documents | id | updatedAt | merge | Fetches documents created in Linear. |
| external_users | id | updatedAt | merge | Fetches information about external users. |
| initiative | id | updatedAt | merge | Fetches initiative data for high-level planning. |
| integrations | id | updatedAt | merge | Fetches integration configurations. |
| labels | id | updatedAt | merge | Fetches labels used for categorizing issues. |
| project_updates | id | updatedAt | merge | Fetches updates posted to projects |
| team_memberships | id | updatedAt | merge | Fetches team membership information. |
| initiative_to_project | id | updatedAt | merge | Fetches relationships between initiatives and projects. |
| project_milestone | id | updatedAt | merge | Retrieves Linear project milestones and checkpoints. |
| project_status | id | updatedAt | merge | Fetches project status information. |
| projects | id | updatedAt | merge | Fetches project-level data. |
| teams | id | updatedAt | merge | Fetches information about the teams configured in Linear. |
| organization | id | updatedAt | merge | Fetches organization-level information. |

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/linear_ingestion.yml
```
Running this command ingests data from Linear into your Postgres database.

<img alt="Linear" src="./media/linear_ingestion.png">
