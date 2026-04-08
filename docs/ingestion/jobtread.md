# JobTread

[JobTread](https://www.jobtread.com/) is a construction management platform that helps contractors manage jobs, estimates, invoices, budgets, tasks, and more.

Bruin supports JobTread as a source for [Ingestr assets](/assets/ingestr). You can ingest data from JobTread into your data platform.

To set up a JobTread connection, add a configuration item in the `.bruin.yml` file and in your asset file. The configuration requires `grant_key` and `organization_id`.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
  jobtread:
    - name: "jobtread"
      grant_key: "your_grant_key"
      organization_id: "your_organization_id"
```

- `grant_key`: A grant key used to authenticate with the JobTread API. Create one at [Settings > Integrations > API > Grants](https://app.jobtread.com/settings/integrations/api/grants).
- `organization_id`: The ID of the organization to ingest data from.

> [!WARNING]
> Grant keys expire after 3 months of inactivity.

### Step 2: Create an asset file for data ingestion

Create an [asset configuration](/assets/ingestr#asset-structure) file (e.g., `jobtread_ingestion.yml`) inside the assets folder with the following content:

```yaml
name: public.jobtread
type: ingestr

parameters:
  source_connection: jobtread
  source_table: 'jobs'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Always `ingestr` for JobTread.
- `source_connection`: The JobTread connection name defined in `.bruin.yml`.
- `source_table`: Name of the JobTread table to ingest.
- `destination`: The destination connection name.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| `accounts` | id | - | replace | Customer and vendor accounts |
| `jobs` | id | - | replace | Construction jobs/projects |
| `contacts` | id | - | replace | Contacts associated with accounts |
| `documents` | id | - | replace | All document types including estimates, invoices, bills, and orders |
| `tasks` | id | - | replace | Tasks and to-dos linked to jobs |
| `cost_codes` | id | - | replace | Budget cost code categories |
| `cost_types` | id | - | replace | Cost type definitions (labor, materials, etc.) |
| `cost_items` | id | - | replace | Budget line items on jobs |
| `locations` | id | - | replace | Job site locations with addresses |
| `custom_fields` | id | - | replace | Custom field definitions |
| `daily_logs` | id | - | replace | Daily job site logs with weather data |
| `time_entries` | id | - | replace | Time tracking records for labor |
| `files` | id | - | replace | File attachments |
| `comments` | id | - | replace | Comments on jobs, tasks, documents, etc. |
| `document_payments` | id | - | replace | Payments applied to documents |
| `cost_groups` | id | - | replace | Budget categories and templates |
| `events` | id | createdAt | merge | Audit log of all actions in the system |

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/jobtread_ingestion.yml
```

Running this command ingests data from JobTread into your destination database.

> [!WARNING]
> JobTread does not expose an `updatedAt` field on any entity, so most tables use a full replace strategy. Only the `events` table supports incremental loading via `createdAt` since events are immutable.
