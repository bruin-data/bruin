# BambooHR

[BambooHR](https://www.bamboohr.com/) is an HR platform for employee records, time off, time tracking, and related workforce data.

Bruin supports BambooHR as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest workforce data into your data platform.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

BambooHR accepts either an API key or an OAuth access token — provide exactly one.

```yaml
connections:
    bamboohr:
        - name: "bamboohr"
          company_domain: "acme"
          api_key: "your_api_key"
          timezone: "America/Denver"
```

Or with an OAuth access token:

```yaml
connections:
    bamboohr:
        - name: "bamboohr"
          company_domain: "acme"
          access_token: "your_oauth_access_token"
```

- `company_domain` (required): The part before `.bamboohr.com` in the company URL. For `https://acme.bamboohr.com`, use `acme`.
- `api_key` (optional): A BambooHR API key. BambooHR applies the permissions of the user who created the key. Use either `api_key` or `access_token`, **not both**.
- `access_token` (optional): An OAuth bearer token. Required for the `locations` table (needs the `field` scope). Use either `api_key` or `access_token`, **not both**.
- `timezone` (optional): The company's IANA timezone, such as `America/Denver`. Required for `timesheet_entries`, whose date boundaries BambooHR interprets in the company timezone.

API-key authentication is intended for a customer's own, internally operated integration. Use OAuth for hosted third-party services, as required by BambooHR's Developer Terms.

### Step 2: Create an asset file for data ingestion

Create a YAML file (e.g. bamboohr_ingestion.yml) inside the assets folder:

```yaml
name: public.employees
type: ingestr

parameters:
  source_connection: bamboohr
  source_table: 'employees'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Set to `ingestr`.
- `source_connection`: The name of the BambooHR connection defined in `.bruin.yml`.
- `source_table`: One of the tables below. The `employees` table accepts a comma-separated `fields` parameter to request extra standard or custom field aliases, e.g. `employees?fields=workEmail,hireDate,departmentName`.
- `destination`: The destination platform/type, for example `postgres`.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/bamboohr_ingestion.yml
```

## Available Source Tables

| Table | Primary Key | Incremental Key | Incremental Strategy | Details |
| ----- | ----------- | --------------- | -------------------- | ------- |
| `employees` | `employeeId` | - | replace | Complete employee roster with optional extra fields |
| `employee_directory` | `id` | - | replace | Published company directory |
| `employee_fields` | `id` | - | replace | Standard and custom employee-field metadata |
| `users` | `id` | - | replace | Enabled and disabled BambooHR user accounts |
| `locations` | `id` | - | replace | Active and archived job locations; requires an OAuth token with the `field` scope |
| `time_off_requests` | `id` | `start` | merge | Time-off requests overlapping the requested date interval |
| `time_off_types` | `id` | - | replace | Available time-off categories |
| `time_off_default_hours` | `name` | - | replace | Default work hours by weekday |
| `time_off_policies` | `id` | - | replace | Non-deleted time-off policies |
| `timesheet_entries` | `id` | `date` | merge | Clock and hour entries from BambooHR Time Tracking |

## Incremental behavior and limitations

- `time_off_requests` uses `--interval-start` and `--interval-end` as an inclusive overlap window. Without an interval it requests the full supported date range.
- `timesheet_entries` requires the company `timezone` in the connection and only exposes the latest 365 days — it defaults to that entire window and rejects dates outside it.
- `locations` requires `access_token` authentication with the BambooHR `field` scope.
