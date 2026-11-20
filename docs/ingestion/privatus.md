# Privatus Analytics

[Privatus Analytics](https://privatusanalytics.com/) is cookie-free website analytics that filters out bot traffic.

Bruin supports Privatus Analytics as a source for [Ingestr assets](/assets/ingestr), and you can use it to put your website traffic next to your sales, sign-ups and ad costs in your data warehouse.

In order to set up a Privatus Analytics connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Privatus Analytics as a data source and run ingestion.

## Configuration

### Step 1: Get an API token

1. Sign in at [privatusanalytics.com](https://privatusanalytics.com/).
2. Open **Workspace settings**, then **API tokens**.
3. Make a new token and copy it. It starts with `pat_` and reads one workspace.

The free plan includes API access.

### Step 2: Add a connection to .bruin.yml file

To connect to Privatus Analytics, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      privatus:
        - name: "privatus"
          api_key: "pat_123"
          site_ids:
            - "pa_AB12CD34EF"
            - "pa_XY98ZW76VU"
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | Yes | - | The name to identify this Privatus Analytics connection. |
| `api_key` | Yes | - | An API token, starting with `pat_`. |
| `site_ids` | No | every site in the workspace | The site IDs to copy, each like `pa_AB12CD34EF`. |

### Step 3: Create an asset file for data ingestion

To ingest data from Privatus Analytics, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., privatus_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.privatus_daily_stats
type: ingestr
connection: postgres

parameters:
  source_connection: privatus
  source_table: 'daily_stats'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Privatus Analytics connection defined in .bruin.yml.
- `source_table`: The name of the table to read. See [Source tables](#source-tables).

Any other dimension can be read with `breakdown:<dimension>`:

```yaml
name: public.privatus_countries
type: ingestr
connection: postgres

parameters:
  source_connection: privatus
  source_table: 'breakdown:country'

  destination: postgres
```

Custom event properties use the `prop:` prefix:

```yaml
name: public.privatus_plans
type: ingestr
connection: postgres

parameters:
  source_connection: privatus
  source_table: 'breakdown:prop:plan'

  destination: postgres
```

### Step 4: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/privatus_ingestion.yml
```

As a result of this command, Bruin will ingest the daily stats from Privatus Analytics into your Postgres database.

## Source tables

| Source table | Primary key | Incremental key | Strategy | Details |
|--------------|-------------|-----------------|----------|---------|
| `workspaces` | `id` | - | replace | The workspace the token belongs to: name, time zone, currency and plan |
| `sites` | `id` | - | replace | Each site with its domain, time zone and privacy settings |
| `goals` | `id` | - | replace | Goals set up on each site, active and archived |
| `daily_stats` | `site_id`, `date` | `date` | delete+insert | One row per site per day: visitors, visits, pageviews, views per visit, bounce rate, visit duration and revenue |
| `pages` | `site_id`, `date`, `value` | `date` | delete+insert | Pageviews, visits, time on page and scroll depth per page path per day |
| `referrers` | `site_id`, `date`, `value` | `date` | delete+insert | Visitors per referring domain per day. An empty `value` means a direct visit |
| `events` | `site_id`, `date`, `value` | `date` | delete+insert | Completions, visits and revenue per custom event per day |
| `goal_conversions` | `site_id`, `date`, `goal_id` | `date` | delete+insert | Completions, conversions, conversion rate and revenue per goal per day |
| `breakdown:<dimension>` | `site_id`, `date`, `value` | `date` | delete+insert | Any other dimension per day, for example `breakdown:country`, `breakdown:channel`, `breakdown:utm_campaign` or `breakdown:prop:plan` |

A dimension is lowercase letters and underscores, or `prop:` followed by up to 64 letters, digits, `_`, `.` or `-`. See the [Privatus Analytics API docs](https://privatusanalytics.com/docs/api) for the full list of dimensions.

Every row from a site has `site_id`, so all tables join on it. Days follow the time zone set on each site. Durations are in milliseconds, rates are percents from 0 to 100, and revenue is in cents.

## Incremental loading

The daily tables read the run's start and end dates, and copy the last 30 days when no dates are given. Each run deletes the rows for the days it reads and writes them again, so running it daily keeps today's numbers right as the day goes on. Past days can change in Privatus Analytics, for example when you rename or merge events, edit a goal or reset data, and the next run that reads those days picks up the change.

`workspaces`, `sites` and `goals` are fully replaced on every run.

## Limitations

- A run replaces whole days, so it also removes rows from sites that are not in `site_ids`. Copy each `site_ids` selection into its own table.
- A run with dates older than your plan's history removes those days from the table, because the API no longer returns them.
- The per-page, per-referrer and other breakdown tables need one request per site per day. A long date range on many sites can take a while on the Free plan.
