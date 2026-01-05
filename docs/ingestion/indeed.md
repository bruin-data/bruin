# Indeed
[Indeed](https://www.indeed.com/) is a job search and advertising platform that enables employers to post jobs and manage sponsored job campaigns.

Bruin supports Indeed as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Indeed into your data warehouse.

In order to set up Indeed connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You will need `client_id`, `client_secret`, and `employer_id`. For details on how to obtain these credentials, please refer to the [Indeed API documentation](https://docs.indeed.com/api/sponsored-jobs-api/sponsored-jobs-api-reference).

Follow the steps below to correctly set up Indeed as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Indeed, you need to add a configuration item to the connections section of the .bruin.yml file. This configuration must comply with the following schema:

```yaml
    connections:
      indeed:
        - name: "my-indeed"
          client_id: "your_client_id"
          client_secret: "your_client_secret"
          employer_id: "your_employer_id"
```
- `name`: The name of the connection
- `client_id`: OAuth client ID for Indeed API authentication
- `client_secret`: OAuth client secret for Indeed API authentication
- `employer_id`: The employer ID associated with your Indeed account

### Step 2: Create an asset file for data ingestion

To ingest data from Indeed, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., indeed_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.indeed
type: ingestr
connection: postgres

parameters:
  source_connection: my-indeed
  source_table: 'campaigns'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Indeed connection defined in .bruin.yml.
- `source_table`: The name of the data table in Indeed that you want to ingest.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| campaigns | - | - | replace | Retrieves all campaigns for the employer |
| campaign_details | - | - | replace | Retrieves detailed information for each campaign |
| campaign_budget | - | - | replace | Retrieves budget information for each campaign |
| campaign_jobs | - | - | replace | Retrieves all jobs associated with each campaign |
| campaign_properties | - | - | replace | Retrieves properties for each campaign |
| campaign_stats | - | Date | merge | Retrieves daily statistics for each campaign |
| account | - | - | replace | Retrieves account information including job sources |
| traffic_stats | - | date | merge | Retrieves daily traffic statistics |

## Incremental Loading

The `campaign_stats` and `traffic_stats` tables support incremental loading using date-based merge strategy. Use `--interval-start` and `--interval-end` to specify the date range:

```yaml
name: public.indeed_campaign_stats
type: ingestr
connection: postgres

parameters:
  source_connection: my-indeed
  source_table: 'campaign_stats'
  destination: postgres
```

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/indeed_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Indeed table into your Postgres database.
