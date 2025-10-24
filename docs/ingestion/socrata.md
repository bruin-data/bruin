# Socrata
[Socrata](https://dev.socrata.com/) is an open data platform used by governments and organizations to publish and share public datasets. The platform powers thousands of open data portals worldwide, including data.gov and many city, state, and federal government sites.

Bruin supports Socrata as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from any Socrata-powered open data portal into your data warehouse.

In order to set up Socrata connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need the `domain` and `app_token`. Optionally, you can provide `username` and `password` for accessing private datasets.

Follow the steps below to correctly set up Socrata as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Socrata, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
  socrata:
    - name: "my-socrata"
      domain: "data.seattle.gov"
      app_token: "your_app_token"
      username: "your_username"  # optional, for private datasets
      password: "your_password"  # optional, for private datasets
```

- `domain`: The Socrata domain (e.g., `data.seattle.gov`, `data.cityofnewyork.us`)
- `app_token`: Socrata app token for API access (required)
- `username`: Username for authentication (optional, required for private datasets)
- `password`: Password for authentication (optional, required for private datasets)

### Step 2: Create an asset file for data ingestion

To ingest data from Socrata, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., socrata_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.socrata_data
type: ingestr
connection: postgres

parameters:
  source_connection: my-socrata
  source_table: '2khk-5ukd'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the socrata connection defined in .bruin.yml.
- `source_table`: The Socrata dataset ID in 4x4 format (e.g., `2khk-5ukd`).

## Available Source Tables

Socrata source allows ingesting datasets by specifying their dataset ID as the source table:

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| `<dataset_id>` | `:id` | user-defined | replace/merge | Loads all records from the specified Socrata dataset. Uses `replace` by default, or `merge` when incremental key is specified. |

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/socrata_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Socrata dataset into your Postgres database.
