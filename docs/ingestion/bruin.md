# Bruin

[Bruin](https://getbruin.com/) is a data platform that allows you to build, test, and deploy data pipelines. Bruin Cloud provides an API to access your pipeline metadata and execution information.

Bruin supports Bruin Cloud as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Bruin Cloud into your data warehouse.

In order to set up a Bruin Cloud connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Bruin Cloud as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Bruin Cloud, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      bruin:
        - name: "my-bruin"
          api_token: "your_api_token_here"
```
- `api_token`: The API token used for authentication with the Bruin API.

### How to get your API token

1. Go to [cloud.getbruin.com](https://cloud.getbruin.com)
2. Navigate to **Teams** section
3. Click on **Create API Token**
4. Make sure **Pipeline List** is selected as the permission
5. Copy the generated API token

### Step 2: Create an asset file for data ingestion

To ingest data from Bruin Cloud, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., bruin_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.bruin_pipelines
type: ingestr
connection: postgres

parameters:
  source_connection: my-bruin
  source_table: 'pipelines'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Bruin Cloud connection defined in .bruin.yml.
- `source_table`: The name of the data table in Bruin Cloud that you want to ingest. For example, `pipelines` is the table of Bruin Cloud that you want to ingest.

## Available Source Tables

| Table     | PK | Inc Key | Inc Strategy | Details                                                          |
|-----------|----|---------|--------------|-----------------------------------------------------------------|
| pipelines | -  | -       | replace      | Contains information about your data pipelines including metadata and configuration. |
| assets    | -  | -       | replace      | Contains information about your data assets including metadata and configuration. |

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/bruin_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Bruin Cloud table into your Postgres database.
