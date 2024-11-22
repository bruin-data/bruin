# Notion
[Notion ](https://www.notion.so/) is an all-in-one workspace for note-taking, project management, and database management.

Bruin supports Notion as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Notion into your data warehouse.

In order to set up Notion connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need `api_key`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/notion#setup-guide).

Follow the steps below to correctly set up Notion as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Notion, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      notion:
        - name: "my-notion"
          api_key: "YOUR_NOTION_API_KEY"
```
### Step 2: Create an asset file for data ingestion

To ingest data from Notion, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., notion_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.notion
type: ingestr
connection: postgres

parameters:
  source_connection: my_notion
  source_table: 'd8ee2d159ac34cfc85827ba5a0a8ae71'

  destination: postgress
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Notion connection defined in .bruin.yml.
- `source_table`: The name of the data table in Notion that you want to ingest. Use the `database ID` as the source_table. For example, if the Notion URL is: https://www.notion.so/d8ee2d159ac34cfc85827ba5a0a8ae71?v=c714dec3742440cc91a8c38914f83b6b, the database ID is the string immediately following notion.so/ and preceding any question marks. In this example, the `database ID` is `d8ee2d159ac34cfc85827ba5a0a8ae71`.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/notion_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Notion table into your Postgres database.