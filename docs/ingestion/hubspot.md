# HubSpot
[HubSpot](https://www.hubspot.com/) is a customer relationship management software that helps businesses attract visitors, connect with customers, and close deals.

Bruin supports HubSpot as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from HubSpot into your data warehouse.

In order to set up HubSpot connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You will need the `api_key`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/hubspot#setup-guide).

Follow the steps below to correctly set up HubSpot as a data source and run ingestion.
### Step 1: Add a connection to .bruin.yml file

To connect to HubSpot, you need to add a configuration item to the connections section of the .bruin.yml file. This configuration must comply with the following schema:

```yaml
    connections:
      hubspot:
        - name: "my-hubspot"
          api_key: "pat-123"
```
- `name`: The name of the connection
- `api_key`: The API key is used for authentication with the HubSpot API

### Step 2: Create an asset file for data ingestion

To ingest data from HubSpot, you need to create an [asset configuration](https://bruin-data.github.io/bruin/assets/ingestr.html#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., hubspot_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.hubspot
type: ingestr
connection: postgres

parameters:
  source_connection: my-hubspot
  source_table: 'companies'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the hubspot connection defined in .bruin.yml.
- `source_table`: The name of the data table in hubspot that you want to ingest. For example, `companies` is a data table in hubspot that you may want to ingest. You can find the available source tables in hubspot [here](https://bruin-data.github.io/ingestr/supported-sources/hubspot.html#tables).

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/hubspot_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given HubSpot table into your Postgres database.

<img width="1124" alt="hubspot" src="https://github.com/user-attachments/assets/c88f2781-1e78-4d5b-8cb1-60b7993ea674">




