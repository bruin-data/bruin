# HubSpot
[HubSpot](https://www.hubspot.com/) is a customer relationship management software that helps businesses attract visitors, connect with customers, and close deals.

Bruin supports HubSpot as a source, and you can use it to ingest data from HubSpot into your data warehouse.

In order to set up the HubSpot connection, you need to add a configuration item in the .bruin.yml file and asset file, complying with the following schema. For that, you will need the `api_key`. For more information on obtaining the credentials, read [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/hubspot#setup-guide).

Follow the steps below to correctly set up HubSpot as a data source and run ingestion.

**Step 1: Add a Connection to .bruin.yml**

To connect to Hubspot, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      hubspot:
        - name: "my-hubspot"
          api_key: "pat-123"
```
- name: The name of the connection
- api_key: The API key is used for authentication with the HubSpot API

**Step 2: Create an Asset File for Data Ingestion**

To ingest data from HubSpot, you need to create an [asset configuration](https://bruin-data.github.io/bruin/assets/ingestr.html#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., hubSpot_ingestion.yml) and add the following content:

```yaml
name: public.hubspot
type: ingestr
connection: postgres

parameters:
  source_connection: my-hubspot
  source_table: 'companies'

  destination: postgres
```
- name: The name of the asset.

- type: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.

- connection: This is the destination connection, which defines where the data should be stored. For example: "postgres" indicates that the ingested data will be stored in a PostgreSQL database.

**parameters:**
- source_connection: The name of the hubspot connection defined in .bruin.yml

- source_table: The name of the data table in hubspot that you want to ingest. For example, "companies" would ingest data related to companies. [Available source tables in HubSpot](https://bruin-data.github.io/ingestr/supported-sources/hubspot.html#tables)


**Step 3: [Run](https://bruin-data.github.io/bruin/commands/run.html) Asset to Ingest Data**
```
bruin run --file hubspot_ingestion.yml
```
It will ingest hubspot data to postgres.
