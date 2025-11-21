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

To ingest data from HubSpot, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., hubspot_ingestion.yml) inside the assets folder and add the following content:

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
- `source_table`: The name of the data table in hubspot that you want to ingest. For example, `companies` is a data table in hubspot that you may want to ingest.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------| 
| companies | - | - | replace | Retrieves information about organizations. |
| contacts | - | - | replace | Retrieves information about visitors, potential customers, and leads. |
| deals | - | - | replace | Retrieves deal records and tracks deal progress. |
| tickets | - | - | replace | Handles requests for help from customers or users. |
| products | - | - | replace | Retrieves pricing information of products. |
| quotes | - | - | replace | Retrieves price proposals that salespeople can create and send. |
| schemas | id | - | merge | Returns all object schemas that have been defined for your account. |

## Custom Objects

HubSpot allows you to create custom objects to store unique business data that's not covered by the standard objects. ingestr supports ingesting data from custom objects using the following format:

```plaintext
custom:<custom_object_name>
```

or with associations to other objects:

```plaintext
custom:<custom_object_name>:<associations>
```

### Parameters

- `custom_object_name`: The name of your custom object in HubSpot (can be either singular or plural form)
- `associations` (optional): Comma-separated list of object types to include as associations (e.g., `companies,deals,tickets,contacts`)

### Examples

Ingesting a custom object called "licenses":

```sh
ingestr ingest \
  --source-uri 'hubspot://?api_key=pat_test_12345' \
  --source-table 'custom:licenses' \
  --dest-uri duckdb:///hubspot.duckdb \
  --dest-table 'licenses.data'
```

Ingesting a custom object with associations to companies, deals, and contacts:

```sh
ingestr ingest \
  --source-uri 'hubspot://?api_key=pat_test_12345' \
  --source-table 'custom:licenses:companies,deals,contacts' \
  --dest-uri duckdb:///hubspot.duckdb \
  --dest-table 'licenses.data'
```

When you include associations, the response will contain information about the related objects, allowing you to track relationships between your custom objects and standard HubSpot objects.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/hubspot_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given HubSpot table into your Postgres database.

<img width="1124" alt="hubspot" src="https://github.com/user-attachments/assets/c88f2781-1e78-4d5b-8cb1-60b7993ea674">




