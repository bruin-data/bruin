# Pipedrive
[Pipedrive](https://www.pipedrive.com/) is a cloud-based sales Customer Relationship Management (CRM) tool designed to help businesses manage leads and deals, track communication, and automate sales processes.

Bruin supports Pipedrive as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Pipedrive into your data platform.

To set up a Pipedrive connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need `api_token`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/Pipedrive#grab-api-token).

Follow the steps below to set up Pipedrive correctly as a data source and run ingestion.
### Step 1: Add a connection to the .bruin.yml file
In order to set up Pipedrive connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. This configuration must comply with the following schema:

```yaml
connections:
      pipedrive:
        - name: "pipedrive"
          api_token: "token-123"   
```
- `api_token`: token used for authentication with the Pipedrive API

### Step 2: Create an asset file for data ingestion
To ingest data from Pipedrive, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., Pipedrive_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.pipedrive
type: ingestr

parameters:
  source_connection: pipedrive
  source_table: 'users'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset’s type. Set this to `ingestr` to use the ingestr data pipeline. For Pipedrive, it will be always `ingestr`
- `source_connection`: The name of the Pipedrive connection defined in `.bruin.yml`
- `source_table`: The name of the table in Pipedrive to ingest. Available tables are:
  - `activities`: Refers to scheduled events or tasks associated with deals, contacts, or organizations
  - `organizations`: Refers to company or entity with which you have potential or existing business dealings.
  - `products`: Refers to items or services offered for sale that can be associated with deals
  - `deals`: Refers to potential sale or transaction that you can track through various stages
  - `users`: Refers to Individual with a unique login credential who can access and use the platform
  - `persons`: Refers individual contacts or leads that can be linked to sales deals
- `destination`: The name of the destination connection.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/Pipedrive_asset.yml
```
As a result of this command, Bruin will ingest data from the given Pipedrive table into your Postgres database.

********
<img alt="Pipedrive" src="./media/Pipedrive.png">




